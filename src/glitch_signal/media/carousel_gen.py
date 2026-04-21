"""LinkedIn PDF carousel generator.

LinkedIn document posts (PDF carousels) are the highest-engagement format on
the platform — 24.42% avg vs ~4% for text-only. This module produces one
ready-to-upload PDF per signal for a text brand, using:

  1. Claude / Gemini  → slide structure (hook, N body slides, CTA)
  2. fal.ai FLUX      → one branded background per slide (dark + brand accent)
  3. Pillow           → text overlay (title + body + slide number) on each slide
  4. img2pdf          → compile into a single PDF

Output landing pad: `{settings.video_storage_path}/carousels/{brand_id}/<uuid>.pdf`.
Every call produces a fresh file; re-runs never overwrite.

Pair with upload_post.publish() content_type="document" to land the PDF on
LinkedIn as a native document post (highest-distribution format).
"""
from __future__ import annotations

import asyncio
import json
import pathlib
import uuid
from typing import Any

import litellm
import structlog
from PIL import Image, ImageDraw, ImageFont
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from glitch_signal.agent.llm import pick
from glitch_signal.config import brand_config, settings
from glitch_signal.db.models import Signal
from glitch_signal.media.image_gen import generate_image

log = structlog.get_logger(__name__)

# LinkedIn recommends 4:5 for document posts — renders full-height in feed.
SLIDE_W = 1080
SLIDE_H = 1350

# Fonts shipped with most Debian/Ubuntu systems — no extra install.
_FONT_BOLD = "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
_FONT_REGULAR = "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"
_FONT_MONO = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf"

# Brand defaults; overridden by brand_config.brand.{accent_color, base_color}.
_DEFAULT_ACCENT = "#00ff88"
_DEFAULT_BASE = "#0a0a0f"

_SLIDE_SYSTEM = """You are writing a LinkedIn PDF carousel for a technical founder's AI lab.

Carousels on LinkedIn get 6x the engagement of text posts when they deliver
real frameworks / lessons / concrete decisions. Empty "5 tips" filler dies.

Your job: take the supplied signal (a shipped piece of work) and break it
into a tight carousel that teaches one coherent idea end-to-end.

Rules:
- Voice matches the brand voice file verbatim — no "thrilled to announce",
  no "game-changer", no emoji walls. Technical, direct, specific.
- Hook slide: one sentence ≤ 12 words that stops the scroll. Subtitle ≤ 18 words.
- Body slides: each one self-contained. Title ≤ 8 words, body ≤ 35 words.
- Every slide carries a concrete specific — prefer a DECISION or a TRADEOFF
  to a METRIC. Metrics only if the signal contained them; don't invent.
- NO slide should just repeat the hook in different words.
- CTA slide: invite people to the repo / site. One action, no ladder.

HARD GUARD RAILS — any carousel violating these will be rejected at review:
1. Do NOT claim a measured outcome (percent / ROI / reduction / growth /
   savings / revenue) unless the signal you were given explicitly contains
   that number. Describe the BUILD, not the RESULT.
2. Do NOT use marketing verbs: "reduces", "boosts", "delivers", "improves".
   Use "targets", "aims to", "was built to", "is running", "is in production".
3. Do NOT use hype adjectives: game-changing, revolutionary, cutting-edge,
   industry-leading, robust, powerful, seamless.
4. No "excited to announce" / "thrilled to share" / "proud to introduce".
5. Never promise financial outcomes. Never say "results guaranteed".

Output valid JSON only, no markdown fences, matching this schema:
{
  "hook": {"title": "<≤12 words>", "subtitle": "<≤18 words>"},
  "body": [
    {"title": "<≤8 words>", "body": "<≤35 words>"},
    ...
  ],
  "cta": {"title": "<≤10 words>", "subtitle": "<≤18 words>", "link": "<url>"}
}
"""


class CarouselError(RuntimeError):
    pass


async def generate_carousel(
    signal: Signal | None,
    brand_id: str,
    *,
    body_slides: int = 5,
    cta_link: str = "github.com/glitch-exec-labs",
    slide_data_override: dict[str, Any] | None = None,
) -> pathlib.Path:
    """Generate a LinkedIn-ready PDF carousel. Returns the PDF path.

    Two modes:
      - Signal-driven (signal set, slide_data_override=None): LLM generates
        slide content from the Signal + voice + playbook.
      - Pre-written (slide_data_override set): caller supplies the exact
        slide content as a dict matching the schema. No LLM call. Useful
        when an operator wants pixel-perfect control over a specific post.

    Total slide count = 1 (hook) + len(body) + 1 (cta). Six is the LinkedIn
    sweet spot; more than 10 loses engagement.

    Dry-run mode returns a fake path without calling fal.ai or the LLM.
    """
    s = settings()
    signal_id_for_log = signal.id if signal else "override"
    if s.is_dry_run:
        fake = pathlib.Path(f"/tmp/dry-run-carousel-{uuid.uuid4().hex[:8]}.pdf")
        log.info("carousel.dry_run", signal_id=signal_id_for_log, path=str(fake))
        return fake

    if slide_data_override is not None:
        slide_data = slide_data_override
    else:
        if signal is None:
            raise CarouselError(
                "generate_carousel: either signal or slide_data_override required"
            )
        slide_data = await _generate_slide_content(
            signal=signal,
            brand_id=brand_id,
            body_slides=body_slides,
            cta_link=cta_link,
        )
    total_slides = 1 + len(slide_data["body"]) + 1

    # Generate background images in parallel (cheap on fal.ai; ~2s each sync).
    background_paths = await _generate_backgrounds(
        slide_data=slide_data, brand_id=brand_id, total=total_slides
    )

    # Compose each slide's PNG with text overlay
    out_dir = pathlib.Path(s.video_storage_path) / "carousels" / brand_id
    out_dir.mkdir(parents=True, exist_ok=True)
    carousel_id = uuid.uuid4().hex
    slide_png_paths: list[pathlib.Path] = []

    accent, base, secondary = _brand_colors(brand_id)

    # Slide 1 — hook
    hook_path = out_dir / f"{carousel_id}_01_hook.png"
    _render_slide(
        background_path=background_paths[0],
        title=slide_data["hook"]["title"],
        body=slide_data["hook"]["subtitle"],
        slide_num=1,
        slide_total=total_slides,
        accent=accent,
        base=base,
        secondary=secondary,
        title_size=78,
        body_size=38,
        is_hook=True,
    ).save(hook_path, "PNG", optimize=True)
    slide_png_paths.append(hook_path)

    # Body slides
    for i, body in enumerate(slide_data["body"], start=2):
        p = out_dir / f"{carousel_id}_{i:02d}_body.png"
        _render_slide(
            background_path=background_paths[i - 1],
            title=body["title"],
            body=body["body"],
            slide_num=i,
            slide_total=total_slides,
            accent=accent,
            base=base,
            secondary=secondary,
        ).save(p, "PNG", optimize=True)
        slide_png_paths.append(p)

    # CTA slide
    cta_idx = total_slides
    cta_path = out_dir / f"{carousel_id}_{cta_idx:02d}_cta.png"
    _render_slide(
        background_path=background_paths[-1],
        title=slide_data["cta"]["title"],
        body=slide_data["cta"]["subtitle"] + "\n\n" + slide_data["cta"]["link"],
        slide_num=cta_idx,
        slide_total=total_slides,
        accent=accent,
        base=base,
        secondary=secondary,
        title_size=72,
        body_size=40,
        is_cta=True,
    ).save(cta_path, "PNG", optimize=True)
    slide_png_paths.append(cta_path)

    # Compile to PDF
    pdf_path = out_dir / f"{carousel_id}.pdf"
    _compile_pdf(slide_png_paths, pdf_path)

    # Clean up intermediate PNGs (keep PDF only)
    for p in slide_png_paths:
        try:
            p.unlink()
        except OSError:
            pass

    log.info(
        "carousel.done",
        brand_id=brand_id,
        signal_id=signal_id_for_log,
        path=str(pdf_path),
        slides=total_slides,
        size_kb=pdf_path.stat().st_size // 1024,
    )
    return pdf_path


# ---------------------------------------------------------------------------
# LLM: produce structured slide content
# ---------------------------------------------------------------------------

@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type(
        (litellm.ServiceUnavailableError, litellm.RateLimitError, litellm.APIConnectionError)
    ),
)
async def _generate_slide_content(
    *,
    signal: Signal,
    brand_id: str,
    body_slides: int,
    cta_link: str,
) -> dict[str, Any]:
    cfg = brand_config(brand_id)
    voice = _load_file(cfg.get("voice_prompt_path"))
    playbook = _load_file(cfg.get("platform_playbook_path"))

    system = (
        f"{voice}\n\n"
        f"---\n"
        f"{_SLIDE_SYSTEM}\n"
        f"---\n"
        f"Platform playbook (condensed):\n{playbook[:2500]}\n"
        f"---\n"
        f"Produce exactly {body_slides} body slides. CTA link: {cta_link}"
    )
    user = (
        f"Signal:\n"
        f"Source: {signal.source} ({signal.source_ref})\n"
        f"Summary: {signal.summary}\n"
        f"Novelty: {signal.novelty_score:.2f}\n\n"
        f"Write the carousel."
    )

    s_ = settings()
    tier = "smart" if (s_.openai_api_key or s_.anthropic_api_key) else "cheap"
    mc = pick(tier)
    resp = await litellm.acompletion(
        model=mc.model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        response_format={"type": "json_object"},
        max_tokens=4096,
        **mc.kwargs,
    )
    raw = resp.choices[0].message.content or "{}"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CarouselError(f"LLM returned invalid JSON: {exc} :: {raw[:200]!r}") from exc

    if "hook" not in data or "body" not in data or "cta" not in data:
        raise CarouselError(f"LLM output missing required keys: {list(data.keys())}")
    return data


# ---------------------------------------------------------------------------
# fal.ai: generate one brand-consistent background per slide
# ---------------------------------------------------------------------------

async def _generate_backgrounds(
    *,
    slide_data: dict[str, Any],
    brand_id: str,
    total: int,
) -> list[pathlib.Path]:
    """Generate `total` background images. All share a base visual language so
    the carousel reads as a set, not a scrapbook.
    """
    base_prompt = (
        "Dark minimal tech background, deep black base with subtle neon green "
        "circuit patterns, abstract, professional, no text, no humans, no UI, "
        "minimal composition, cinematic lighting, Glitch Executor brand aesthetic"
    )

    prompts = []
    # Hook: the most distinct visual
    prompts.append(
        f"{base_prompt}, hero composition with a single strong focal glow, "
        "bold and intentional"
    )
    # Body: calmer, readable backgrounds
    for _ in slide_data["body"]:
        prompts.append(
            f"{base_prompt}, quieter composition so text reads clearly, "
            "gentle gradient with subtle accent particles"
        )
    # CTA: closing visual
    prompts.append(
        f"{base_prompt}, closing composition with upward energy, "
        "convergence of light elements, brand sign-off feel"
    )

    assert len(prompts) == total, f"expected {total} prompts, got {len(prompts)}"

    async def _one(prompt: str) -> pathlib.Path:
        return await generate_image(prompt=prompt, brand_id=brand_id, aspect="4:5")

    # Generate in parallel — fal.ai handles it fine, cost is the same
    paths = await asyncio.gather(*[_one(p) for p in prompts])
    return list(paths)


# ---------------------------------------------------------------------------
# Pillow: text overlay on a background image
# ---------------------------------------------------------------------------

def _render_slide(
    *,
    background_path: pathlib.Path,
    title: str,
    body: str,
    slide_num: int,
    slide_total: int,
    accent: str,
    base: str,
    secondary: str = "#0088ff",
    title_size: int = 64,
    body_size: int = 36,
    is_hook: bool = False,
    is_cta: bool = False,
) -> Image.Image:
    """Compose one carousel slide: background + dark overlay + text + chrome."""
    # Resize background to exact slide dimensions
    bg = Image.open(background_path).convert("RGBA")
    bg = _resize_cover(bg, SLIDE_W, SLIDE_H)

    # Darken for text readability — semi-transparent black overlay
    darkness = 140 if is_hook or is_cta else 170  # hook/cta: slightly lighter so hero detail reads
    overlay = Image.new("RGBA", (SLIDE_W, SLIDE_H), (0, 0, 0, darkness))
    bg = Image.alpha_composite(bg, overlay)

    # Soft vignette on edges — pulls eye to center text
    vignette = Image.new("RGBA", (SLIDE_W, SLIDE_H), (0, 0, 0, 0))
    vdraw = ImageDraw.Draw(vignette)
    for i, alpha in enumerate([40, 30, 20, 10]):
        inset = (i + 1) * 20
        vdraw.rectangle(
            [(inset, inset), (SLIDE_W - inset, SLIDE_H - inset)],
            outline=(0, 0, 0, alpha),
            width=20,
        )
    bg = Image.alpha_composite(bg, vignette)

    draw = ImageDraw.Draw(bg)

    # Header — brand block: accent bar + monospace wordmark anchor
    #   ▌  GLITCH · GROW
    draw.rectangle([(80, 80), (80 + 12, 80 + 60)], fill=accent)
    wordmark_font = _font(_FONT_MONO, 22)
    draw.text(
        (110, 92),
        "GLITCH · EXECUTOR",
        font=wordmark_font,
        fill=(255, 255, 255, 230),
    )

    # Slide counter (top-right) in secondary blue for color rhythm
    mono_small = _font(_FONT_MONO, 24)
    counter_text = f"{slide_num:02d} / {slide_total:02d}"
    tw = draw.textlength(counter_text, font=mono_small)
    draw.text(
        (SLIDE_W - 80 - tw, 92),
        counter_text,
        font=mono_small,
        fill=_hex_to_rgba(secondary, 230),
    )

    # Title + body — center-left-aligned block
    title_font = _font(_FONT_BOLD, title_size)
    body_font = _font(_FONT_REGULAR, body_size)

    content_x = 90
    content_width = SLIDE_W - 2 * content_x
    title_wrapped = _wrap_text(title, title_font, content_width, draw)
    body_wrapped = _wrap_text(body, body_font, content_width, draw)

    # Vertical layout: put the block ~40% down the slide (rule-of-thirds feel)
    title_h = _text_block_height(title_wrapped, title_font, draw)
    body_h = _text_block_height(body_wrapped, body_font, draw)
    gap = 60
    total_h = title_h + gap + body_h
    y_top = (SLIDE_H - total_h) // 2

    # Title
    y = y_top
    for line in title_wrapped:
        draw.text((content_x, y), line, font=title_font, fill=(255, 255, 255, 255))
        y += title_font.size + 10

    # Accent underline below title
    y += 20
    draw.rectangle(
        [(content_x, y - 10), (content_x + 80, y - 6)],
        fill=accent,
    )

    # Body
    y = y_top + title_h + gap
    for line in body_wrapped:
        # CTA link detection — monospace + accent color + fit-to-width
        if is_cta and ("github.com" in line or "glitchexecutor.com" in line):
            # URLs never wrap (no spaces), so shrink the font until the
            # line fits within content_width instead of truncating.
            link_font = _fit_mono_to_width(line, content_width, body_size - 4)
            draw.text(
                (content_x, y),
                line,
                font=link_font,
                fill=accent,
            )
            y += link_font.size + 10
        else:
            draw.text((content_x, y), line, font=body_font, fill=(230, 230, 230, 255))
            y += body_font.size + 10

    # ── Footer ─────────────────────────────────────────────────────────────
    # Progress bar on every slide — thin horizontal line that fills as you
    # advance through the carousel. Gives the deck a continuous rhythm.
    bar_y = SLIDE_H - 40
    bar_margin = 80
    bar_w = SLIDE_W - 2 * bar_margin
    # Track (dim)
    draw.rectangle(
        [(bar_margin, bar_y), (bar_margin + bar_w, bar_y + 3)],
        fill=(90, 90, 90, 180),
    )
    # Filled portion (bright accent) — proportional to slide_num / total
    fill_w = int(bar_w * (slide_num / slide_total))
    draw.rectangle(
        [(bar_margin, bar_y), (bar_margin + fill_w, bar_y + 3)],
        fill=accent,
    )

    # Wordmark footer on hook/cta only — keeps body slides text-focused
    if is_hook or is_cta:
        footer_font = _font(_FONT_MONO, 22)
        footer = "glitchexecutor.com"
        draw.text(
            (content_x, SLIDE_H - 95),
            footer,
            font=footer_font,
            fill=(200, 200, 200, 220),
        )
        # Secondary-color pip next to the wordmark for color rhythm
        fw = draw.textlength(footer, font=footer_font)
        pip_x = content_x + fw + 16
        draw.rectangle(
            [(pip_x, SLIDE_H - 90), (pip_x + 8, SLIDE_H - 78)],
            fill=_hex_to_rgba(secondary, 255),
        )

    return bg.convert("RGB")


def _hex_to_rgba(hex_color: str, alpha: int = 255) -> tuple[int, int, int, int]:
    """Convert '#00ff88' → (0, 255, 136, alpha)."""
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16), alpha)


def _fit_mono_to_width(
    text: str,
    max_width: int,
    start_size: int,
    min_size: int = 18,
) -> ImageFont.FreeTypeFont:
    """Shrink the mono font until `text` fits within max_width. Used for
    long URLs on the CTA slide that can't word-wrap."""
    # Rough pixel-per-char estimate avoids instantiating N fonts: bail fast
    # once the shrink is clearly unnecessary. For correctness we measure.
    tmp = Image.new("RGB", (1, 1))
    d = ImageDraw.Draw(tmp)
    size = start_size
    while size > min_size:
        font = _font(_FONT_MONO, size)
        if d.textlength(text, font=font) <= max_width:
            return font
        size -= 2
    return _font(_FONT_MONO, min_size)


def _resize_cover(img: Image.Image, target_w: int, target_h: int) -> Image.Image:
    """Resize + center-crop so the image fills (target_w, target_h) exactly."""
    src_w, src_h = img.size
    scale = max(target_w / src_w, target_h / src_h)
    new_w, new_h = int(src_w * scale), int(src_h * scale)
    img = img.resize((new_w, new_h), Image.LANCZOS)
    left = (new_w - target_w) // 2
    top = (new_h - target_h) // 2
    return img.crop((left, top, left + target_w, top + target_h))


def _wrap_text(text: str, font, max_width: int, draw: ImageDraw.ImageDraw) -> list[str]:
    """Greedy word-wrap — respects explicit newlines the LLM included."""
    out_lines: list[str] = []
    for paragraph in text.split("\n"):
        if not paragraph.strip():
            out_lines.append("")
            continue
        words = paragraph.split()
        line = ""
        for w in words:
            candidate = f"{line} {w}".strip()
            if draw.textlength(candidate, font=font) <= max_width:
                line = candidate
            else:
                if line:
                    out_lines.append(line)
                line = w
        if line:
            out_lines.append(line)
    return out_lines


def _text_block_height(lines: list[str], font, draw: ImageDraw.ImageDraw) -> int:
    return len(lines) * (font.size + 10)


def _font(path: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(path, size)


def _brand_colors(brand_id: str) -> tuple[str, str, str]:
    """Return (accent, base, secondary) from brand config. Secondary defaults
    to electric blue if the brand didn't declare one."""
    try:
        brand = brand_config(brand_id).get("brand", {})
    except KeyError:
        brand = {}
    return (
        brand.get("accent_color") or _DEFAULT_ACCENT,
        brand.get("base_color") or _DEFAULT_BASE,
        brand.get("secondary_color") or "#0088ff",
    )


def _load_file(path: str | None) -> str:
    if not path:
        return ""
    p = pathlib.Path(path)
    return p.read_text() if p.exists() else ""


# ---------------------------------------------------------------------------
# img2pdf: compile PNG slides into a single PDF
# ---------------------------------------------------------------------------

def _compile_pdf(slide_paths: list[pathlib.Path], pdf_path: pathlib.Path) -> None:
    import img2pdf

    # img2pdf handles RGB PNGs natively — no reencoding, lossless
    raw_bytes = [p.read_bytes() for p in slide_paths]
    with open(pdf_path, "wb") as fh:
        fh.write(img2pdf.convert(raw_bytes))
