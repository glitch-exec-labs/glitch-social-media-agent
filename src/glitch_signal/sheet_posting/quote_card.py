"""Quote-card rendering: one sheet body → one fully designed image via
gpt-image-2 → upload as a single LinkedIn/X image post.

Quote cards work where a short, punchy angle needs visual impact — stat
reveals, one-line takeaways, milestone callouts. Unlike carousel rows
(multi-slide deck) and text rows (plain tweet), a quote card is ONE image
with the key phrase rendered inside it by the model.

Pipeline:
    1. LLM condenses the sheet body into:
         - headline       ≤ 10 words (the hook line)
         - subline        ≤ 15 words (optional support)
         - link           (pulled from body if present, else org URL)
    2. gpt-image-2 renders a designed image using a structured prompt
       with brand chrome (wordmark, accent, circuit pattern, link footer).
    3. Caller uploads the image via Upload-Post upload_photos() with the
       original sheet body as the post caption.
"""
from __future__ import annotations

import json
import pathlib

import litellm
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from glitch_signal.agent.llm import pick
from glitch_signal.config import brand_config, settings
from glitch_signal.media.image_gen import generate_designed_image

log = structlog.get_logger(__name__)


_QUOTE_SYSTEM = """You distill a polished social-media post into the text for a
single designed image (a "quote card" for LinkedIn or X).

The image will render the headline, subline, and link BY THE MODEL inside
the composition — so each piece must be short, impactful, and stand alone
visually.

Rules:
- Voice stays identical to the input body; do not rewrite the tone.
- Do not fabricate claims. If a number appears in the body, you may use it.
  If no number is there, don't invent one.
- Do not use marketing verbs ("reduces", "delivers", "boosts").
- No hype adjectives (seamless, robust, game-changing, cutting-edge).
- Headline must be ≤ 10 words. Sharpest possible phrasing. Opens the thought.
- Subline must be ≤ 15 words. Closes the thought or adds the "why it matters".
- Link: lift any GitHub / domain URL from the body. If none, use the default provided.

Output ONLY valid JSON matching this exact schema — no markdown fences:
{
  "headline": "<≤10 words>",
  "subline":  "<≤15 words>",
  "link":     "<url or empty string>"
}
"""


class QuoteCardError(RuntimeError):
    pass


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type(
        (litellm.ServiceUnavailableError, litellm.RateLimitError, litellm.APIConnectionError)
    ),
)
async def _distill_body(
    *, body: str, brand_id: str, default_link: str,
) -> dict[str, str]:
    """LLM call: polished post body → {headline, subline, link} for the card."""
    cfg = brand_config(brand_id)
    voice_path = cfg.get("voice_prompt_path")
    voice = ""
    if voice_path:
        p = pathlib.Path(voice_path)
        if p.exists():
            voice = p.read_text()

    system = (
        f"{voice}\n\n---\n{_QUOTE_SYSTEM}\n---\n"
        f"Default link if the body has no URL: {default_link}"
    )
    user = f"The body:\n\n{body}\n\nReturn the JSON."

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
        max_tokens=1024,
        **mc.kwargs,
    )
    raw = (resp.choices[0].message.content or "").strip() or "{}"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise QuoteCardError(f"LLM returned invalid JSON: {exc} :: {raw[:200]!r}") from exc
    for key in ("headline", "subline", "link"):
        if key not in data:
            data[key] = "" if key == "subline" or key == "link" else ""
    return {
        "headline": str(data["headline"]).strip(),
        "subline": str(data["subline"]).strip(),
        "link": str(data["link"]).strip() or default_link,
    }


def _build_quote_prompt(
    *,
    headline: str,
    subline: str,
    link: str,
    brand_id: str,
) -> str:
    """Ready-to-send gpt-image-2 prompt for a single quote-card image."""
    cfg = brand_config(brand_id).get("brand", {})
    accent = cfg.get("accent_color") or "#00ff88"
    base = cfg.get("base_color") or "#0a0a0f"
    secondary = cfg.get("secondary_color") or "#0088ff"
    # Brand wordmark varies slightly per brand
    wordmark = "GLITCH · EXECUTOR" if brand_id == "glitch_executor" else "TEJAS · GLITCH"

    chrome = (
        f"Deep black background color {base}. Subtle dark neon green circuit-"
        f"pattern texture fading in from the edges and corners. "
        f"Top-left corner: a small bright neon green vertical accent bar in "
        f"color {accent}, followed by small uppercase monospace white text "
        f"'{wordmark}'. "
        f"Top-right corner: small uppercase monospace text 'GLITCH EXECUTOR' "
        f"in electric blue color {secondary}."
    )
    main = (
        "Main composition centered vertically, left-aligned with 90px margins. "
        f"Large bold sans-serif headline in clean white: '{headline}'. "
        f"Thin bright neon green underline accent bar in {accent} below the "
        "headline."
    )
    if subline:
        main += (
            f" Medium-sized subline below the underline in lighter gray: "
            f"'{subline}'."
        )
    footer = ""
    if link:
        footer = (
            f" Bottom-left: small monospace text '{link}' in neon green "
            f"{accent}, followed by a tiny blue {secondary} pip."
        )

    return (
        "A social-media post image, 1:1 square format, Glitch Executor brand. "
        "Dark, minimal, tech-lab, professional. No humans, no emojis, no "
        "clipart, no photos. Clean geometric sans-serif typography. All text "
        "must render with perfect spelling.\n\n"
        f"{chrome}\n\n{main}\n\n{footer}"
    )


async def generate_quote_card(
    body: str,
    brand_id: str,
    *,
    default_link: str = "github.com/glitch-exec-labs",
    quality: str = "high",
) -> pathlib.Path:
    """End-to-end: body → distilled JSON → designed image path."""
    distilled = await _distill_body(
        body=body, brand_id=brand_id, default_link=default_link,
    )
    prompt = _build_quote_prompt(
        headline=distilled["headline"],
        subline=distilled["subline"],
        link=distilled["link"],
        brand_id=brand_id,
    )
    path = await generate_designed_image(
        prompt=prompt, brand_id=brand_id, aspect="1:1", quality=quality,
    )
    log.info(
        "quote_card.done",
        brand_id=brand_id,
        path=str(path),
        headline=distilled["headline"],
    )
    return path
