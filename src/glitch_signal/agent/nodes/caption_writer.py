"""CaptionWriter node — LLM-generates title + caption + hashtags for a Signal.

Runs after drive_scout in the drive_footage pipeline. Writes a ContentScript
(script_body = caption) and a VideoAsset pointing at the already-downloaded
local file — bypassing storyboard / video generation / assembler entirely.

Voice guide: brand config's `voice_prompt_path` (a markdown file, gitignored)
provides the per-brand style. Falls back to a neutral default when absent.
"""
from __future__ import annotations

import json
import pathlib
import uuid
from datetime import UTC, datetime

import litellm
import structlog

from glitch_signal.agent.llm import pick
from glitch_signal.agent.state import SignalAgentState
from glitch_signal.config import brand_config, settings
from glitch_signal.db.models import ContentScript, Signal, VideoAsset
from glitch_signal.db.session import _session_factory

log = structlog.get_logger(__name__)

_ASSEMBLER_TAG = "drive_passthrough@1.0"   # marks assets that skipped assembler

_DEFAULT_VOICE = (
    "Warm, grounded, honest. No hype, no superlatives, no engagement bait. "
    "Write like a real person talking to one person, not a brand broadcasting."
)

_SYSTEM_TEMPLATE = """You are writing TikTok captions for a brand.

BRAND: {display_name}
VOICE RULES:
{voice}

CONTEXT: The video has already been shot and edited. You are writing
*around* the video, not describing it frame-by-frame. Keep the viewer
watching and wanting to engage.

CAPTION RULES:
- Total length ≤ 2000 characters.
- Start with a hook in the first 80 characters — this is what shows before
  the "more" cutoff in the TikTok feed.
- End with 3–8 hashtags, space-separated, all lowercase, no punctuation.
- No emoji walls. At most 2 emojis in the whole caption.
- Never fabricate product claims, medical claims, or results.

Return JSON ONLY:
{{
  "title": "plain text, ≤ 100 chars, used for TikTok title field",
  "caption": "full caption text including hashtags at the end",
  "hashtags": ["tag1", "tag2", ...]
}}
"""


async def caption_writer_node(state: SignalAgentState) -> SignalAgentState:
    signal_id = state.get("signal_id")
    if not signal_id:
        return {**state, "error": "caption_writer: missing signal_id"}

    brand_id = state.get("brand_id") or settings().default_brand_id
    platform = state.get("platform") or "tiktok"

    factory = _session_factory()
    async with factory() as session:
        signal = await session.get(Signal, signal_id)
        if not signal:
            return {**state, "error": f"caption_writer: Signal {signal_id} not found"}

        # The local path was stashed by drive_scout in the signals list; if
        # this node is re-run standalone (e.g. caption regeneration), fall back
        # to the conventional location.
        local_path = _resolve_local_path(state, signal, brand_id)

        title, caption, hashtags = await _generate_caption(signal, brand_id, platform)

        script_id = str(uuid.uuid4())
        asset_id = str(uuid.uuid4())
        now = datetime.now(UTC).replace(tzinfo=None)

        cs = ContentScript(
            id=script_id,
            brand_id=brand_id,
            signal_id=signal_id,
            platform=platform,
            script_body=caption,
            content_type="drive",
            key_visuals=json.dumps([]),
            shots="[]",
            status="done",
            created_at=now,
        )
        session.add(cs)

        asset = VideoAsset(
            id=asset_id,
            brand_id=brand_id,
            script_id=script_id,
            file_path=str(local_path),
            duration_s=await _probe_duration(local_path),
            assembler_version=_ASSEMBLER_TAG,
            created_at=now,
        )
        session.add(asset)

        signal.status = "scripted"
        session.add(signal)
        await session.commit()

    log.info(
        "caption_writer.done",
        brand_id=brand_id,
        signal_id=signal_id,
        script_id=script_id,
        asset_id=asset_id,
        title=title[:60],
        n_hashtags=len(hashtags),
    )

    return {
        **state,
        "brand_id": brand_id,
        "script_id": script_id,
        "script_body": caption,
        "content_type": "drive",
        "key_visuals": [],
        "asset_id": asset_id,
        "asset_path": str(local_path),
    }


async def _generate_caption(
    signal: Signal, brand_id: str, platform: str
) -> tuple[str, str, list[str]]:
    cfg = brand_config(brand_id)
    display_name = cfg.get("display_name", brand_id)
    voice = _load_voice(cfg) or _DEFAULT_VOICE
    default_hashtags: list[str] = cfg.get("default_hashtags") or []

    # DISPATCH_MODE gates PUBLISH actions (posting to TikTok, sending emails,
    # etc.), NOT every LLM call. Caption generation is cheap, text-only,
    # and exactly what the operator needs to review during dry-run —
    # skipping it leaves them previewing template fallback captions that
    # don't reflect the real system behaviour.
    #
    # The previous implementation hard-coded tier="smart" (Claude Sonnet)
    # which requires an Anthropic key. For caption writing the cost/quality
    # trade-off doesn't justify Sonnet — tier="cheap" (Gemini Flash) is
    # the right default since we always have a Google key for the Scout
    # novelty scorer anyway.
    mc = pick("cheap")
    system_prompt = _SYSTEM_TEMPLATE.format(display_name=display_name, voice=voice)
    user_msg = (
        f"Platform: {platform}\n"
        f"Drive clip filename: {signal.summary}\n"
        f"Default hashtags to consider: {', '.join(default_hashtags) or '(none)'}\n\n"
        "Write the post."
    )

    raw_content = ""
    try:
        resp = await litellm.acompletion(
            model=mc.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_msg},
            ],
            response_format={"type": "json_object"},
            # Gemini 2.5 Flash counts reasoning ("thinking") tokens against
            # max_tokens and will silently truncate the visible output when
            # the ceiling is tight. 4096 comfortably covers a 2000-char
            # caption plus whatever thinking the model wants to do.
            max_tokens=4096,
            **mc.kwargs,
        )
        raw_content = resp.choices[0].message.content or ""
        data = _parse_caption_json(raw_content)
    except Exception as exc:
        log.warning(
            "caption_writer.llm_failed",
            error=str(exc),
            raw_preview=raw_content[:200] if raw_content else "",
        )
        data = {}

    title = str(data.get("title", "")).strip()[:100] or display_name
    caption = str(data.get("caption", "")).strip()[:2000]
    raw_tags = data.get("hashtags") or []
    hashtags = [str(t).lstrip("#").strip().lower() for t in raw_tags if t]

    # Fail-soft fallback: if the LLM path didn't yield a caption, compose
    # one from the brand's default_hashtags (stripping "#" then re-adding
    # so the caption body is correctly prefixed).
    if not caption:
        fallback_tags = hashtags or [
            h.lstrip("#").strip().lower() for h in default_hashtags if h
        ]
        hashtag_block = " ".join(f"#{t}" for t in fallback_tags)
        caption = (f"{display_name}" + (f"\n\n{hashtag_block}" if hashtag_block else "")).strip()
        hashtags = fallback_tags

    return title, caption, hashtags


def _parse_caption_json(raw: str) -> dict:
    """Best-effort JSON parse for LLM output.

    Handles the common failure modes we've seen in practice:
    - leading/trailing whitespace or markdown fences (```json ... ```)
    - output that ended mid-generation (truncated) — try to recover the
      last valid {"title": ..., "caption": ..., "hashtags": [...]} block
    """
    if not raw:
        return {}
    text = raw.strip()

    # Strip markdown code fences if the model ignored response_format=json.
    if text.startswith("```"):
        # Drop the leading ```[json] and the trailing ```
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    # Happy path
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Recovery: find the last closing brace that still yields valid JSON.
    # This rescues outputs truncated after "caption": "..." but before the
    # closing brace.
    last_brace = text.rfind("}")
    while last_brace > 0:
        try:
            return json.loads(text[: last_brace + 1])
        except json.JSONDecodeError:
            last_brace = text.rfind("}", 0, last_brace)
    return {}


def _load_voice(cfg: dict) -> str | None:
    rel = cfg.get("voice_prompt_path")
    if not rel:
        return None
    path = pathlib.Path(rel)
    if not path.is_absolute():
        # Resolve relative to the repo root (CWD of the service).
        path = pathlib.Path.cwd() / rel
    if not path.exists():
        log.warning("caption_writer.voice_prompt_missing", path=str(path))
        return None
    return path.read_text().strip()


def _resolve_local_path(
    state: SignalAgentState, signal: Signal, brand_id: str
) -> pathlib.Path:
    # Prefer what drive_scout passed through in state.
    for entry in state.get("signals") or []:
        if entry.get("id") == signal.id and entry.get("local_path"):
            return pathlib.Path(entry["local_path"])
    # Fallback: conventional location (drive_scout's download target).
    return (
        pathlib.Path(settings().video_storage_path)
        / "drive"
        / brand_id
        / f"{signal.source_ref}.mp4"
    )


async def _probe_duration(path: pathlib.Path) -> float:
    if not path.exists():
        return 0.0
    try:
        import ffmpeg
        probe = ffmpeg.probe(str(path))
        return float(probe["format"].get("duration", 0.0))
    except Exception:
        return 0.0
