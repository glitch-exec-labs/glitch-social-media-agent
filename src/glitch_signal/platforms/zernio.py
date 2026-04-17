"""Zernio publisher — multi-platform social posting via an audited partner app.

Zernio (formerly Late) is a managed social-posting API that holds audited
apps for TikTok, IG, FB, YT, X, etc. Using Zernio as a thin publish layer
sidesteps the TikTok-side audit wall that blocks direct-post for unaudited
first-party apps.

Publish flow:
  1. zernio.media.upload(path) → hosted CDN URL
  2. zernio.posts.create(content, platforms=[{platform, accountId}],
                         media_items=[{type, url}], publish_now=True)

Per-brand config lives under platforms.zernio_tiktok (or zernio_<platform>)
and must carry:
  - enabled: true
  - account_id: <Zernio internal account id from client.accounts.list()>

The platform string on ScheduledPost rows for this path is "zernio_tiktok"
so the dispatcher (agent/nodes/publisher.py) can route to us without
disturbing the direct "tiktok" path that still exists for when our own
app gets audited.

All live calls gated behind DISPATCH_MODE=dry_run.
"""
from __future__ import annotations

import asyncio
import pathlib
import uuid

import structlog

from glitch_signal.config import brand_config, settings
from glitch_signal.db.models import ContentScript
from glitch_signal.db.session import _session_factory

log = structlog.get_logger(__name__)

# Map of our platform-key suffixes to Zernio's platform enum value.
_PLATFORM_MAP = {
    "zernio_tiktok":    "tiktok",
    "zernio_instagram": "instagram",
    "zernio_youtube":   "youtube",
    "zernio_twitter":   "twitter",
    "zernio_facebook":  "facebook",
}


async def publish(
    platform: str,
    file_path: str,
    script_id: str,
    brand_id: str | None = None,
) -> tuple[str, str | None]:
    """Publish a video via Zernio. Returns (zernio_post_id, share_url|None)."""
    s = settings()

    if s.is_dry_run:
        fake_id = f"zernio-dry-{uuid.uuid4().hex[:10]}"
        log.info(
            "zernio.publish.dry_run",
            publish_id=fake_id,
            file_path=file_path,
            brand_id=brand_id,
            platform=platform,
        )
        return fake_id, None

    if not brand_id:
        raise ValueError("zernio.publish: brand_id is required for live publish")
    if not s.zernio_api_key:
        raise RuntimeError("ZERNIO_API_KEY is not set")

    target = _PLATFORM_MAP.get(platform)
    if not target:
        raise ValueError(f"zernio.publish: unknown platform key {platform!r}")

    cfg_block = (
        brand_config(brand_id).get("platforms", {}).get(platform, {}) or {}
    )
    account_id = cfg_block.get("account_id")
    if not account_id:
        raise RuntimeError(
            f"zernio.publish: brand={brand_id!r} is missing "
            f"platforms.{platform}.account_id — find it via "
            f"client.accounts.list() and put it in the brand config"
        )

    path = pathlib.Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"zernio.publish: file missing: {file_path}")

    caption, title, hashtags = await _read_caption(script_id, brand_id, cfg_block)

    # Zernio SDK is blocking httpx-under-the-hood → run in a thread so we
    # don't block the scheduler's event loop during large file uploads.
    return await asyncio.to_thread(
        _publish_sync,
        api_key=s.zernio_api_key,
        target_platform=target,
        account_id=account_id,
        file_path=str(path),
        caption=caption,
        title=title,
        hashtags=hashtags,
        tiktok_settings=_tiktok_settings(cfg_block) if target == "tiktok" else None,
    )


# ---------------------------------------------------------------------------
# Blocking worker — runs in a thread via asyncio.to_thread
# ---------------------------------------------------------------------------

def _publish_sync(
    *,
    api_key: str,
    target_platform: str,
    account_id: str,
    file_path: str,
    caption: str,
    title: str,
    hashtags: list[str],
    tiktok_settings: dict | None,
) -> tuple[str, str | None]:
    import zernio

    client = zernio.Zernio(api_key=api_key)

    # 1. Upload media to Zernio's CDN.
    upload = client.media.upload(file_path=file_path)
    media_url = getattr(upload, "url", None) or getattr(upload, "media_url", None)
    if not media_url:
        # Fall back to dict access for SDK versions that return a dict.
        media_url = upload.model_dump().get("url") if hasattr(upload, "model_dump") else None
    if not media_url:
        raise RuntimeError(f"zernio.upload: no URL in response: {upload!r}")

    log.info(
        "zernio.media.uploaded",
        target_platform=target_platform,
        media_url=media_url,
        file_path=file_path,
    )

    # 2. Create + publish the post.
    kwargs: dict = dict(
        content=caption,
        platforms=[{"platform": target_platform, "accountId": account_id}],
        media_items=[{"type": "video", "url": media_url}],
        publish_now=True,
        hashtags=hashtags or None,
    )
    if title:
        kwargs["title"] = title
    if tiktok_settings is not None:
        kwargs["tiktok_settings"] = tiktok_settings

    resp = client.posts.create(**kwargs)
    post = getattr(resp, "post", None)
    post_id = getattr(post, "id", None) or getattr(post, "field_id", None)
    share_url = getattr(post, "url", None) or getattr(post, "share_url", None)

    if not post_id:
        # Best-effort extraction from model_dump for SDK variants.
        dump = resp.model_dump() if hasattr(resp, "model_dump") else {}
        post_dict = dump.get("post") or {}
        post_id = post_dict.get("id") or post_dict.get("_id") or str(uuid.uuid4())
        share_url = share_url or post_dict.get("url")

    log.info(
        "zernio.publish.done",
        target_platform=target_platform,
        zernio_post_id=post_id,
        share_url=share_url,
    )
    return post_id, share_url


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _read_caption(
    script_id: str, brand_id: str, cfg_block: dict
) -> tuple[str, str, list[str]]:
    """Pull the caption + title + hashtags out of the ContentScript row.

    Caption writer stores the full post text (including hashtags at the
    bottom) in ContentScript.script_body. We re-extract the hashtag list
    so the Zernio post carries them as structured fields too — some
    platforms render hashtags differently when they're separate vs
    embedded in the caption body.
    """
    factory = _session_factory()
    async with factory() as session:
        cs = await session.get(ContentScript, script_id) if script_id else None

    caption = (cs.script_body if cs else "").strip()
    # Title fallback: first sentence, capped.
    title = ""
    if caption:
        first = caption.split(".")[0][:100].strip()
        title = first or caption[:100].strip()
    else:
        title = brand_config(brand_id).get("display_name", brand_id)

    # Hashtags: anything token-like starting with "#" in the caption.
    hashtags: list[str] = []
    for tok in caption.split():
        if tok.startswith("#") and len(tok) > 1:
            hashtags.append(tok[1:].rstrip(".,!?").lower())
    if not hashtags:
        # Fall back to brand default_tags on the cfg block if present.
        hashtags = [
            t.lstrip("#").strip().lower()
            for t in (cfg_block.get("default_tags") or [])
            if t
        ]

    return caption, title, hashtags


def _tiktok_settings(cfg_block: dict) -> dict:
    """Map our brand config's TikTok prefs into Zernio's tiktok_settings dict."""
    return {
        "privacyLevel": cfg_block.get("default_privacy_level", "PUBLIC_TO_EVERYONE"),
        "disableDuet": bool(cfg_block.get("disable_duet", False)),
        "disableStitch": bool(cfg_block.get("disable_stitch", False)),
        "disableComment": bool(cfg_block.get("disable_comment", False)),
        "videoCoverTimestampMs": int(cfg_block.get("video_cover_timestamp_ms", 1000)),
    }
