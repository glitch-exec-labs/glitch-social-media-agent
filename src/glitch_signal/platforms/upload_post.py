"""Upload-Post publisher — multi-platform social posting via an audited partner app.

Third publisher in the repo, alongside:
  - platforms/tiktok.py  (direct-post to TikTok, blocked for unaudited apps)
  - platforms/zernio.py  (another audited vendor)

Why Upload-Post in addition to Zernio:
  - $16/mo (annual) Basic tier gives unlimited uploads across 5 profiles — cheaper
    than Zernio for real volume
  - Richer API surface: comments read+reply, per-platform analytics,
    scheduled-post management, webhooks on publish success/failure
  - Accepts both local paths AND URLs on upload_video → we reuse the
    signed /media/fetch URL pattern built for Zernio. Zero re-upload.

Platform-key convention mirrors zernio_*:
  upload_post_tiktok, upload_post_instagram, upload_post_youtube, …

Per-brand config lives under platforms.upload_post_<target> and must carry:
  - enabled: true
  - user: <Upload-Post profile username, e.g. "Namhya">

DISPATCH_MODE=dry_run short-circuits without calling the SDK.
"""
from __future__ import annotations

import asyncio
import pathlib
import uuid

import structlog

from glitch_signal.config import brand_config, settings
from glitch_signal.crypto import make_state_token
from glitch_signal.db.models import ContentScript
from glitch_signal.db.session import _session_factory

log = structlog.get_logger(__name__)

# Map our platform-key suffixes to Upload-Post's platform enum values.
# Upload-Post's canonical platform names (from their SDK):
#   tiktok, instagram, youtube, linkedin, facebook, pinterest, threads,
#   bluesky, x, reddit, google_business
_PLATFORM_MAP = {
    "upload_post_tiktok":    "tiktok",
    "upload_post_instagram": "instagram",
    "upload_post_youtube":   "youtube",
    "upload_post_linkedin":  "linkedin",
    "upload_post_facebook":  "facebook",
    "upload_post_x":         "x",
    "upload_post_threads":   "threads",
    "upload_post_pinterest": "pinterest",
    "upload_post_bluesky":   "bluesky",
    "upload_post_reddit":    "reddit",
}

# TTL for the signed URL we hand Upload-Post. Their upload worker usually
# fetches within seconds; 1 hour is a very generous ceiling.
_MEDIA_URL_TTL_S = 60 * 60


async def publish(
    platform: str,
    file_path: str,
    script_id: str,
    brand_id: str | None = None,
) -> tuple[str, str | None]:
    """Publish a video via Upload-Post. Returns (provider_post_id, share_url|None)."""
    s = settings()

    if s.is_dry_run:
        fake_id = f"uploadpost-dry-{uuid.uuid4().hex[:10]}"
        log.info(
            "upload_post.publish.dry_run",
            publish_id=fake_id,
            file_path=file_path,
            brand_id=brand_id,
            platform=platform,
        )
        return fake_id, None

    if not brand_id:
        raise ValueError("upload_post.publish: brand_id is required for live publish")
    if not s.upload_post_api_key:
        raise RuntimeError("UPLOAD_POST_API_KEY is not set")

    target = _PLATFORM_MAP.get(platform)
    if not target:
        raise ValueError(f"upload_post.publish: unknown platform key {platform!r}")

    cfg_block = (
        brand_config(brand_id).get("platforms", {}).get(platform, {}) or {}
    )
    user = cfg_block.get("user")
    if not user:
        raise RuntimeError(
            f"upload_post.publish: brand={brand_id!r} missing "
            f"platforms.{platform}.user — the Upload-Post managed-user profile name"
        )

    path = pathlib.Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"upload_post.publish: file missing: {file_path}")

    caption, title, _hashtags = await _read_caption(script_id, brand_id, cfg_block)
    video_url = _build_signed_media_url(path)

    log.info(
        "upload_post.publish.media_url_issued",
        brand_id=brand_id,
        file_path=str(path),
        media_url_host=video_url.split("/")[2] if "://" in video_url else video_url,
        target=target,
        user=user,
    )

    # Upload-Post SDK is blocking requests-based → run in a thread.
    return await asyncio.to_thread(
        _publish_sync,
        api_key=s.upload_post_api_key,
        user=user,
        target_platform=target,
        video_url=video_url,
        caption=caption,
        title=title,
        extras=_platform_extras(target, cfg_block),
    )


# ---------------------------------------------------------------------------
# Blocking worker
# ---------------------------------------------------------------------------

def _publish_sync(
    *,
    api_key: str,
    user: str,
    target_platform: str,
    video_url: str,
    caption: str,
    title: str,
    extras: dict,
) -> tuple[str, str | None]:
    import upload_post

    client = upload_post.UploadPostClient(api_key=api_key)

    # Upload-Post accepts either a local path OR a URL for video_path.
    # We pass our signed URL so the 80+ MB file isn't re-streamed from our
    # server through Python → their API. Upload-Post fetches directly.
    kwargs: dict = dict(
        video_path=video_url,
        title=title or None,
        user=user,
        platforms=[target_platform],
    )
    # Caption differs per platform. TikTok/IG accept `description` or the
    # title field as body text. We put the full caption in `description` so
    # hashtags render correctly on the platforms that need them.
    if caption:
        kwargs["description"] = caption
    kwargs.update(extras)

    resp = client.upload_video(**kwargs)

    # Upload-Post returns a dict with success flag + per-platform status and
    # a request_id used for status polling. Shape example:
    #   {"success": True, "request_id": "...", "status": "queued", "results": {...}}
    if not resp.get("success", True):
        raise RuntimeError(f"Upload-Post upload_video failed: {resp}")

    request_id = (
        resp.get("request_id")
        or (resp.get("results", {}) or {}).get("request_id")
        or str(uuid.uuid4())
    )
    share_url = _extract_share_url(resp, target_platform)

    log.info(
        "upload_post.publish.done",
        target=target_platform,
        user=user,
        request_id=request_id,
        share_url=share_url,
    )
    return request_id, share_url


def _extract_share_url(resp: dict, target_platform: str) -> str | None:
    """Best-effort extraction of the published post URL from the SDK response.

    Upload-Post's response shape varies by async/sync mode. Try a few paths
    before giving up.
    """
    # Sync publish response
    results = resp.get("results") or {}
    per_plat = results.get(target_platform) or {}
    for key in ("url", "post_url", "share_url", "permalink"):
        val = per_plat.get(key)
        if val:
            return str(val)
    # Sometimes URL is at top-level
    for key in ("url", "post_url", "share_url"):
        val = resp.get(key)
        if val:
            return str(val)
    return None


# ---------------------------------------------------------------------------
# Platform-specific extras — pull TikTok/IG/YT settings from brand cfg
# ---------------------------------------------------------------------------

def _platform_extras(target: str, cfg_block: dict) -> dict:
    """Map brand config keys to Upload-Post's platform-specific kwargs."""
    if target == "tiktok":
        extras: dict = {
            "privacy_level": cfg_block.get("default_privacy_level", "PUBLIC_TO_EVERYONE"),
            "disable_duet": bool(cfg_block.get("disable_duet", False)),
            "disable_stitch": bool(cfg_block.get("disable_stitch", False)),
            "disable_comment": bool(cfg_block.get("disable_comment", False)),
            "cover_timestamp": int(cfg_block.get("video_cover_timestamp_ms", 1000)),
        }
        if cfg_block.get("post_mode"):
            extras["post_mode"] = cfg_block["post_mode"]
        if cfg_block.get("is_aigc") is not None:
            extras["is_aigc"] = bool(cfg_block["is_aigc"])
        return extras
    if target == "instagram":
        return {
            "media_type": cfg_block.get("media_type", "REELS"),
            **({"share_to_feed": True} if cfg_block.get("share_to_feed", True) else {}),
        }
    if target == "youtube":
        return {
            "privacyStatus": cfg_block.get("privacy_status", "public"),
            "categoryId": cfg_block.get("category_id", "22"),
        }
    return {}


# ---------------------------------------------------------------------------
# Signed URL (same HMAC scheme used by platforms/zernio.py)
# ---------------------------------------------------------------------------

def _build_signed_media_url(local_path: pathlib.Path) -> str:
    """Return an HMAC-signed public URL served by /media/fetch."""
    s = settings()
    token = make_state_token(
        {"p": str(local_path.resolve()), "k": "media"},
        ttl_s=_MEDIA_URL_TTL_S,
    )
    base = s.media_public_base_url.rstrip("/")
    return f"{base}/media/fetch?token={token}"


# ---------------------------------------------------------------------------
# Caption + title extraction from ContentScript
# ---------------------------------------------------------------------------

async def _read_caption(
    script_id: str, brand_id: str, cfg_block: dict
) -> tuple[str, str, list[str]]:
    """Pull caption + title + hashtags out of the ContentScript row."""
    factory = _session_factory()
    async with factory() as session:
        cs = await session.get(ContentScript, script_id) if script_id else None

    caption = (cs.script_body if cs else "").strip()
    title = ""
    if caption:
        first = caption.split(".")[0][:100].strip()
        title = first or caption[:100].strip()
    else:
        title = brand_config(brand_id).get("display_name", brand_id)

    hashtags: list[str] = []
    for tok in caption.split():
        if tok.startswith("#") and len(tok) > 1:
            hashtags.append(tok[1:].rstrip(".,!?").lower())
    if not hashtags:
        hashtags = [
            t.lstrip("#").strip().lower()
            for t in (cfg_block.get("default_tags") or [])
            if t
        ]

    return caption, title, hashtags
