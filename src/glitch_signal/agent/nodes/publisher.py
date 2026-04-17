"""Publisher node — routes ScheduledPost to the correct platform client.

Invoked by scheduler/queue.py, not the LangGraph graph directly.
On success: writes PublishedPost, marks ScheduledPost done.
On failure: applies retry backoff (same pattern as cod-confirm handleFailure).
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import structlog

from glitch_signal.config import settings
from glitch_signal.db.models import PublishedPost, ScheduledPost, VideoAsset
from glitch_signal.db.session import _session_factory

log = structlog.get_logger(__name__)


async def publish(scheduled_post_id: str) -> None:
    """Entry point called by the scheduler tick."""
    factory = _session_factory()
    async with factory() as session:
        sp = await session.get(ScheduledPost, scheduled_post_id)
        if not sp:
            log.error("publisher.not_found", scheduled_post_id=scheduled_post_id)
            return

        asset = await session.get(VideoAsset, sp.asset_id)
        if not asset:
            await _mark_failed(session, sp, "VideoAsset not found")
            return

        sp.status = "dispatching"
        sp.attempts += 1
        sp.last_attempt_at = datetime.now(UTC).replace(tzinfo=None)
        session.add(sp)
        await session.commit()

    try:
        brand_id = getattr(sp, "brand_id", None) or getattr(asset, "brand_id", None)
        platform_post_id, platform_url = await _publish_to_platform(
            sp.platform, asset.file_path, asset.script_id, brand_id=brand_id
        )
    except Exception as exc:
        log.error("publisher.failed", scheduled_post_id=scheduled_post_id, error=str(exc))
        await _handle_failure(scheduled_post_id, str(exc))
        return

    factory = _session_factory()
    async with factory() as session:
        sp = await session.get(ScheduledPost, scheduled_post_id)
        if sp:
            sp.status = "done"
            session.add(sp)

        pub = PublishedPost(
            id=str(uuid.uuid4()),
            brand_id=getattr(sp, "brand_id", "glitch_executor") if sp else "glitch_executor",
            scheduled_post_id=scheduled_post_id,
            platform=sp.platform if sp else "unknown",
            platform_post_id=platform_post_id,
            platform_url=platform_url,
            published_at=datetime.now(UTC).replace(tzinfo=None),
        )
        session.add(pub)
        await session.commit()

    log.info(
        "publisher.done",
        scheduled_post_id=scheduled_post_id,
        platform_post_id=platform_post_id,
        url=platform_url,
    )


async def _publish_to_platform(
    platform: str,
    file_path: str,
    script_id: str,
    brand_id: str | None = None,
) -> tuple[str, str | None]:
    if settings().is_dry_run:
        log.info("publisher.dry_run", platform=platform, file_path=file_path, brand_id=brand_id)
        return f"dry-run-{uuid.uuid4().hex[:8]}", None

    if platform == "youtube_shorts":
        from glitch_signal.platforms.youtube import upload_short
        return await upload_short(file_path, script_id, brand_id=brand_id)

    if platform == "tiktok":
        from glitch_signal.platforms.tiktok import publish as tiktok_publish
        return await tiktok_publish(file_path, script_id, brand_id=brand_id)

    if platform.startswith("zernio_"):
        from glitch_signal.platforms.zernio import publish as zernio_publish
        return await zernio_publish(platform, file_path, script_id, brand_id=brand_id)

    if platform == "twitter":
        from glitch_signal.platforms.twitter import post_video
        return await post_video(file_path, script_id)

    if platform == "instagram_reels":
        from glitch_signal.platforms.instagram import post_reel
        return await post_reel(file_path, script_id)

    raise ValueError(f"Unknown platform: {platform!r}")


async def _handle_failure(scheduled_post_id: str, error: str) -> None:
    factory = _session_factory()
    async with factory() as session:
        sp = await session.get(ScheduledPost, scheduled_post_id)
        if not sp:
            return

        sp.last_error = error[:1000]
        s = settings()
        now = datetime.now(UTC).replace(tzinfo=None)

        if sp.attempts == 1:
            delay_ms = s.publish_retry_1_ms
        elif sp.attempts == 2:
            delay_ms = s.publish_retry_2_ms
        else:
            sp.status = "failed"
            session.add(sp)
            await session.commit()
            return

        sp.status = "queued"
        sp.scheduled_for = now + timedelta(milliseconds=delay_ms)
        session.add(sp)
        await session.commit()


async def _mark_failed(session, sp: ScheduledPost, reason: str) -> None:
    sp.status = "failed"
    sp.last_error = reason
    session.add(sp)
    await session.commit()
