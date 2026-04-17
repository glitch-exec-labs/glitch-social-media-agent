"""Publisher node — routes ScheduledPost to the correct platform client.

Invoked by scheduler/queue.py, not the LangGraph graph directly.
On success: writes PublishedPost, marks ScheduledPost done.
On failure: applies retry backoff (same pattern as cod-confirm handleFailure).
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy import select

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

        # Idempotency guard — a prior attempt may have written PublishedPost
        # but crashed before flipping scheduled_post.status to "done" (process
        # kill, DB commit blip, etc.). If we see a PublishedPost row, the
        # vendor already posted; do not ask it to post again.
        result = await session.execute(
            select(PublishedPost).where(
                PublishedPost.scheduled_post_id == scheduled_post_id
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            log.info(
                "publisher.already_published",
                scheduled_post_id=scheduled_post_id,
                platform_post_id=existing.platform_post_id,
            )
            sp.status = "done"
            session.add(sp)
            await session.commit()
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
        attempts_before_call = sp.attempts

    try:
        brand_id = getattr(sp, "brand_id", None) or getattr(asset, "brand_id", None)
        # Pre-publish ffmpeg transforms (brand-config driven). Returns the
        # original path unchanged for brands with no `media_pipeline`
        # entry for this platform — zero cost on the common path.
        from glitch_signal.media.ffmpeg import apply_transforms
        publish_file_path = await apply_transforms(
            asset.file_path, brand_id or "", sp.platform
        )
        platform_post_id, platform_url = await _publish_to_platform(
            sp.platform,
            publish_file_path,
            asset.script_id,
            brand_id=brand_id,
            attempts=attempts_before_call,
        )
    except Exception as exc:
        log.error("publisher.failed", scheduled_post_id=scheduled_post_id, error=str(exc))
        await _handle_failure(scheduled_post_id, str(exc))
        return

    # Vendor-async handoff: publishers that finish over webhooks return a
    # `webhook_pending:<request_id>` sentinel instead of a real post_id.
    # We persist the request_id so the webhook handler / reconciliation
    # sweep can correlate the callback back to this ScheduledPost, and
    # flip status to `awaiting_webhook` so the scheduler stops trying to
    # republish it.
    from glitch_signal.platforms.upload_post import (
        extract_request_id,
        is_webhook_pending,
    )
    if is_webhook_pending(platform_post_id):
        request_id = extract_request_id(platform_post_id)
        factory = _session_factory()
        async with factory() as session:
            sp = await session.get(ScheduledPost, scheduled_post_id)
            if sp:
                sp.status = "awaiting_webhook"
                sp.vendor_request_id = request_id
                session.add(sp)
                await session.commit()
        log.info(
            "publisher.awaiting_webhook",
            scheduled_post_id=scheduled_post_id,
            vendor_request_id=request_id,
        )
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
    attempts: int = 1,
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

    if platform.startswith("upload_post_"):
        from glitch_signal.platforms.upload_post import publish as upload_post_publish
        return await upload_post_publish(
            platform, file_path, script_id, brand_id=brand_id, attempts=attempts
        )

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
