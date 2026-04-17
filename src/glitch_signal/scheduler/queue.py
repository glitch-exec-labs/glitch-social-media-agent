"""Scheduler — Python port of glitch-cod-confirm/src/lib/scheduler.js.

Six concurrent tick functions run every SCHEDULER_TICK_MS (default 30s):

1. dispatch_video_jobs       — poll VideoJob(dispatched), call model.poll(), update status
2. check_shots_complete      — for ContentScript(generating), if all shots done → trigger assembler
3. promote_veto_windows      — ScheduledPost(pending_veto, deadline ≤ now) → queued
4. dispatch_scheduled_posts  — ScheduledPost(queued, scheduled_for ≤ now) → dispatching → publish
5. send_orm_auto_responses   — OrmResponse(pending_review, auto_send_at ≤ now) → send
6. sweep_stuck               — ScheduledPost(dispatching, last_attempt > 5min) → requeue

DISPATCH_MODE=dry_run: all ticks log intent but make no external calls.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import structlog
from sqlmodel import select

from glitch_signal.config import settings
from glitch_signal.db.models import (
    ContentScript,
    OrmResponse,
    ScheduledPost,
    VideoJob,
)
from glitch_signal.db.session import _session_factory

log = structlog.get_logger(__name__)

_running = False
_stop_event: asyncio.Event | None = None


def start() -> None:
    """Start the scheduler loop as a background asyncio task."""
    global _running, _stop_event
    if _running:
        return
    _running = True
    _stop_event = asyncio.Event()
    asyncio.create_task(_loop())
    log.info("scheduler.started", tick_ms=settings().scheduler_tick_ms)


def stop() -> None:
    global _running
    _running = False
    if _stop_event:
        _stop_event.set()
    log.info("scheduler.stopped")


async def _loop() -> None:
    tick_s = settings().scheduler_tick_ms / 1000
    while _running:
        try:
            await _tick()
        except Exception as exc:
            log.error("scheduler.tick_error", error=str(exc))
        await asyncio.sleep(tick_s)


async def _tick() -> None:
    await asyncio.gather(
        _dispatch_video_jobs(),
        _check_shots_complete(),
        _promote_veto_windows(),
        _dispatch_scheduled_posts(),
        _send_orm_auto_responses(),
        _sweep_stuck(),
        _poll_orm_mentions(),
        _reconcile_awaiting_webhook(),
        return_exceptions=True,
    )


# ---------------------------------------------------------------------------
# 1. dispatch_video_jobs — poll dispatched VideoJob rows
# ---------------------------------------------------------------------------

async def _dispatch_video_jobs() -> None:
    from glitch_signal.video_models.kling import get_model

    factory = _session_factory()
    async with factory() as session:
        result = await session.execute(
            select(VideoJob).where(VideoJob.status == "dispatched")
        )
        jobs = result.scalars().all()

    for job in jobs:
        if not job.api_job_id:
            continue
        try:
            model = get_model(job.model if job.model != "mock" else "kling_2")
            result_obj = await model.poll(job.api_job_id)

            factory = _session_factory()
            async with factory() as session:
                j = await session.get(VideoJob, job.id)
                if not j:
                    continue
                if result_obj.status == "done":
                    j.status = "done"
                    j.video_url = result_obj.video_url
                    j.completed_at = datetime.now(UTC).replace(tzinfo=None)
                    if result_obj.cost_usd:
                        j.cost_usd = result_obj.cost_usd
                elif result_obj.status == "failed":
                    j.status = "failed"
                    j.last_error = result_obj.error or "model reported failure"
                    j.completed_at = datetime.now(UTC).replace(tzinfo=None)
                else:
                    j.status = "dispatched"  # still processing
                session.add(j)
                await session.commit()

            if result_obj.status in ("done", "failed"):
                log.info(
                    "scheduler.video_job_updated",
                    job_id=job.id,
                    status=result_obj.status,
                )
        except Exception as exc:
            log.warning("scheduler.video_job_poll_error", job_id=job.id, error=str(exc))


# ---------------------------------------------------------------------------
# 2. check_shots_complete — trigger assembler when all shots done
# ---------------------------------------------------------------------------

async def _check_shots_complete() -> None:
    factory = _session_factory()
    async with factory() as session:
        result = await session.execute(
            select(ContentScript).where(ContentScript.status == "generating")
        )
        scripts = result.scalars().all()

    for cs in scripts:
        factory = _session_factory()
        async with factory() as session:
            result = await session.execute(
                select(VideoJob).where(VideoJob.script_id == cs.id)
            )
            jobs = result.scalars().all()

        if not jobs:
            continue

        failed = [j for j in jobs if j.status == "failed"]
        done = [j for j in jobs if j.status == "done"]

        if failed:
            log.warning(
                "scheduler.shots_failed",
                script_id=cs.id,
                n_failed=len(failed),
            )
            continue

        if len(done) == len(jobs):
            log.info("scheduler.all_shots_done", script_id=cs.id, n_shots=len(done))
            await _trigger_assembler(cs.id)


async def _trigger_assembler(script_id: str) -> None:
    """Re-invoke the LangGraph pipeline from the video_assembler node."""
    from glitch_signal.agent.graph import build_graph

    graph = build_graph()
    state = {"script_id": script_id, "all_shots_done": True}
    try:
        await graph.ainvoke(state, config={"entry_override": "video_assembler"})
    except Exception as exc:
        log.error("scheduler.assembler_trigger_failed", script_id=script_id, error=str(exc))


# ---------------------------------------------------------------------------
# 3. promote_veto_windows — pending_veto → queued when deadline passes
# ---------------------------------------------------------------------------

async def _promote_veto_windows() -> None:
    now = datetime.now(UTC).replace(tzinfo=None)
    factory = _session_factory()
    async with factory() as session:
        result = await session.execute(
            select(ScheduledPost).where(
                ScheduledPost.status == "pending_veto",
                ScheduledPost.veto_deadline <= now,
            )
        )
        posts = result.scalars().all()

        for sp in posts:
            sp.status = "queued"
            session.add(sp)
            log.info("scheduler.veto_window_expired", scheduled_post_id=sp.id)
        await session.commit()


# ---------------------------------------------------------------------------
# 4. dispatch_scheduled_posts — queued → dispatching → publish
# ---------------------------------------------------------------------------

async def _dispatch_scheduled_posts() -> None:
    now = datetime.now(UTC).replace(tzinfo=None)

    factory = _session_factory()
    async with factory() as session:
        # Atomically claim rows to prevent double-dispatch
        result = await session.execute(
            select(ScheduledPost).where(
                ScheduledPost.status == "queued",
                ScheduledPost.scheduled_for <= now,
            ).limit(10)
        )
        posts = result.scalars().all()

        for sp in posts:
            sp.status = "dispatching"
            sp.last_attempt_at = now
            session.add(sp)
        await session.commit()

    for sp in posts:
        if settings().is_dry_run:
            log.info("scheduler.dry_run_publish", scheduled_post_id=sp.id, platform=sp.platform)
            factory = _session_factory()
            async with factory() as session:
                s = await session.get(ScheduledPost, sp.id)
                if s:
                    s.status = "done"
                    await session.commit()
            continue

        try:
            from glitch_signal.agent.nodes.publisher import publish
            await publish(sp.id)
        except Exception as exc:
            log.error("scheduler.publish_error", scheduled_post_id=sp.id, error=str(exc))


# ---------------------------------------------------------------------------
# 5. send_orm_auto_responses
# ---------------------------------------------------------------------------

async def _send_orm_auto_responses() -> None:
    now = datetime.now(UTC).replace(tzinfo=None)
    factory = _session_factory()
    async with factory() as session:
        result = await session.execute(
            select(OrmResponse).where(
                OrmResponse.status == "pending_review",
                OrmResponse.auto_send_at <= now,
            )
        )
        responses = result.scalars().all()

    for orm_resp in responses:
        try:
            from glitch_signal.orm.responder import send_approved_response
            await send_approved_response(orm_resp.id)
        except Exception as exc:
            log.warning(
                "scheduler.orm_auto_send_error",
                orm_resp_id=orm_resp.id,
                error=str(exc),
            )


# ---------------------------------------------------------------------------
# 6. sweep_stuck — dispatching > 5min → requeue with backoff
# ---------------------------------------------------------------------------

async def _sweep_stuck() -> None:
    stuck_after_s = settings().scheduler_stuck_after_ms / 1000
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=stuck_after_s)

    factory = _session_factory()
    async with factory() as session:
        result = await session.execute(
            select(ScheduledPost).where(
                ScheduledPost.status == "dispatching",
                ScheduledPost.last_attempt_at <= cutoff,
            )
        )
        stuck = result.scalars().all()

        for sp in stuck:
            log.warning("scheduler.stuck_job", scheduled_post_id=sp.id, attempts=sp.attempts)
            now = datetime.now(UTC).replace(tzinfo=None)
            s = settings()

            if sp.attempts >= 3:
                sp.status = "failed"
                sp.last_error = "max retries exceeded"
            elif sp.attempts == 2:
                sp.status = "queued"
                sp.scheduled_for = now + timedelta(milliseconds=s.publish_retry_2_ms)
            else:
                sp.status = "queued"
                sp.scheduled_for = now + timedelta(milliseconds=s.publish_retry_1_ms)

            session.add(sp)
        await session.commit()


# ---------------------------------------------------------------------------
# 7. reconcile_awaiting_webhook — fallback for Upload-Post webhooks that
# don't arrive within the reconcile window. Polls get_status once and
# finalizes the ScheduledPost if the vendor has published.
# ---------------------------------------------------------------------------

async def _reconcile_awaiting_webhook() -> None:
    import uuid as _uuid

    from glitch_signal.db.models import PublishedPost
    from glitch_signal.platforms.upload_post import (
        _PLATFORM_MAP,
        poll_status_for_request,
    )

    s = settings()
    window_s = s.upload_post_webhook_reconcile_after_s
    if window_s <= 0:
        return

    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=window_s)

    factory = _session_factory()
    async with factory() as session:
        result = await session.execute(
            select(ScheduledPost).where(
                ScheduledPost.status == "awaiting_webhook",
                ScheduledPost.last_attempt_at <= cutoff,
            ).limit(10)
        )
        candidates = result.scalars().all()

    for sp in candidates:
        target = _PLATFORM_MAP.get(sp.platform)
        if not target or not sp.vendor_request_id:
            continue
        try:
            ppid, url = await poll_status_for_request(sp.vendor_request_id, target)
        except Exception as exc:
            log.warning(
                "scheduler.reconcile_awaiting_webhook_error",
                scheduled_post_id=sp.id,
                request_id=sp.vendor_request_id,
                error=str(exc)[:200],
            )
            # Mark failed on hard error from the vendor; keep otherwise so
            # the next tick can retry.
            factory = _session_factory()
            async with factory() as session:
                s_row = await session.get(ScheduledPost, sp.id)
                if s_row:
                    s_row.status = "failed"
                    s_row.last_error = str(exc)[:1000]
                    session.add(s_row)
                    await session.commit()
            continue

        if not ppid and not url:
            # Still in flight — leave awaiting_webhook; try again next tick.
            continue

        factory = _session_factory()
        async with factory() as session:
            s_row = await session.get(ScheduledPost, sp.id)
            if not s_row:
                continue
            existing = (await session.execute(
                select(PublishedPost).where(PublishedPost.scheduled_post_id == sp.id)
            )).scalar_one_or_none()
            if existing:
                s_row.status = "done"
                session.add(s_row)
                await session.commit()
                continue
            pub = PublishedPost(
                id=str(_uuid.uuid4()),
                brand_id=s_row.brand_id,
                scheduled_post_id=s_row.id,
                platform=s_row.platform,
                platform_post_id=ppid or sp.vendor_request_id,
                platform_url=url,
                published_at=datetime.now(UTC).replace(tzinfo=None),
            )
            s_row.status = "done"
            session.add(pub)
            session.add(s_row)
            await session.commit()
            log.info(
                "scheduler.reconcile_awaiting_webhook_finalized",
                scheduled_post_id=sp.id,
                platform_post_id=pub.platform_post_id,
                via="get_status",
            )


# ---------------------------------------------------------------------------
# 8. poll_orm_mentions — Phase 1 Twitter monitor
# ---------------------------------------------------------------------------

async def _poll_orm_mentions() -> None:
    try:
        from glitch_signal.orm.monitor import poll_all
        new = await poll_all()
        if new:
            log.info("scheduler.orm_poll", new_mentions=new)
    except Exception as exc:
        log.warning("scheduler.orm_poll_error", error=str(exc))
