"""Post one queued row from the sheet → Upload-Post → write result back.

Called by the scheduler tick in scheduler/queue.py. Idempotency comes from
the sheet's id column: every call flips the row to posted|failed so the
next tick won't re-fire it.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import structlog

from glitch_signal.config import brand_config, settings
from glitch_signal.db.models import PublishedPost, ScheduledPost
from glitch_signal.db.session import _session_factory
from glitch_signal.integrations.google_sheets import update_row_by_key
from glitch_signal.sheet_posting.reader import SHEET_COLUMNS, QueuedPost

log = structlog.get_logger(__name__)


async def post_one(row: QueuedPost) -> tuple[bool, str]:
    """Publish a single queued row. Returns (ok, message).

    On success, updates the sheet with posted_at / post_url / platform_post_id
    and flips status to posted. On failure, flips to failed with the error
    in notes.
    """
    cfg = brand_config(row.brand_id)
    block = (cfg.get("platforms", {}) or {}).get(row.platform) or {}
    user = block.get("user")
    if not block.get("enabled") or not user:
        return await _mark_failed(row, f"{row.brand_id}.{row.platform} not enabled / missing user")

    api_key = settings().upload_post_api_key
    if not api_key:
        return await _mark_failed(row, "UPLOAD_POST_API_KEY unset")

    if settings().is_dry_run:
        log.info(
            "sheet_posting.dry_run",
            row_id=row.id,
            brand_id=row.brand_id,
            platform=row.platform,
        )
        await _write_result(
            row,
            status="posted",
            post_url="https://dry-run.local/fake",
            platform_post_id=f"dry-{row.id[:8]}",
        )
        return True, "[dry-run] marked posted"

    target = row.platform.replace("upload_post_", "")

    try:
        resp = await asyncio.to_thread(
            _post_to_upload_post,
            api_key=api_key,
            user=user,
            target=target,
            text=row.body.strip(),
            target_linkedin_page_id=block.get("target_linkedin_page_id") if target == "linkedin" else None,
        )
    except Exception as exc:
        log.warning("sheet_posting.upload_failed", row_id=row.id, error=str(exc)[:200])
        return await _mark_failed(row, f"upload_text failed: {exc}")

    # Normalize the response — same handling as the foundation/launch scripts
    platform_post_id, post_url = _extract_post_identifiers(resp, target)
    pending = platform_post_id is None and post_url is None
    status = "posted" if not pending else "posted"  # background-accepted counts as posted

    await _write_result(
        row,
        status=status,
        post_url=post_url or "",
        platform_post_id=platform_post_id or "",
        extra_note=(
            "background upload — status confirmed via webhook / reconcile" if pending else ""
        ),
    )

    # Also write a PublishedPost row so downstream features (comment sweeper,
    # analytics) can find this post. We create a synthetic ScheduledPost
    # first because PublishedPost FKs to it; text posts use the nullable
    # asset_id path added in migration 0006.
    if platform_post_id:
        try:
            await _write_audit_rows(row, platform_post_id, post_url)
        except Exception as exc:
            log.warning("sheet_posting.audit_write_failed", row_id=row.id, error=str(exc)[:200])

    log.info(
        "sheet_posting.posted",
        row_id=row.id,
        brand_id=row.brand_id,
        platform=row.platform,
        platform_post_id=platform_post_id,
    )
    return True, "posted"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _post_to_upload_post(
    *,
    api_key: str,
    user: str,
    target: str,
    text: str,
    target_linkedin_page_id: str | None,
) -> dict:
    import upload_post

    client = upload_post.UploadPostClient(api_key=api_key)
    kwargs: dict = {"title": text, "user": user, "platforms": [target]}
    if target == "linkedin" and target_linkedin_page_id:
        kwargs["target_linkedin_page_id"] = target_linkedin_page_id
    return client.upload_text(**kwargs)


def _extract_post_identifiers(resp: dict, target: str) -> tuple[str | None, str | None]:
    """Pull (platform_post_id, post_url) out of Upload-Post's response shape."""
    if not isinstance(resp, dict):
        return None, None
    results = resp.get("results") or {}
    if isinstance(results, dict):
        block = results.get(target) or {}
    elif isinstance(results, list) and results:
        block = results[0] if isinstance(results[0], dict) else {}
    else:
        block = {}
    pid = (
        block.get("platform_post_id")
        or block.get("post_id")
        or (block.get("url", "").rsplit("/", 1)[-1] if block.get("url") else None)
    )
    url = block.get("post_url") or block.get("url")
    return pid, url


async def _write_result(
    row: QueuedPost,
    *,
    status: str,
    post_url: str = "",
    platform_post_id: str = "",
    extra_note: str = "",
) -> None:
    s = settings()
    now = datetime.now(UTC).replace(tzinfo=None)
    updates = {
        "status": status,
        "posted_at": now.isoformat(timespec="seconds"),
        "post_url": post_url,
        "platform_post_id": platform_post_id,
    }
    if extra_note:
        existing = row.notes or ""
        updates["notes"] = (existing + ("; " if existing else "") + extra_note).strip()
    try:
        await update_row_by_key(
            sheet_id=s.glitch_posts_sheet_id,
            worksheet=s.glitch_posts_worksheet,
            columns=SHEET_COLUMNS,
            key_column="id",
            key_value=row.id,
            updates=updates,
        )
    except Exception as exc:
        log.error("sheet_posting.sheet_update_failed", row_id=row.id, error=str(exc)[:200])


async def _mark_failed(row: QueuedPost, reason: str) -> tuple[bool, str]:
    await _write_result(row, status="failed", extra_note=reason[:180])
    return False, reason


async def _write_audit_rows(
    row: QueuedPost, platform_post_id: str, post_url: str | None
) -> None:
    """Write ScheduledPost + PublishedPost so comment sweeper / analytics see this post."""
    import uuid

    now = datetime.now(UTC).replace(tzinfo=None)
    factory = _session_factory()
    async with factory() as session:
        sp = ScheduledPost(
            id=str(uuid.uuid4()),
            brand_id=row.brand_id,
            asset_id=None,
            script_id=None,
            platform=row.platform,
            scheduled_for=now,
            status="done",
            veto_deadline=now,
            attempts=1,
            last_attempt_at=now,
        )
        session.add(sp)
        await session.flush()

        pp = PublishedPost(
            id=str(uuid.uuid4()),
            brand_id=row.brand_id,
            scheduled_post_id=sp.id,
            platform=row.platform,
            platform_post_id=platform_post_id,
            platform_url=post_url,
            published_at=now,
        )
        session.add(pp)
        await session.commit()
