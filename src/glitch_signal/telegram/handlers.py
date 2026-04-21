"""Telegram command and callback handlers for Glitch Social Media Agent."""
from __future__ import annotations

import pathlib
from datetime import UTC, datetime

import structlog
from sqlmodel import select
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from glitch_signal.config import brand_config, brand_ids, settings
from glitch_signal.db.models import (
    MentionEvent,
    OrmResponse,
    ScheduledPost,
    Signal,
    VideoAsset,
    VideoJob,
)
from glitch_signal.db.session import _session_factory

log = structlog.get_logger(__name__)


def _is_admin(update: Update) -> bool:
    if update.effective_user is None:
        return False
    return update.effective_user.id in settings().admin_telegram_ids


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return
    await update.message.reply_text(
        "Glitch Social Media Agent online.\n"
        "Commands: /help /status /signals /orm\n"
        "Approve/veto via inline buttons or /approve <id> /veto <id>"
    )


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return
    await update.message.reply_text(
        "/status          — queue depth, last signal, cost this week\n"
        "/signals         — last 5 signals with novelty score\n"
        "/preview <id>    — re-send a video preview\n"
        "/approve <id>    — approve a pending_veto post immediately\n"
        "/veto <id>       — veto a pending_veto post\n"
        "/orm             — last 10 mention events with tier\n"
        "/orm_approve <id> — approve a pending_review ORM response\n"
        "/orm_veto <id>   — veto a pending_review ORM response\n"
        "/reply <url> [brand|founder]       — draft a reply to an X/LinkedIn post\n"
        "/reply_with_text <url> :: <text>   — same, but paste the post text inline"
    )


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return

    factory = _session_factory()
    async with factory() as session:
        pending_veto_r = await session.execute(
            select(ScheduledPost).where(ScheduledPost.status == "pending_veto")
        )
        queued_r = await session.execute(
            select(ScheduledPost).where(ScheduledPost.status == "queued")
        )
        signals_r = await session.execute(
            select(Signal).where(Signal.status == "queued").limit(1)
        )
        cost_r = await session.execute(select(VideoJob))

    pending_veto_rows = pending_veto_r.scalars().all()
    queued_rows = queued_r.scalars().all()
    last_signal = signals_r.scalar_one_or_none()
    all_jobs = cost_r.scalars().all()

    # Cost this week (rough — sum all job costs, not week-filtered)
    total_cost = sum(j.cost_usd or 0.0 for j in all_jobs)

    configured_brands = brand_ids()
    lines = ["Glitch Social Media Agent status"]

    if len(configured_brands) > 1:
        # Multi-brand: break counts down by brand so the operator can tell at
        # a glance which brand is driving the queue depth.
        lines.append(f"Brands configured: {', '.join(configured_brands)}")
        for bid in configured_brands:
            display = brand_config(bid).get("display_name", bid)
            pv = sum(1 for sp in pending_veto_rows if getattr(sp, "brand_id", None) == bid)
            q  = sum(1 for sp in queued_rows       if getattr(sp, "brand_id", None) == bid)
            lines.append(f"  [{display}] pending_veto={pv} queued={q}")
    else:
        lines.append(f"Pending veto: {len(pending_veto_rows)}")
        lines.append(f"Queued to publish: {len(queued_rows)}")

    lines.append(f"Last signal: {last_signal.summary[:60] if last_signal else 'none'}")
    lines.append(f"Total LLM+video cost: ${total_cost:.2f}")

    await update.message.reply_text("\n".join(lines))


async def cmd_signals(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return

    factory = _session_factory()
    async with factory() as session:
        result = await session.execute(
            select(Signal).order_by(Signal.created_at.desc()).limit(5)
        )
    signals = result.scalars().all()

    if not signals:
        await update.message.reply_text("No signals found.")
        return

    lines = []
    for sig in signals:
        lines.append(
            f"[{sig.status}] {sig.novelty_score:.2f} — {sig.summary[:80]}\n"
            f"  {sig.source}:{sig.source_ref[:12]}"
        )
    await update.message.reply_text("\n\n".join(lines))


async def cmd_preview(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return
    args = ctx.args or []
    if not args:
        await update.message.reply_text("Usage: /preview <scheduled_post_id>")
        return

    sp_id = args[0]
    factory = _session_factory()
    async with factory() as session:
        sp = await session.get(ScheduledPost, sp_id)
        asset = await session.get(VideoAsset, sp.asset_id) if sp else None

    if not sp or not asset:
        await update.message.reply_text(f"No scheduled post found for id {sp_id[:8]}")
        return

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Approve now", callback_data=f"approve:{sp_id}"),
        InlineKeyboardButton("Veto", callback_data=f"veto:{sp_id}"),
    ]])

    file_path = pathlib.Path(asset.file_path)
    if not file_path.exists():
        await update.message.reply_text(
            f"Asset file not found: {asset.file_path}", reply_markup=keyboard
        )
        return

    file_size = file_path.stat().st_size
    caption = f"Preview — {sp.platform}\nStatus: {sp.status}\nID: {sp_id[:8]}"

    if file_size > 50 * 1024 * 1024:
        await update.message.reply_text(
            f"{caption}\n\nFile too large ({file_size // 1024 // 1024}MB) for Telegram.\nPath: {asset.file_path}",
            reply_markup=keyboard,
        )
    else:
        with open(asset.file_path, "rb") as f:
            await update.message.reply_video(
                video=f, caption=caption, reply_markup=keyboard
            )


async def cmd_approve(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return
    args = ctx.args or []
    if not args:
        await update.message.reply_text("Usage: /approve <scheduled_post_id>")
        return
    await _approve_scheduled_post(args[0], update)


async def cmd_veto(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return
    args = ctx.args or []
    if not args:
        await update.message.reply_text("Usage: /veto <scheduled_post_id>")
        return
    await _veto_scheduled_post(args[0], update)


async def cmd_orm(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return

    factory = _session_factory()
    async with factory() as session:
        result = await session.execute(
            select(MentionEvent).order_by(MentionEvent.received_at.desc()).limit(10)
        )
    events = result.scalars().all()

    if not events:
        await update.message.reply_text("No mention events found.")
        return

    lines = []
    for e in events:
        guardrail = " GUARDRAIL" if e.guardrail_hit else ""
        lines.append(
            f"[{e.tier or '?'}]{guardrail} @{e.from_handle} ({e.platform})\n"
            f"  {e.body[:80]}"
        )
    await update.message.reply_text("\n\n".join(lines))


async def cmd_orm_approve(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return
    args = ctx.args or []
    if not args:
        await update.message.reply_text("Usage: /orm_approve <response_id>")
        return
    await _approve_orm_response(args[0], update)


async def cmd_orm_veto(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update):
        return
    args = ctx.args or []
    if not args:
        await update.message.reply_text("Usage: /orm_veto <response_id>")
        return
    await _veto_orm_response(args[0], update)


# ---------------------------------------------------------------------------
# Inline keyboard callback router
# ---------------------------------------------------------------------------

async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if not _is_admin(update):
        return

    data = query.data or ""
    if data.startswith("approve:"):
        sp_id = data[8:]
        await _approve_scheduled_post(sp_id, update, query=query)
    elif data.startswith("veto:"):
        sp_id = data[5:]
        await _veto_scheduled_post(sp_id, update, query=query)
    elif data.startswith("orm_approve:"):
        resp_id = data[12:]
        await _approve_orm_response(resp_id, update, query=query)
    elif data.startswith("orm_veto:"):
        resp_id = data[9:]
        await _veto_orm_response(resp_id, update, query=query)
    elif data.startswith("rply_a:"):
        reply_id = data[7:]
        await _approve_comment_reply(reply_id, update, query=query)
    elif data.startswith("rply_v:"):
        reply_id = data[7:]
        await _veto_comment_reply(reply_id, update, query=query)
    elif data.startswith("strply_a:"):
        sr_id = data[9:]
        await _approve_strategic_reply(sr_id, update, query=query)
    elif data.startswith("strply_v:"):
        sr_id = data[9:]
        await _veto_strategic_reply(sr_id, update, query=query)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _approve_scheduled_post(sp_id: str, update, query=None) -> None:
    factory = _session_factory()
    async with factory() as session:
        sp = await session.get(ScheduledPost, sp_id)
        if not sp:
            text = f"Scheduled post {sp_id[:8]} not found."
        elif sp.status not in ("pending_veto", "queued"):
            text = f"Post {sp_id[:8]} is already {sp.status}."
        else:
            sp.status = "queued"
            sp.scheduled_for = datetime.now(UTC).replace(tzinfo=None)
            session.add(sp)
            await session.commit()
            text = f"Post {sp_id[:8]} approved — queued for immediate publish."

    reply = query.edit_message_text if query else update.message.reply_text
    await reply(text)


async def _veto_scheduled_post(sp_id: str, update, query=None) -> None:
    factory = _session_factory()
    async with factory() as session:
        sp = await session.get(ScheduledPost, sp_id)
        if not sp:
            text = f"Scheduled post {sp_id[:8]} not found."
        elif sp.status == "vetoed":
            text = f"Post {sp_id[:8]} already vetoed."
        else:
            sp.status = "vetoed"
            session.add(sp)
            await session.commit()
            text = f"Post {sp_id[:8]} vetoed."

    reply = query.edit_message_text if query else update.message.reply_text
    await reply(text)


async def _approve_orm_response(resp_id: str, update, query=None) -> None:
    from glitch_signal.orm.responder import send_approved_response
    await send_approved_response(resp_id)
    text = f"ORM response {resp_id[:8]} approved and sent."
    reply = query.edit_message_text if query else update.message.reply_text
    await reply(text)


async def _veto_orm_response(resp_id: str, update, query=None) -> None:
    factory = _session_factory()
    async with factory() as session:
        resp = await session.get(OrmResponse, resp_id)
        if resp:
            resp.status = "vetoed"
            session.add(resp)
            await session.commit()
    text = f"ORM response {resp_id[:8]} vetoed."
    reply = query.edit_message_text if query else update.message.reply_text
    await reply(text)


# ---------------------------------------------------------------------------
# Comment-reply handlers (Feature #1: engagement on our own posts)
# ---------------------------------------------------------------------------

async def _approve_comment_reply(reply_id: str, update, query=None) -> None:
    from glitch_signal.comments.sweeper import approve_reply

    ok, msg = await approve_reply(reply_id)
    text = f"Comment reply {reply_id[:8]} — {msg}"
    reply = query.edit_message_text if query else update.message.reply_text
    await reply(text)


async def _veto_comment_reply(reply_id: str, update, query=None) -> None:
    from glitch_signal.comments.sweeper import veto_reply

    ok, msg = await veto_reply(reply_id)
    text = f"Comment reply {reply_id[:8]} — {msg}"
    reply = query.edit_message_text if query else update.message.reply_text
    await reply(text)


# ---------------------------------------------------------------------------
# Strategic-reply handlers (Feature #2: reply to other people's posts)
# ---------------------------------------------------------------------------

async def _approve_strategic_reply(sr_id: str, update, query=None) -> None:
    from glitch_signal.comments.strategic import approve_strategic

    ok, msg = await approve_strategic(sr_id)
    text = f"Strategic reply {sr_id[:8]} — {msg}"
    reply = query.edit_message_text if query else update.message.reply_text
    await reply(text)


async def _veto_strategic_reply(sr_id: str, update, query=None) -> None:
    from glitch_signal.comments.strategic import veto_strategic

    ok, msg = await veto_strategic(sr_id)
    text = f"Strategic reply {sr_id[:8]} — {msg}"
    reply = query.edit_message_text if query else update.message.reply_text
    await reply(text)


# ---------------------------------------------------------------------------
# Strategic reply — /reply and /reply_with_text commands
# ---------------------------------------------------------------------------

async def cmd_reply(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/reply <url>                  → founder voice, auto-fetch post text
    /reply <url> brand             → brand voice
    /reply <url> founder           → founder voice (explicit)
    """
    if not _is_admin(update):
        return
    args = ctx.args or []
    if not args:
        await update.message.reply_text(
            "Usage: /reply <url> [brand|founder]\n"
            "If auto-fetch fails, use /reply_with_text <url> :: <post text>"
        )
        return

    url = args[0]
    voice = (args[1].lower() if len(args) > 1 else "founder").strip()
    brand_id = "glitch_executor" if voice == "brand" else "glitch_founder"

    from glitch_signal.comments.strategic import queue_strategic_reply

    sr_id, payload = await queue_strategic_reply(
        target_url=url,
        brand_id=brand_id,
        requested_by=str(update.effective_user.id) if update.effective_user else None,
    )
    if not sr_id:
        await update.message.reply_text(payload)
        return

    await _send_strategic_preview(update, sr_id, brand_id, payload)


async def cmd_reply_with_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/reply_with_text <url> :: <pasted post text>     (founder voice)
    /reply_with_text <url> :: <pasted post text> :: brand
    """
    if not _is_admin(update):
        return
    raw = (update.message.text or "").split(maxsplit=1)
    if len(raw) < 2:
        await update.message.reply_text(
            "Usage: /reply_with_text <url> :: <post text> [:: brand|founder]"
        )
        return

    parts = [p.strip() for p in raw[1].split("::")]
    if len(parts) < 2:
        await update.message.reply_text(
            "Separate the URL from the post text with '::'. "
            "Optionally add another :: brand|founder at the end."
        )
        return
    url, target_text = parts[0], parts[1]
    voice = parts[2].lower() if len(parts) > 2 else "founder"
    brand_id = "glitch_executor" if voice == "brand" else "glitch_founder"

    from glitch_signal.comments.strategic import queue_from_text

    sr_id, payload = await queue_from_text(
        target_url=url,
        target_text=target_text,
        brand_id=brand_id,
        requested_by=str(update.effective_user.id) if update.effective_user else None,
    )
    if not sr_id:
        await update.message.reply_text(payload)
        return

    await _send_strategic_preview(update, sr_id, brand_id, payload)


async def _send_strategic_preview(
    update, sr_id: str, brand_id: str, drafted_reply: str
) -> None:
    display = brand_config(brand_id).get("display_name", brand_id)
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Post reply", callback_data=f"strply_a:{sr_id}"),
        InlineKeyboardButton("Skip", callback_data=f"strply_v:{sr_id}"),
    ]])
    msg = (
        f"[{display}] Strategic reply draft\n"
        f"ID: {sr_id[:8]}\n"
        f"───\n"
        f"Drafted reply:\n{drafted_reply}"
    )
    await update.message.reply_text(msg, reply_markup=keyboard)
