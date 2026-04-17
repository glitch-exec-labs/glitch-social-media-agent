"""TelegramPreview node — sends assembled video to founder with 48h veto window.

Writes ScheduledPost(status=pending_veto). The scheduler promotes to queued
when veto_deadline passes without a veto command.
"""
from __future__ import annotations

import pathlib
import uuid
from datetime import UTC, datetime, timedelta

import structlog
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from glitch_signal.agent.state import SignalAgentState
from glitch_signal.config import brand_config, brand_ids, settings
from glitch_signal.db.models import ContentScript, ScheduledPost, VideoAsset
from glitch_signal.db.session import _session_factory

log = structlog.get_logger(__name__)

VETO_WINDOW_HOURS = 48
TELEGRAM_VIDEO_SIZE_LIMIT = 50 * 1024 * 1024  # 50 MB


async def telegram_preview_node(state: SignalAgentState) -> SignalAgentState:
    asset_id = state.get("asset_id")
    asset_path = state.get("asset_path")
    script_id = state.get("script_id")
    platform = state.get("platform", "youtube_shorts")

    if not asset_id or not asset_path:
        return {**state, "error": "telegram_preview: missing asset_id or asset_path"}

    factory = _session_factory()
    async with factory() as session:
        cs = await session.get(ContentScript, script_id) if script_id else None
        asset = await session.get(VideoAsset, asset_id)

    now = datetime.now(UTC).replace(tzinfo=None)
    veto_deadline = now + timedelta(hours=VETO_WINDOW_HOURS)
    scheduled_for = now + timedelta(hours=VETO_WINDOW_HOURS)

    brand_id = (
        state.get("brand_id")
        or (getattr(asset, "brand_id", None) if asset else None)
        or settings().default_brand_id
    )

    # Write ScheduledPost in pending_veto state
    factory = _session_factory()
    async with factory() as session:
        sp = ScheduledPost(
            id=str(uuid.uuid4()),
            brand_id=brand_id,
            asset_id=asset_id,
            platform=platform,
            scheduled_for=scheduled_for,
            status="pending_veto",
            veto_deadline=veto_deadline,
        )
        session.add(sp)
        await session.commit()
        sp_id = sp.id

    # Send Telegram preview
    caption = _build_caption(cs, asset, platform, sp_id, brand_id=brand_id)
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Approve now", callback_data=f"approve:{sp_id}"),
        InlineKeyboardButton("Veto", callback_data=f"veto:{sp_id}"),
    ]])

    if not settings().is_dry_run:
        await _send_preview(asset_path, caption, keyboard)

    log.info(
        "telegram_preview.sent",
        scheduled_post_id=sp_id,
        veto_deadline=veto_deadline.isoformat(),
    )
    return {
        **state,
        "preview_sent": True,
        "veto_deadline": veto_deadline.isoformat(),
    }


def _build_caption(cs, asset, platform: str, sp_id: str, brand_id: str | None = None) -> str:
    platform_label = {
        "youtube_shorts": "YouTube Shorts",
        "twitter": "X / Twitter",
        "instagram_reels": "Instagram Reels",
    }.get(platform, platform)

    content_type = getattr(cs, "content_type", "unknown") if cs else "unknown"
    duration = f"{getattr(asset, 'duration_s', 0):.0f}s" if asset else "?"
    qc = f"{getattr(asset, 'quality_score', 0) or 0:.2f}" if asset else "?"

    # Brand-prefix the header when more than one brand is configured, so
    # the operator doesn't confuse previews across brands.
    prefix = ""
    if brand_id and len(brand_ids()) > 1:
        display = brand_config(brand_id).get("display_name", brand_id)
        prefix = f"[{display}] "

    return (
        f"{prefix}Video preview — {platform_label}\n"
        f"Type: {content_type} | Duration: {duration} | QC: {qc}\n"
        f"ID: {sp_id[:8]}\n\n"
        f"Approve = publish in 48h. Veto = cancel."
    )


async def _send_preview(asset_path: str, caption: str, keyboard) -> None:
    bot = Bot(token=settings().telegram_bot_token_signal)
    admin_ids = settings().admin_telegram_ids

    file_size = pathlib.Path(asset_path).stat().st_size if pathlib.Path(asset_path).exists() else 0

    for admin_id in admin_ids:
        if file_size > TELEGRAM_VIDEO_SIZE_LIMIT:
            # Too large for Telegram bot API — send notice with path
            await bot.send_message(
                chat_id=admin_id,
                text=f"{caption}\n\nFile too large for Telegram ({file_size // 1024 // 1024}MB).\nPath: {asset_path}",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
            )
        else:
            with open(asset_path, "rb") as video_file:
                await bot.send_video(
                    chat_id=admin_id,
                    video=video_file,
                    caption=caption,
                    reply_markup=keyboard,
                    supports_streaming=True,
                )
