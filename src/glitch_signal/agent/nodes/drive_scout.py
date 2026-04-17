"""DriveScout node — polls a brand's Drive folder for new pre-edited clips.

Entry point for the `content_source: drive_footage` pipeline. For each
unseen Drive file it:
  1. Downloads to {video_storage_path}/drive/<brand_id>/<file_id>.mp4
  2. Creates a Signal row with source="drive", source_ref=file_id
  3. Emits the first new signal_id into state so the rest of the graph
     (caption_writer → telegram_preview) processes it.

Dedup: a file is considered seen when a Signal with
(brand_id, source="drive", source_ref=file_id) already exists.
"""
from __future__ import annotations

import pathlib
import uuid
from datetime import UTC, datetime

import structlog
from sqlmodel import select

from glitch_signal.agent.state import SignalAgentState
from glitch_signal.config import brand_config, settings
from glitch_signal.db.models import Signal
from glitch_signal.db.session import _session_factory
from glitch_signal.integrations import google_drive

log = structlog.get_logger(__name__)


async def drive_scout_node(state: SignalAgentState) -> SignalAgentState:
    brand_id = state.get("brand_id") or settings().default_brand_id
    cfg = brand_config(brand_id)

    if cfg.get("content_source") != "drive_footage":
        return {
            **state,
            "error": (
                f"drive_scout: brand {brand_id!r} content_source is "
                f"{cfg.get('content_source')!r}, expected 'drive_footage'"
            ),
        }

    folder_id = (
        cfg.get("drive_folder_id")
        or cfg.get("platforms", {}).get("drive_folder_id")
    )
    if not folder_id:
        return {
            **state,
            "error": f"drive_scout: no drive_folder_id in brand config for {brand_id!r}",
        }

    try:
        files = await google_drive.list_video_files(folder_id)
    except Exception as exc:
        log.error("drive_scout.list_failed", brand=brand_id, error=str(exc))
        return {**state, "error": f"drive_scout: list failed: {exc}"}

    if not files:
        log.info("drive_scout.empty_folder", brand=brand_id, folder_id=folder_id)
        return {**state, "brand_id": brand_id, "signals": []}

    new_signals: list[dict] = []
    storage_root = pathlib.Path(settings().video_storage_path) / "drive" / brand_id
    storage_root.mkdir(parents=True, exist_ok=True)

    factory = _session_factory()
    async with factory() as session:
        for f in files:
            if await _already_seen(session, brand_id, f.id):
                continue

            local_path = storage_root / f"{f.id}{pathlib.Path(f.name).suffix.lower() or '.mp4'}"
            if not local_path.exists():
                try:
                    bytes_written = await google_drive.download_file(f.id, local_path)
                    log.info(
                        "drive_scout.downloaded",
                        brand=brand_id,
                        file_id=f.id,
                        name=f.name,
                        bytes=bytes_written,
                    )
                except Exception as exc:
                    log.error(
                        "drive_scout.download_failed",
                        brand=brand_id,
                        file_id=f.id,
                        error=str(exc),
                    )
                    continue

            sig = Signal(
                id=str(uuid.uuid4()),
                brand_id=brand_id,
                source="drive",
                source_ref=f.id,
                summary=f"Drive clip: {f.name}",
                novelty_score=1.0,   # drive clips are always "keep" — no scoring phase
                status="queued",
                created_at=datetime.now(UTC).replace(tzinfo=None),
            )
            session.add(sig)
            new_signals.append({
                "id": sig.id,
                "source_ref": f.id,
                "name": f.name,
                "local_path": str(local_path),
                "size": f.size,
            })

        await session.commit()

    log.info(
        "drive_scout.done",
        brand=brand_id,
        new=len(new_signals),
        total_in_folder=len(files),
    )

    # If the graph was invoked for a specific signal_id (manual re-run),
    # keep it. Otherwise promote the first new signal so the downstream
    # nodes have something to process.
    first_new_id = new_signals[0]["id"] if new_signals else ""
    return {
        **state,
        "brand_id": brand_id,
        "signal_id": state.get("signal_id") or first_new_id,
        "signals": new_signals,
        "platform": state.get("platform") or "tiktok",
    }


async def _already_seen(session, brand_id: str, drive_file_id: str) -> bool:
    result = await session.execute(
        select(Signal).where(
            Signal.brand_id == brand_id,
            Signal.source == "drive",
            Signal.source_ref == drive_file_id,
        ).limit(1)
    )
    return result.scalar_one_or_none() is not None
