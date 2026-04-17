"""LangGraph StateGraph for the Glitch Social Media Agent video pipeline.

Two entry paths, chosen per-invocation via `state["content_source"]`:

  ai_generated (default, Glitch Executor):
    scout → script_writer → storyboard → video_router → video_generator → END
    [scheduler re-enters at video_assembler when all shots complete]
    video_assembler → quality_check → [pass]     → telegram_preview → END
                                    → [retry]    → storyboard (retry_count < 2)
                                    → [escalate] → END (Telegram alert sent)

  drive_footage (Namhya-style — pre-edited clips from a Drive folder):
    drive_scout → caption_writer → telegram_preview → END
    [video generation + QC are bypassed — footage is post-ready]

If content_source is absent, routing defaults to ai_generated so existing
/jobs/scout callers behave exactly as before this PR.
"""
from __future__ import annotations

from langgraph.graph import END, StateGraph

from glitch_signal.agent.nodes.caption_writer import caption_writer_node
from glitch_signal.agent.nodes.drive_scout import drive_scout_node
from glitch_signal.agent.nodes.quality_check import quality_check_node
from glitch_signal.agent.nodes.scout import scout_node
from glitch_signal.agent.nodes.script_writer import script_writer_node
from glitch_signal.agent.nodes.storyboard import storyboard_node
from glitch_signal.agent.nodes.telegram_preview import telegram_preview_node
from glitch_signal.agent.nodes.video_assembler import video_assembler_node
from glitch_signal.agent.nodes.video_generator import video_generator_node
from glitch_signal.agent.nodes.video_router import video_router_node
from glitch_signal.agent.state import SignalAgentState

MAX_QC_RETRIES = 2


def _qc_router(state: SignalAgentState) -> str:
    if state.get("qc_passed"):
        return "pass"
    retry_count = int(state.get("retry_count") or 0)
    if retry_count < MAX_QC_RETRIES:
        return "retry"
    return "escalate"


async def _escalate_node(state: SignalAgentState) -> SignalAgentState:
    """Send Telegram alert when QC fails after max retries."""
    import structlog

    from glitch_signal.config import settings
    log = structlog.get_logger(__name__)

    asset_id = state.get("asset_id", "unknown")
    script_id = state.get("script_id", "unknown")
    qc_notes = state.get("qc_notes", "")

    msg = (
        f"QC escalation — video failed after {MAX_QC_RETRIES} retries\n"
        f"Script: {script_id[:8]}\n"
        f"Asset: {asset_id[:8]}\n"
        f"Notes: {qc_notes[:200]}"
    )
    log.error("graph.qc_escalated", script_id=script_id, asset_id=asset_id)

    if not settings().is_dry_run:
        try:
            from telegram import Bot
            bot = Bot(token=settings().telegram_bot_token_signal)
            for admin_id in settings().admin_telegram_ids:
                await bot.send_message(chat_id=admin_id, text=msg)
        except Exception as exc:
            log.error("graph.escalate_telegram_failed", error=str(exc))

    return {**state, "error": f"QC failed after {MAX_QC_RETRIES} retries: {qc_notes}"}


def _entry_router(state: SignalAgentState) -> str:
    """Pick the entry node based on content_source."""
    cs = (state.get("content_source") or "").strip().lower()
    return "drive_scout" if cs == "drive_footage" else "scout"


def build_graph() -> StateGraph:
    """Build and compile the full pipeline graph."""
    graph = StateGraph(SignalAgentState)

    # ai_generated branch (existing Glitch Executor pipeline)
    graph.add_node("scout", scout_node)
    graph.add_node("script_writer", script_writer_node)
    graph.add_node("storyboard", storyboard_node)
    graph.add_node("video_router", video_router_node)
    graph.add_node("video_generator", video_generator_node)
    graph.add_node("video_assembler", video_assembler_node)
    graph.add_node("quality_check", quality_check_node)
    graph.add_node("telegram_preview", telegram_preview_node)
    graph.add_node("escalate", _escalate_node)

    # drive_footage branch (Namhya-style)
    graph.add_node("drive_scout", drive_scout_node)
    graph.add_node("caption_writer", caption_writer_node)

    # Conditional entry — drive_footage brands skip the whole AI chain.
    graph.set_conditional_entry_point(
        _entry_router,
        {"scout": "scout", "drive_scout": "drive_scout"},
    )

    # ai_generated path
    graph.add_edge("scout", "script_writer")
    graph.add_edge("script_writer", "storyboard")
    graph.add_edge("storyboard", "video_router")
    graph.add_edge("video_router", "video_generator")
    graph.add_edge("video_generator", END)

    # drive_footage path — no video gen, no assembler, no QC
    graph.add_edge("drive_scout", "caption_writer")
    graph.add_edge("caption_writer", "telegram_preview")

    # Assembler branch (scheduler-triggered re-entry for ai_generated)
    graph.add_edge("video_assembler", "quality_check")
    graph.add_conditional_edges(
        "quality_check",
        _qc_router,
        {
            "pass": "telegram_preview",
            "retry": "storyboard",
            "escalate": "escalate",
        },
    )
    graph.add_edge("telegram_preview", END)
    graph.add_edge("escalate", END)

    return graph.compile()


# Singleton — built once per process
_graph = None


def get_graph():
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph
