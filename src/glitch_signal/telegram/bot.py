"""Telegram bot bootstrap for Glitch Social Media Agent.

Mirrors glitch-grow-ads-agent/src/ads_agent/telegram/bot.py pattern.
"""
from __future__ import annotations

from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
)
from telegram.ext._aioratelimiter import AIORateLimiter

from glitch_signal.config import settings
from glitch_signal.telegram import handlers


def build_app() -> Application:
    s = settings()
    app = (
        Application.builder()
        .token(s.telegram_bot_token_signal)
        .rate_limiter(AIORateLimiter())
        .build()
    )

    # Command handlers
    app.add_handler(CommandHandler("start", handlers.cmd_start))
    app.add_handler(CommandHandler("help", handlers.cmd_help))
    app.add_handler(CommandHandler("status", handlers.cmd_status))
    app.add_handler(CommandHandler("signals", handlers.cmd_signals))
    app.add_handler(CommandHandler("preview", handlers.cmd_preview))
    app.add_handler(CommandHandler("approve", handlers.cmd_approve))
    app.add_handler(CommandHandler("veto", handlers.cmd_veto))
    app.add_handler(CommandHandler("orm", handlers.cmd_orm))
    app.add_handler(CommandHandler("orm_approve", handlers.cmd_orm_approve))
    app.add_handler(CommandHandler("orm_veto", handlers.cmd_orm_veto))

    # Inline keyboard callbacks
    app.add_handler(CallbackQueryHandler(handlers.callback_handler))

    return app


def run_polling() -> None:
    """Local dev: python -m glitch_signal.telegram.bot"""
    app = build_app()
    app.run_polling()


if __name__ == "__main__":
    run_polling()
