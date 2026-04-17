#!/usr/bin/env python3
"""One-time registration of our inbound webhook with Upload-Post.

Usage (from repo root, with .env loaded):
    source .venv/bin/activate
    set -a; source .env; set +a
    python scripts/register_upload_post_webhook.py

This reads:
    UPLOAD_POST_API_KEY
    UPLOAD_POST_WEBHOOK_SECRET   (generate one: python -c "import secrets; print(secrets.token_urlsafe(32))")
    PUBLIC_BASE_URL              (defaults to https://grow.glitchexecutor.com)

…and POSTs to Upload-Post's notifications config endpoint to enable
webhook delivery for all relevant events.

Events subscribed:
  - upload_completed
  - social_account_connected
  - social_account_disconnected
  - social_account_reauth_required

Re-run this whenever you rotate UPLOAD_POST_WEBHOOK_SECRET.
"""
from __future__ import annotations

import os
import sys

import requests


def main() -> int:
    api_key = os.environ.get("UPLOAD_POST_API_KEY", "").strip()
    secret = os.environ.get("UPLOAD_POST_WEBHOOK_SECRET", "").strip()
    base_url = os.environ.get("PUBLIC_BASE_URL", "https://grow.glitchexecutor.com").rstrip("/")

    if not api_key:
        print("ERROR: UPLOAD_POST_API_KEY is not set in env", file=sys.stderr)
        return 2
    if not secret:
        print(
            "ERROR: UPLOAD_POST_WEBHOOK_SECRET is not set.\n"
            "  Generate one with:\n"
            "    python -c 'import secrets; print(secrets.token_urlsafe(32))'\n"
            "  Save it to .env, then re-run.",
            file=sys.stderr,
        )
        return 2

    webhook_url = f"{base_url}/webhooks/upload_post/{secret}"
    events = [
        "upload_completed",
        "social_account_connected",
        "social_account_disconnected",
        "social_account_reauth_required",
    ]

    print(f"Registering webhook: {webhook_url}")
    print(f"Events: {', '.join(events)}")

    # Per https://docs.upload-post.com/api/webhooks/
    resp = requests.post(
        "https://app.upload-post.com/api/uploadposts/users/notifications",
        headers={
            "Authorization": f"Apikey {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "channels": ["webhook"],
            "webhook_url": webhook_url,
            "webhook_events": events,
        },
        timeout=30,
    )

    print(f"HTTP {resp.status_code}")
    try:
        print(resp.json())
    except Exception:
        print(resp.text)

    if resp.status_code >= 400:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
