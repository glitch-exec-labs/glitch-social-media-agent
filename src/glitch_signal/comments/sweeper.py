"""Comment engagement sweeper — read comments on our published posts,
triage them, draft replies in brand voice, queue for Telegram approval.

Flow:
    1. Scheduler tick calls sweep_comments() every N minutes
    2. For each recent PublishedPost (within lookback window):
         a. Upload-Post get_post_comments(user, post_id)
         b. For each comment not yet in comment_reply table:
            - LLM triage (reply_worthy | spam | promo | skip)
            - If reply_worthy: LLM drafts reply in brand voice
            - Write CommentReply row, status=pending_approval
            - Send Telegram preview with Approve/Veto buttons
    3. Telegram callback handler (handlers.py) fires approve/veto →
       reply_to_comment() via Upload-Post

LinkedIn algorithm rewards author-replies 15× more than likes, which makes
this the single highest-leverage engagement lever we have on the platform.
"""
from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta

import litellm
import structlog
from sqlmodel import select
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from glitch_signal.agent.llm import pick
from glitch_signal.agent.nodes.text_writer import _forbidden_hits
from glitch_signal.config import brand_config, settings
from glitch_signal.db.models import CommentReply, PublishedPost
from glitch_signal.db.session import _session_factory

log = structlog.get_logger(__name__)

# How far back we look for new comments on any given sweep. Posts older
# than this are considered "past the active engagement window" and we stop
# polling to save API calls.
COMMENT_LOOKBACK_DAYS = 14

# Upload-Post comment-read is free but still an API call per post. Cap
# per-tick so one sweep doesn't spike the API budget.
MAX_POSTS_PER_SWEEP = 20

# Upload-Post platforms where get_post_comments works. Keep narrow to
# avoid hitting an endpoint for platforms that don't support comment APIs.
COMMENT_PLATFORMS = ("upload_post_linkedin", "upload_post_x")


async def sweep_comments() -> dict:
    """One sweep pass. Returns a summary dict for logging."""
    now = datetime.now(UTC).replace(tzinfo=None)
    cutoff = now - timedelta(days=COMMENT_LOOKBACK_DAYS)

    summary = {"posts_checked": 0, "new_comments": 0, "drafts_queued": 0, "skipped": 0}
    factory = _session_factory()
    async with factory() as session:
        result = await session.execute(
            select(PublishedPost)
            .where(PublishedPost.published_at >= cutoff)
            .where(PublishedPost.platform.in_(COMMENT_PLATFORMS))
            .order_by(PublishedPost.published_at.desc())
            .limit(MAX_POSTS_PER_SWEEP)
        )
        posts = result.scalars().all()

    for post in posts:
        try:
            new_count, draft_count, skip_count = await _sweep_one_post(post)
            summary["posts_checked"] += 1
            summary["new_comments"] += new_count
            summary["drafts_queued"] += draft_count
            summary["skipped"] += skip_count
        except Exception as exc:
            log.warning(
                "comments.sweep_one_failed",
                published_post_id=post.id,
                platform=post.platform,
                error=str(exc)[:200],
            )

    if summary["new_comments"] or summary["drafts_queued"]:
        log.info("comments.sweep_summary", **summary)
    return summary


async def _sweep_one_post(post: PublishedPost) -> tuple[int, int, int]:
    """Return (new_comments, drafts_queued, skipped)."""
    cfg = brand_config(post.brand_id)
    platforms_cfg = cfg.get("platforms", {}) or {}
    block = platforms_cfg.get(post.platform) or {}
    user = block.get("user")
    if not user:
        # No Upload-Post user configured for this platform on this brand.
        return 0, 0, 0

    api_key = settings().upload_post_api_key
    if not api_key:
        return 0, 0, 0

    comments = await asyncio.to_thread(
        _fetch_comments, api_key, user, post.platform_post_id
    )
    if not comments:
        return 0, 0, 0

    new_count = 0
    drafted = 0
    skipped = 0

    factory = _session_factory()
    async with factory() as session:
        # Deduplicate: skip comments we already have a row for
        ids = [c.get("id") or c.get("comment_id") for c in comments]
        ids = [i for i in ids if i]
        if ids:
            existing = (await session.execute(
                select(CommentReply.platform_comment_id).where(
                    CommentReply.platform_comment_id.in_(ids)
                )
            )).scalars().all()
        else:
            existing = []
        seen = set(existing)

        for c in comments:
            cid = c.get("id") or c.get("comment_id")
            if not cid or cid in seen:
                continue
            new_count += 1

            text = (c.get("text") or c.get("body") or c.get("message") or "").strip()
            if not text:
                skipped += 1
                continue

            commenter = (
                c.get("author") or {}
            ) if isinstance(c.get("author"), dict) else {}
            handle = commenter.get("handle") or commenter.get("username") or c.get("author_handle")
            name = commenter.get("name") or c.get("author_name")

            # Don't reply to ourselves (our brand account commenting on our own post)
            if handle and user and handle.lower() == str(user).lower():
                skipped += 1
                continue

            try:
                triage = await _triage_comment(text, platform=post.platform)
            except Exception as exc:
                log.warning("comments.triage_failed", error=str(exc)[:200])
                triage = "skip"

            if triage != "reply_worthy":
                # Record anyway so we don't re-triage next sweep
                row = CommentReply(
                    id=str(uuid.uuid4()),
                    brand_id=post.brand_id,
                    platform=post.platform,
                    published_post_id=post.id,
                    platform_post_id=post.platform_post_id,
                    platform_comment_id=cid,
                    commenter_handle=handle,
                    commenter_name=name,
                    comment_text=text,
                    triage_tier=triage,
                    status="ignored",
                    created_at=datetime.now(UTC).replace(tzinfo=None),
                )
                session.add(row)
                skipped += 1
                continue

            try:
                drafted_reply = await _draft_reply(
                    brand_id=post.brand_id,
                    platform=post.platform,
                    original_post=_post_excerpt(post),
                    comment_text=text,
                )
            except Exception as exc:
                log.warning("comments.draft_failed", error=str(exc)[:200])
                drafted_reply = None

            row = CommentReply(
                id=str(uuid.uuid4()),
                brand_id=post.brand_id,
                platform=post.platform,
                published_post_id=post.id,
                platform_post_id=post.platform_post_id,
                platform_comment_id=cid,
                commenter_handle=handle,
                commenter_name=name,
                comment_text=text,
                triage_tier=triage,
                status="pending_approval" if drafted_reply else "failed",
                drafted_reply=drafted_reply,
                created_at=datetime.now(UTC).replace(tzinfo=None),
            )
            session.add(row)
            await session.flush()
            if drafted_reply:
                drafted += 1
                await _send_approval_message(row)

        await session.commit()

    return new_count, drafted, skipped


def _fetch_comments(api_key: str, user: str, platform_post_id: str) -> list[dict]:
    """Blocking SDK call → list of comment dicts."""
    import upload_post

    client = upload_post.UploadPostClient(api_key=api_key)
    try:
        resp = client.get_post_comments(user=user, post_id=platform_post_id)
    except Exception as exc:
        log.warning(
            "comments.fetch_failed",
            user=user,
            platform_post_id=platform_post_id,
            error=str(exc)[:200],
        )
        return []

    # Upload-Post response shapes vary. Normalise: want a flat list of dicts.
    if isinstance(resp, list):
        return resp
    if not isinstance(resp, dict):
        return []
    for key in ("comments", "results", "data", "items"):
        v = resp.get(key)
        if isinstance(v, list):
            return v
    return []


def _post_excerpt(post: PublishedPost) -> str:
    """Short reference to the post we're replying under, for the drafter prompt."""
    url = post.platform_url or post.platform_post_id
    return f"Platform: {post.platform}  ·  URL: {url}"


# ---------------------------------------------------------------------------
# LLM — triage + draft
# ---------------------------------------------------------------------------

_TRIAGE_SYSTEM = """You are triaging incoming comments on a technical founder's posts.
Classify the comment into exactly ONE tier and return it as JSON.

Tiers:
- reply_worthy: genuine engagement worth a reply. Real question, a correction,
  a thoughtful disagreement, someone sharing their relevant experience, an
  industry peer adding perspective, or a warm "this is interesting" from
  someone whose follow-up could turn into a real conversation.
- promo: the commenter is promoting their service/product, DM pitches,
  "great post, check out my newsletter" patterns.
- spam: bot replies, emoji strings, off-topic engagement-farming,
  "follow for follow".
- skip: very low signal — generic "great post!" / "nice!" / single emoji
  that doesn't warrant a reply from the founder.

Return ONLY JSON: {"tier": "<one of the four>", "reason": "<≤12 words>"}
"""

_REPLY_SYSTEM = """You are writing a short reply to a comment on a post by a technical founder.

Hard rules — a reply that breaks any of these will be rejected:
- 1-3 sentences max. Never longer. Sharper is better.
- Match the brand voice file verbatim. No marketing verbs, no hype adjectives,
  no engagement-bait questions ("what do you think?").
- No fabricated metrics. Describe decisions, tradeoffs, specifics — not
  outcomes we haven't measured.
- No thanks for comment / "appreciate it" openers.
- Where the commenter asked a specific question, answer it directly.
- Where the commenter made a point, respond to the actual point (agree,
  push back, extend — not a generic "totally agree!").
- If the comment is about a technical detail, add real information.

Output ONLY the reply text. No quotes, no preamble, no JSON."""


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    retry=retry_if_exception_type(
        (litellm.ServiceUnavailableError, litellm.RateLimitError, litellm.APIConnectionError)
    ),
)
async def _triage_comment(text: str, *, platform: str) -> str:
    """Return one of: reply_worthy | promo | spam | skip."""
    mc = pick("smart" if (settings().openai_api_key or settings().anthropic_api_key) else "cheap")
    resp = await litellm.acompletion(
        model=mc.model,
        messages=[
            {"role": "system", "content": _TRIAGE_SYSTEM},
            {"role": "user", "content": f"Platform: {platform}\nComment: {text}"},
        ],
        response_format={"type": "json_object"},
        max_tokens=512,
        **mc.kwargs,
    )
    raw = resp.choices[0].message.content or "{}"
    try:
        data = json.loads(raw)
        tier = str(data.get("tier", "skip")).strip().lower()
    except (ValueError, TypeError):
        return "skip"
    if tier not in {"reply_worthy", "promo", "spam", "skip"}:
        return "skip"
    return tier


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    retry=retry_if_exception_type(
        (litellm.ServiceUnavailableError, litellm.RateLimitError, litellm.APIConnectionError)
    ),
)
async def _draft_reply(
    *,
    brand_id: str,
    platform: str,
    original_post: str,
    comment_text: str,
) -> str:
    """LLM drafts a short reply in brand voice."""
    import pathlib

    cfg = brand_config(brand_id)
    voice_path = cfg.get("voice_prompt_path")
    voice_text = ""
    if voice_path:
        p = pathlib.Path(voice_path)
        if p.exists():
            voice_text = p.read_text()

    voice_role = (
        "VOICE IS TEJAS — first-person 'I', personal, lesson/feeling tone."
        if brand_id == "glitch_founder"
        else "VOICE IS GLITCH EXECUTOR — first-person plural 'we', technical, direct."
    )

    system = (
        f"{voice_text}\n\n"
        f"---\n"
        f"{voice_role}\n"
        f"---\n"
        f"{_REPLY_SYSTEM}"
    )
    user = (
        f"The original post (ours):\n{original_post}\n\n"
        f"The incoming comment:\n{comment_text}\n\n"
        f"Write the reply."
    )

    mc = pick("smart" if (settings().openai_api_key or settings().anthropic_api_key) else "cheap")
    resp = await litellm.acompletion(
        model=mc.model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        max_tokens=1024,
        **mc.kwargs,
    )
    body = (resp.choices[0].message.content or "").strip()
    body = _strip_framing(body)

    # Re-use the same forbidden-terms filter the post generator uses
    hits = _forbidden_hits(body)
    if hits:
        log.info("comments.reply_forbidden_hits_regen", hits=hits)
        ban = (
            "Your last reply used banned phrases: " + ", ".join(f'"{h}"' for h in hits)
            + ". Rewrite without any of these, and without any hype adjectives or "
            "marketing verbs. Same content; just change the wording."
        )
        resp = await litellm.acompletion(
            model=mc.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
                {"role": "assistant", "content": body},
                {"role": "user", "content": ban},
            ],
            max_tokens=1024,
            **mc.kwargs,
        )
        body = _strip_framing((resp.choices[0].message.content or "").strip())

    return body


def _strip_framing(text: str) -> str:
    """Remove common 'Here's the reply:' framing the LLM sometimes adds."""
    lines = text.split("\n")
    while lines and lines[0].lower().strip() in (
        "here's the reply:",
        "here is the reply:",
        "reply:",
    ):
        lines = lines[1:]
    out = "\n".join(lines).strip()
    if out.startswith('"') and out.endswith('"'):
        out = out[1:-1].strip()
    return out


# ---------------------------------------------------------------------------
# Telegram approval
# ---------------------------------------------------------------------------

async def _send_approval_message(row: CommentReply) -> None:
    token = settings().telegram_bot_token_signal
    admin_ids = settings().admin_telegram_ids
    if not token or not admin_ids:
        log.warning("comments.approval.skipped_no_telegram", comment_reply_id=row.id)
        return

    from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

    display = brand_config(row.brand_id).get("display_name", row.brand_id)
    platform_label = row.platform.replace("upload_post_", "").upper()

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Send reply", callback_data=f"rply_a:{row.id}"),
        InlineKeyboardButton("Skip", callback_data=f"rply_v:{row.id}"),
    ]])

    commenter = row.commenter_handle or row.commenter_name or "anon"
    msg = (
        f"[{display}] Comment reply — {platform_label}\n"
        f"From: {commenter}\n"
        f"ID: {row.id[:8]}\n"
        f"───\n"
        f"Their comment:\n{row.comment_text[:500]}\n\n"
        f"───\n"
        f"Drafted reply:\n{row.drafted_reply}"
    )

    bot = Bot(token=token)
    for admin_id in admin_ids:
        try:
            await bot.send_message(chat_id=admin_id, text=msg, reply_markup=keyboard)
        except Exception as exc:
            log.warning("comments.approval.send_failed", admin_id=admin_id, error=str(exc))


# ---------------------------------------------------------------------------
# Public API used by the Telegram callback handler
# ---------------------------------------------------------------------------

async def approve_reply(comment_reply_id: str) -> tuple[bool, str]:
    """Called when an operator taps Approve in Telegram. Posts the reply."""
    factory = _session_factory()
    async with factory() as session:
        row = await session.get(CommentReply, comment_reply_id)
        if not row:
            return False, f"comment_reply {comment_reply_id[:8]} not found"
        if row.status not in ("pending_approval", "failed"):
            return False, f"already {row.status}"
        if not row.drafted_reply:
            return False, "no drafted reply"

        cfg = brand_config(row.brand_id)
        user = (cfg.get("platforms", {}).get(row.platform) or {}).get("user")
        if not user:
            return False, f"no Upload-Post user for {row.brand_id}.{row.platform}"

    api_key = settings().upload_post_api_key
    if not api_key:
        return False, "UPLOAD_POST_API_KEY unset"

    try:
        resp = await asyncio.to_thread(
            _post_reply,
            api_key,
            user,
            row.platform_comment_id,
            row.drafted_reply,
        )
    except Exception as exc:
        async with _session_factory()() as session:
            row = await session.get(CommentReply, comment_reply_id)
            if row:
                row.status = "failed"
                row.updated_at = datetime.now(UTC).replace(tzinfo=None)
                session.add(row)
                await session.commit()
        return False, f"reply_to_comment failed: {exc}"

    async with _session_factory()() as session:
        row = await session.get(CommentReply, comment_reply_id)
        if row:
            row.status = "posted"
            row.posted_reply_id = (
                resp.get("id") or resp.get("reply_id") or resp.get("request_id")
                if isinstance(resp, dict) else None
            )
            row.updated_at = datetime.now(UTC).replace(tzinfo=None)
            session.add(row)
            await session.commit()

    log.info("comments.reply.posted", comment_reply_id=comment_reply_id)
    return True, "Reply posted."


async def veto_reply(comment_reply_id: str) -> tuple[bool, str]:
    """Operator tapped Skip. Mark the row ignored and move on."""
    factory = _session_factory()
    async with factory() as session:
        row = await session.get(CommentReply, comment_reply_id)
        if not row:
            return False, f"comment_reply {comment_reply_id[:8]} not found"
        row.status = "ignored"
        row.updated_at = datetime.now(UTC).replace(tzinfo=None)
        session.add(row)
        await session.commit()
    return True, "Skipped."


def _post_reply(api_key: str, user: str, comment_id: str, message: str) -> dict:
    import upload_post

    client = upload_post.UploadPostClient(api_key=api_key)
    return client.reply_to_comment(user=user, comment_id=comment_id, message=message)
