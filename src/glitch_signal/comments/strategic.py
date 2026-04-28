"""Strategic reply — draft a reply to someone else's post on X or LinkedIn.

Covers the 70/30 growth pattern: most follower growth in the
build-in-public space comes from value-adding replies to larger accounts'
posts, not from posting in a vacuum. This module lets the operator drop
a URL into Telegram and have the agent draft a voice-matched reply.

Flow:
    /reply <url>                              (default account = glitch_founder)
    /reply <url> brand                        (use brand voice)
    /reply <url> founder                      (use founder voice)
          ↓
    1. Parse URL → detect platform (x|linkedin)
    2. Fetch the target post's text:
         - X: Twitter oEmbed → scrape a no-auth public view
         - LinkedIn: fetch OG meta description (works for many public posts)
       Fall back to "operator supplies the text" if fetch fails.
    3. LLM drafts a reply in the chosen brand voice.
    4. Save StrategicReply row, status=pending_approval.
    5. Telegram preview with Approve / Skip.
    6. Approve →
         - X:        post via Upload-Post upload_text + quote_tweet_id (quote-
                     reply, not threaded reply — but still engages with the
                     post and surfaces in our own feed).
         - LinkedIn: we cannot programmatically comment on arbitrary LinkedIn
                     posts via any audited API. Status flips to "copied" and
                     we hand the drafted reply text back to the operator in
                     Telegram for manual paste.
"""
from __future__ import annotations

import asyncio
import pathlib
import re
import uuid
from datetime import UTC, datetime

import httpx
import litellm
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from glitch_signal.agent.llm import pick
from glitch_signal.agent.nodes.text_writer import _forbidden_hits
from glitch_signal.config import brand_config, settings
from glitch_signal.db.models import StrategicReply
from glitch_signal.db.session import _session_factory

log = structlog.get_logger(__name__)

# Target post text >N chars gets truncated in the LLM context. We're drafting
# a reply, not a summary — the first N chars are plenty.
TARGET_TEXT_MAX = 1500


async def queue_strategic_reply(
    *,
    target_url: str,
    brand_id: str,
    requested_by: str | None = None,
) -> tuple[str | None, str]:
    """Fetch the target post, draft a reply, save as pending_approval.

    Returns (strategic_reply_id | None, status_message).
    """
    platform, post_id = _classify_url(target_url)

    # Reply-restriction pre-check (X only). If the author has limited
    # who can reply, we'd rather know now and skip the LLM draft entirely
    # than burn tokens on a reply that can't physically post. There's no
    # equivalent control on LinkedIn, so we only probe X.
    if platform == "x" and post_id:
        try:
            from glitch_signal.integrations.x import XClient
            blocked = await XClient(brand_id).get_reply_settings(post_id)
        except Exception:
            blocked = ""
        if blocked and blocked != "everyone":
            return None, (
                f"Author has restricted replies (reply_settings={blocked!r}). "
                "X won't accept a threaded reply from us. Skipping draft. "
                "If you want to engage anyway, post your own tweet that "
                "links to theirs."
            )

    # Fetch target post text (best-effort)
    try:
        target_text, author_handle = await _fetch_target_post(target_url, platform)
    except Exception as exc:
        log.warning("strategic.fetch_failed", url=target_url, error=str(exc)[:200])
        target_text, author_handle = "", None

    if not target_text:
        return None, (
            "Couldn't auto-fetch the post text. Retry with the text pasted:\n"
            "/reply_with_text <url> :: <pasted post text>"
        )

    try:
        drafted = await _draft_strategic_reply(
            brand_id=brand_id,
            platform=platform,
            target_text=target_text,
            author_handle=author_handle,
        )
    except Exception as exc:
        log.warning("strategic.draft_failed", error=str(exc)[:200])
        return None, f"LLM draft failed: {exc}"

    now = datetime.now(UTC).replace(tzinfo=None)
    sr_id = str(uuid.uuid4())
    factory = _session_factory()
    async with factory() as session:
        row = StrategicReply(
            id=sr_id,
            brand_id=brand_id,
            target_platform=platform,
            target_post_url=target_url,
            target_post_id=post_id,
            target_author_handle=author_handle,
            target_post_text=target_text[:TARGET_TEXT_MAX],
            drafted_reply=drafted,
            status="pending_approval",
            requested_by_telegram_id=requested_by,
            created_at=now,
        )
        session.add(row)
        await session.commit()

    return sr_id, drafted


async def queue_from_text(
    *,
    target_url: str,
    target_text: str,
    brand_id: str,
    requested_by: str | None = None,
) -> tuple[str | None, str]:
    """Operator supplied the target post text inline (for when fetch failed)."""
    platform, post_id = _classify_url(target_url)
    try:
        drafted = await _draft_strategic_reply(
            brand_id=brand_id,
            platform=platform,
            target_text=target_text,
            author_handle=None,
        )
    except Exception as exc:
        return None, f"LLM draft failed: {exc}"

    now = datetime.now(UTC).replace(tzinfo=None)
    sr_id = str(uuid.uuid4())
    factory = _session_factory()
    async with factory() as session:
        row = StrategicReply(
            id=sr_id,
            brand_id=brand_id,
            target_platform=platform,
            target_post_url=target_url,
            target_post_id=post_id,
            target_post_text=target_text[:TARGET_TEXT_MAX],
            drafted_reply=drafted,
            status="pending_approval",
            requested_by_telegram_id=requested_by,
            created_at=now,
        )
        session.add(row)
        await session.commit()

    return sr_id, drafted


# ---------------------------------------------------------------------------
# URL classification + fetch
# ---------------------------------------------------------------------------

_X_HOSTS = ("x.com", "twitter.com", "mobile.twitter.com", "www.x.com")
_LI_HOSTS = ("linkedin.com", "www.linkedin.com", "lnkd.in")


def _classify_url(url: str) -> tuple[str, str | None]:
    """Return (platform, post_id). Platform is x|linkedin|unknown."""
    url = url.strip().rstrip("/")
    low = url.lower()

    for host in _X_HOSTS:
        if f"//{host}/" in low:
            # https://x.com/<user>/status/<id>
            m = re.search(r"/status/(\d+)", url)
            return "x", (m.group(1) if m else None)

    for host in _LI_HOSTS:
        if f"//{host}/" in low:
            return "linkedin", None

    return "unknown", None


async def _fetch_target_post(url: str, platform: str) -> tuple[str, str | None]:
    """Return (post_text, author_handle). Best-effort; may return ('', None)."""
    if platform == "x":
        return await _fetch_x_post(url)
    if platform == "linkedin":
        return await _fetch_linkedin_post(url)
    return "", None


async def _fetch_x_post(url: str) -> tuple[str, str | None]:
    """Use X's public oEmbed endpoint — no auth, returns post text as HTML blob."""
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            resp = await client.get(
                "https://publish.twitter.com/oembed",
                params={"url": url, "omit_script": "true"},
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        log.info("strategic.x_oembed_failed", error=str(exc)[:200])
        return "", None

    html = data.get("html") or ""
    author = data.get("author_name") or data.get("author_url", "").rstrip("/").split("/")[-1]

    # Pull inner text from the blockquote. Reasonable regex strip — we're
    # not trying to render, just extract readable words for the LLM.
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&[#a-z0-9]+;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text, author


async def _fetch_linkedin_post(url: str) -> tuple[str, str | None]:
    """Try to scrape OpenGraph meta tags. Not reliable on all LinkedIn URLs."""
    try:
        async with httpx.AsyncClient(
            timeout=10, follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; Glitch-Social-Agent/1.0)"
            },
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
    except Exception as exc:
        log.info("strategic.linkedin_fetch_failed", error=str(exc)[:200])
        return "", None

    html = resp.text

    def _og(name: str) -> str:
        m = re.search(
            rf'<meta[^>]+property=["\']og:{name}["\'][^>]+content=["\']([^"\']+)["\']',
            html, re.IGNORECASE,
        )
        return m.group(1) if m else ""

    title = _og("title")
    description = _og("description")
    combined = (title + "\n\n" + description).strip()
    return combined, None


# ---------------------------------------------------------------------------
# LLM draft
# ---------------------------------------------------------------------------

_STRATEGIC_SYSTEM = """You are writing a short reply to someone else's post on a professional social platform.
The goal is to add real value to the conversation — agreement with extension,
respectful disagreement with a counter-point, a concrete experience, a specific
technical detail the original post missed, or a sharp follow-up question about
a specific point they made.

Hard rules — a reply that breaks any of these will be rejected:
- 1-3 sentences max. Never longer.
- Match the brand voice file verbatim.
- No marketing verbs, no hype adjectives, no "great post / totally agree"
  openers, no "thanks for sharing".
- No engagement-bait questions. Any question must be about a specific point
  in the original post.
- No self-promotion. Never link to our own work unless the original post
  explicitly asked for examples.
- Never fabricate metrics. Never claim an outcome we haven't measured.
- If we genuinely disagree with the post, say so directly and briefly.

Output ONLY the reply text. No quotes, no JSON, no preamble."""


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    retry=retry_if_exception_type(
        (litellm.ServiceUnavailableError, litellm.RateLimitError, litellm.APIConnectionError)
    ),
)
async def _draft_strategic_reply(
    *,
    brand_id: str,
    platform: str,
    target_text: str,
    author_handle: str | None,
) -> str:
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

    author_line = f"Posted by: @{author_handle}\n" if author_handle else ""

    system = (
        f"{voice_text}\n\n"
        f"---\n"
        f"{voice_role}\n"
        f"---\n"
        f"{_STRATEGIC_SYSTEM}"
    )
    user = (
        f"Platform: {platform}\n"
        f"{author_line}"
        f"Their post:\n{target_text[:TARGET_TEXT_MAX]}\n\n"
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
    body = _strip_quotes_and_framing(body)

    hits = _forbidden_hits(body)
    if hits:
        log.info("strategic.forbidden_hits_regen", hits=hits)
        ban = (
            "Your last reply used banned phrases: "
            + ", ".join(f'"{h}"' for h in hits)
            + ". Rewrite without any of these phrases, and without any other "
            "hype adjectives or marketing verbs. Same content — just change wording."
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
        body = _strip_quotes_and_framing(
            (resp.choices[0].message.content or "").strip()
        )

    return body


def _strip_quotes_and_framing(text: str) -> str:
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
# Approve / veto — called from Telegram handlers
# ---------------------------------------------------------------------------

async def approve_strategic(sr_id: str) -> tuple[bool, str]:
    factory = _session_factory()
    async with factory() as session:
        row = await session.get(StrategicReply, sr_id)
        if not row:
            return False, f"strategic_reply {sr_id[:8]} not found"
        if row.status not in ("pending_approval",):
            return False, f"already {row.status}"
        if not row.drafted_reply:
            return False, "no drafted reply"

        cfg = brand_config(row.brand_id)
        platform_key = f"upload_post_{row.target_platform}"
        user = (cfg.get("platforms", {}).get(platform_key) or {}).get("user")

    api_key = settings().upload_post_api_key

    # LinkedIn: we can't programmatically comment on other people's posts.
    # Flip status to 'copied' and let the operator paste manually.
    if row.target_platform != "x":
        async with _session_factory()() as session:
            row = await session.get(StrategicReply, sr_id)
            if row:
                row.status = "copied"
                row.updated_at = datetime.now(UTC).replace(tzinfo=None)
                session.add(row)
                await session.commit()
        return True, (
            f"LinkedIn API doesn't support third-party comments on arbitrary "
            f"posts. Paste this into the comment box:\n\n{row.drafted_reply}"
        )

    # X path: prefer the native /2/tweets endpoint (true threaded reply
    # via in_reply_to_tweet_id), fall back to Upload-Post quote_tweet_id
    # if our own token is unavailable. Native gives us the real reply
    # placement in the conversation; Upload-Post can only quote-tweet.
    if not row.target_post_id:
        return False, "missing target_post_id"

    posted_id: str | None = None
    via: str | None = None

    try:
        from glitch_signal.integrations.x import XClient
        x = XClient(row.brand_id)
        # Re-check reply settings at approval time in case the author
        # tightened them after we drafted. Cheap, single GET.
        if row.target_post_id:
            settings_now = await x.get_reply_settings(row.target_post_id)
            if settings_now and settings_now != "everyone":
                async with _session_factory()() as session:
                    blocked_row = await session.get(StrategicReply, sr_id)
                    if blocked_row:
                        blocked_row.status = "blocked_by_author"
                        blocked_row.updated_at = datetime.now(UTC).replace(tzinfo=None)
                        session.add(blocked_row)
                        await session.commit()
                return False, (
                    f"Author restricted replies (reply_settings={settings_now!r}). "
                    "Not falling back to a quote-tweet — that's a different "
                    "surface and you didn't ask for one."
                )
        result = await x.post_tweet(
            row.drafted_reply,
            in_reply_to_tweet_id=row.target_post_id,
        )
        posted_id = result.tweet_id
        via = "native-reply"
    except Exception as native_exc:
        # If the error is specifically X's reply-restriction signal, treat
        # it as blocked_by_author and refuse the quote-tweet fallback —
        # the operator asked for a reply, not a quote-tweet on our feed.
        err_text = str(native_exc).lower()
        is_reply_blocked = (
            "reply to this conversation is not allowed" in err_text
            or "not been mentioned or otherwise engaged" in err_text
        )
        if is_reply_blocked:
            async with _session_factory()() as session:
                blocked_row = await session.get(StrategicReply, sr_id)
                if blocked_row:
                    blocked_row.status = "blocked_by_author"
                    blocked_row.updated_at = datetime.now(UTC).replace(tzinfo=None)
                    session.add(blocked_row)
                    await session.commit()
            return False, (
                "Author restricted replies on this tweet. Not falling back "
                "to a quote-tweet — that's a different surface."
            )

        log.info(
            "strategic.x_native_unavailable_falling_back_to_upload_post",
            sr_id=sr_id, error=str(native_exc)[:200],
        )
        if not user or not api_key:
            async with _session_factory()() as session:
                fail_row = await session.get(StrategicReply, sr_id)
                if fail_row:
                    fail_row.status = "failed"
                    fail_row.updated_at = datetime.now(UTC).replace(tzinfo=None)
                    session.add(fail_row)
                    await session.commit()
            return False, f"native X failed and no Upload-Post fallback: {native_exc}"
        try:
            resp = await asyncio.to_thread(
                _post_x_quote_reply,
                api_key,
                user,
                row.drafted_reply,
                row.target_post_id,
            )
            via = "upload-post-quote"
            posted_id = resp.get("request_id") if isinstance(resp, dict) else None
        except Exception as exc:
            async with _session_factory()() as session:
                fail_row = await session.get(StrategicReply, sr_id)
                if fail_row:
                    fail_row.status = "failed"
                    fail_row.updated_at = datetime.now(UTC).replace(tzinfo=None)
                    session.add(fail_row)
                    await session.commit()
            return False, f"X reply failed: native={native_exc!s}; fallback={exc!s}"

    async with _session_factory()() as session:
        ok_row = await session.get(StrategicReply, sr_id)
        if ok_row:
            ok_row.status = "posted"
            ok_row.posted_platform_post_id = posted_id
            ok_row.updated_at = datetime.now(UTC).replace(tzinfo=None)
            session.add(ok_row)
            await session.commit()

    suffix = f" ({via})" if via else ""
    return True, f"Reply posted on X{suffix}."


async def veto_strategic(sr_id: str) -> tuple[bool, str]:
    factory = _session_factory()
    async with factory() as session:
        row = await session.get(StrategicReply, sr_id)
        if not row:
            return False, f"strategic_reply {sr_id[:8]} not found"
        row.status = "vetoed"
        row.updated_at = datetime.now(UTC).replace(tzinfo=None)
        session.add(row)
        await session.commit()
    return True, "Skipped."


def _post_x_quote_reply(api_key: str, user: str, text: str, quote_tweet_id: str) -> dict:
    import upload_post

    client = upload_post.UploadPostClient(api_key=api_key)
    return client.upload_text(
        title=text,
        user=user,
        platforms=["x"],
        quote_tweet_id=quote_tweet_id,
    )
