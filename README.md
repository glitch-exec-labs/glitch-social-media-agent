# Glitch Social Media Agent

<p align="center">
  <strong>Multi-brand autonomous social media agent</strong><br>
  Two content sources × three publisher paths × N brands, all behind one gitignored <code>.env</code>
</p>

<p align="center">
  <a href="https://github.com/glitch-exec-labs/glitch-social-media-agent/actions/workflows/ci.yml">
    <img src="https://github.com/glitch-exec-labs/glitch-social-media-agent/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
  <img src="https://img.shields.io/badge/python-3.11%2B-blue" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="MIT License">
  <img src="https://img.shields.io/badge/dispatch-dry__run%20%7C%20live-orange" alt="Dispatch Mode">
  <img src="https://img.shields.io/badge/tests-81%20passing-brightgreen" alt="81 tests">
</p>

---

**Built in public so you can run this on your own brand.** Brand configs (voice prompts, guardrail lists, watermark assets, Drive folder IDs) are private and live only on the deployed box — everything else is here.

Founder's time budget on social: **<30 min/week** (approvals only, not execution).

---

## Contents

- [What it does](#what-it-does)
- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Content sources](#content-sources)
- [Publishers + vendor priority](#publishers--vendor-priority)
- [TikTok OAuth flow](#tiktok-oauth-flow)
- [Signed media URLs for vendors](#signed-media-urls-for-vendors)
- [ORM guardrails](#orm-guardrails)
- [Telegram commands](#telegram-commands)
- [Cost model](#cost-model)
- [Roadmap](#roadmap)
- [Deployment](#deployment)
- [Contributing](#contributing)

---

## What it does

The agent runs **two independent content-production paths** feeding into a **three-tier publisher** across **N brands**.

### Content sources

1. **`ai_generated`** — mines GitHub commits and `MILESTONES.md` diffs for novel signals; LLM scores novelty ≥ 0.6; script → storyboard → per-shot video generation (Kling 2.0) → ffmpeg assemble with brand overlay → Gemini 2.5 Pro vision QC.
2. **`drive_footage`** — polls a brand's Google Drive folder for pre-edited clips; downloads via service-account auth; LLM writes caption + hashtags per brand voice; skips the entire video-gen + assembler + QC chain (the footage is already post-ready).

### Publishers (tried in priority order per brand)

1. **Upload-Post** (default) — audited partner app, $16/mo Basic covers 5 brands × unlimited posts × 10+ platforms.
2. **Zernio** (fallback) — second audited partner app, kept wired for vendor-redundancy.
3. **Direct per-platform apps** (YouTube Data API, TikTok Content Posting API) — used once the respective dev app is audited.

All three are gated behind `DISPATCH_MODE=dry_run|live` and short-circuited to synthetic ids in dry-run.

### ORM + review

- Twitter mentions → hard-stop guardrail check → Gemini classifier (7 tiers) → brand-voice responder with per-tier auto-send vs Telegram review vs escalate.
- Telegram bot with inline approve/veto, multi-brand aware (`[Brand]` prefix when >1 brand is configured).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Content-source branch                           │
│                                                                         │
│  ai_generated:                                                          │
│    Scout ──► ScriptWriter ──► Storyboard ──► VideoRouter ──►            │
│                                                                         │
│                                         VideoGenerator (dispatches,     │
│                                                      scheduler polls)   │
│                                         ┌──────────────────────────┐    │
│                                         ▼                          │    │
│                              VideoAssembler ──► QualityCheck       │    │
│                                                      │             │    │
│  drive_footage:                                      ▼             │    │
│    DriveScout ──► CaptionWriter ────────────► TelegramPreview ─────┘    │
│                                                      │                  │
│                                   (48h veto or /approve)                │
│                                                      ▼                  │
└──────────────────────────────────────────────────────┬──────────────────┘
                                                       │
                                                       ▼
                                       ┌────────────────────────────────┐
                                       │   resolve_publish_platform()   │
                                       │   upload_post_* → zernio_* →   │
                                       │   direct (youtube/tiktok/…)    │
                                       └────────────────┬───────────────┘
                                                        │
                              ┌─────────────────────────┼──────────────────────┐
                              ▼                         ▼                      ▼
                    platforms/upload_post.py   platforms/zernio.py   platforms/tiktok.py
                    platforms/youtube.py       platforms/instagram.py     (direct, audit-gated)

        ORM branch:
          MentionMonitor ──► Guardrails ──► Classifier ──► Responder
                                                                │
                                                                ▼
                                              auto-send / Telegram review / escalate
```

**LangGraph** owns the synchronous reasoning chain. The **scheduler** (`scheduler/queue.py`) owns all async work: shot polling, veto windows, ORM review windows, retry backoff, vendor status polls. LangGraph is never held open for 30+ minutes of external-API waiting.

### Conditional entry point

`state["content_source"]` picks the entry node:

- `"ai_generated"` (default) → `scout_node` → existing Kling pipeline
- `"drive_footage"` → `drive_scout_node` → download + `caption_writer_node` → preview

### Stack

| Layer | Library |
|---|---|
| Agent orchestration | LangGraph 0.2+ |
| LLM routing | LiteLLM (Claude Sonnet 4.6 for scripting, Gemini 2.5 Flash/Pro for scoring/QC/captions) |
| HTTP server | FastAPI + uvicorn (port 3111) |
| Database | SQLModel + Alembic + asyncpg (PostgreSQL) |
| Video assembly | ffmpeg-python |
| Encryption | cryptography (Fernet for platform_auth + HMAC for state/media tokens) |
| Drive ingestion | google-api-python-client + google-auth (service-account) |
| Telegram | python-telegram-bot 21.6+ |
| Video generation | Kling 2.0 API (Phase 1) |
| Posting vendors | `upload-post>=2.1`, `zernio-sdk>=1.3` |

---

## Quick start

```bash
# 1. Clone + install
git clone https://github.com/glitch-exec-labs/glitch-social-media-agent
cd glitch-social-media-agent
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 2. Secrets
cp .env.example .env
# Fill in at minimum: SIGNAL_DB_URL, AUTH_ENCRYPTION_KEY, DISPATCH_MODE.
# Add vendor / provider keys only for features you actually use.

# Generate a Fernet key for encrypted platform tokens:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 3. Brand configs (at least one)
cp brand/configs.example/glitch_executor.example.json brand/configs/glitch_executor.json
# Edit platforms.*.enabled, voice, guardrails, etc.

# 4. Database
createdb glitch_social_media_agent
alembic upgrade head

# 5. Start (dry-run first — zero external calls)
DISPATCH_MODE=dry_run uvicorn glitch_signal.server:app --port 3111

# 6. Health check
curl http://127.0.0.1:3111/healthz

# 7. Trigger — pick your source:
curl -X POST 'http://127.0.0.1:3111/jobs/scout'                    # ai_generated
curl -X POST 'http://127.0.0.1:3111/jobs/drive_scout?brand=<id>'   # drive_footage
```

---

## Configuration

### `.env` — THE single secrets + credentials file

Every third-party integration — LLM providers, video models, platform APIs, Telegram, Make.com, vendors — gets credentials in `.env`. That file is **gitignored** (`.env` and `*.env`) and lives only on the deployed box. Operators copy `.env.example` → `.env`, fill in values, restart the service. No sidecar secret files, no credentials in `brand/configs/`, no credentials in code.

Layered config pattern:

| File | Contains | In git |
|---|---|---|
| `.env` | Secrets, API tokens, infra endpoints | ❌ (gitignored) |
| `brand/configs/<brand_id>.json` | Per-brand non-secret tunables | ❌ (gitignored) |
| `brand/prompts/<brand_id>_voice.md` | Per-brand LLM voice guide | ❌ (gitignored) |
| `brand/schema/brand.config.schema.json` | JSON schema validating brand configs | ✅ |
| `brand/configs.example/*.example.json` | Committed templates | ✅ |
| `.env.example` | Every env var the agent reads | ✅ |

### Core env vars

| Variable | When needed | Purpose |
|---|---|---|
| `SIGNAL_DB_URL` | always | Postgres connection string |
| `AUTH_ENCRYPTION_KEY` | always | Fernet key for `platform_auth` tokens + HMAC for OAuth / media state tokens |
| `DISPATCH_MODE` | always | `dry_run` (no external calls) or `live` |
| `VIDEO_STORAGE_PATH` | always | `/var/lib/glitch-social-media-agent/videos` |
| `DEFAULT_BRAND_ID` | always | `glitch_executor` or similar — used when no brand context |
| `TELEGRAM_BOT_TOKEN_SIGNAL`, `TELEGRAM_ADMIN_IDS` | approval previews on | Telegram bot for approve/veto UX |
| `ANTHROPIC_API_KEY` | ai_generated source | Claude Sonnet for script writing + ORM classifier |
| `GOOGLE_API_KEY` | any LLM path | Gemini Flash (scout/caption) + Pro (QC vision) |
| `KLING_API_KEY` | ai_generated source | Kling 2.0 video generation |
| `GITHUB_TOKEN` | ai_generated Scout | Repo scan for novelty signals |
| `GOOGLE_DRIVE_SA_JSON` | drive_footage source | Service-account JSON path for Drive ingestion |
| `TIKTOK_CLIENT_KEY` / `_SECRET` / `_REDIRECT_URI` | direct TikTok path | Own audited app credentials |
| `UPLOAD_POST_API_KEY` | Upload-Post vendor | Bearer token (JWT) |
| `ZERNIO_API_KEY` | Zernio vendor | Bearer token (`sk_…`) |
| `MAKE_API_TOKEN` / `MAKE_BASE_URL` / `MAKE_ORG_ID` | Make.com automations | Zone-bound (us1/us2/eu1/eu2) |
| `YOUTUBE_CLIENT_SECRETS_FILE` | direct YouTube path | OAuth2 client secrets JSON |

### Brand configs

One file per brand under `brand/configs/<brand_id>.json`. Filename stem must equal `brand_id`. Validated against `brand/schema/brand.config.schema.json` at startup.

```
brand/
  configs/                          # gitignored
    glitch_executor.json            # deployed box only
    drive_brand.json
  configs.example/                  # committed templates
    glitch_executor.example.json
    drive_brand.example.json
  schema/
    brand.config.schema.json        # committed
  prompts/                          # gitignored voice guides
    drive_brand_voice.md
```

Each config carries:

- `brand_id` — must equal filename stem
- `display_name`, `timezone`
- `content_source`: `ai_generated` | `drive_footage`
- `drive_folder_id` — required when `content_source == "drive_footage"`
- `voice_prompt_path` — optional markdown file used by `caption_writer`
- `brand.*` — visual identity (colours, watermark, voice string)
- `video_model_routing.model_map` — per-shot-hint → model mapping (ai_generated path)
- `orm_guardrails.*` — hard-stop phrases, tier thresholds, review windows
- `platforms.*` — per-publisher toggle blocks (see next section)
- `default_hashtags` — fallback tag list

**Adding a new brand:** drop a JSON file in `brand/configs/`, match the `brand_id` to the filename stem, restart. No code change.

---

## Content sources

### `ai_generated` (Glitch Executor pattern)

Same as the original agent flow — Scout → ScriptWriter → Storyboard → VideoRouter → VideoGenerator → VideoAssembler → QualityCheck → TelegramPreview → Publisher. Requires `ANTHROPIC_API_KEY`, `GOOGLE_API_KEY`, `KLING_API_KEY`, `GITHUB_TOKEN`.

Trigger: `POST /jobs/scout` (optionally with `{signal_id, platform}` body to run per-signal).

### `drive_footage` (pattern)

Brand supplies pre-edited video clips via a shared Google Drive folder. The agent picks them up, writes a caption, previews, publishes — no video gen.

**Setup:**
1. Create a GCP service account with Drive-readonly scope. Download the JSON key.
2. `GOOGLE_DRIVE_SA_JSON=/path/to/sa.json` in `.env`.
3. Share the brand's Drive folder with the SA email (`<name>@<project>.iam.gserviceaccount.com`) as **Viewer**.
4. In `brand/configs/<brand_id>.json`:
   ```json
   {
     "content_source": "drive_footage",
     "drive_folder_id": "<33-char Drive folder ID>",
     ...
   }
   ```

Trigger: `POST /jobs/drive_scout?brand=<brand_id>`

Pipeline per invocation:
- List video files in folder → dedup against existing `Signal(source="drive", source_ref=<file_id>)` rows
- Download each new file to `{VIDEO_STORAGE_PATH}/drive/<brand_id>/<file_id><ext>`
- Create `Signal`, promote the first new one through `caption_writer_node`
- `caption_writer` generates title + caption + hashtags via Gemini Flash using the brand's `voice_prompt_path`
- Writes `ContentScript` + `VideoAsset` with `assembler_version="drive_passthrough@1.0"` (bypass marker)
- Hands off to `telegram_preview` → `publisher`

---

## Publishers + vendor priority

The agent supports three publisher tiers. `resolve_publish_platform(brand_id, target)` walks `_PUBLISH_PRIORITY` in `config.py` and picks the first enabled block:

```
tiktok     →  upload_post_tiktok    → zernio_tiktok    → tiktok      (direct)
instagram  →  upload_post_instagram → zernio_instagram → instagram_reels
youtube    →  upload_post_youtube   → zernio_youtube   → youtube_shorts
x          →  upload_post_x         → zernio_twitter   → twitter
facebook   →  upload_post_facebook  → zernio_facebook
threads    →  upload_post_threads
pinterest  →  upload_post_pinterest
bluesky    →  upload_post_bluesky
reddit     →  upload_post_reddit
linkedin   →  upload_post_linkedin
```

First block with `enabled=true` wins. Raises clearly if nothing is enabled.

### Upload-Post (default)

- Lives at `src/glitch_signal/platforms/upload_post.py`.
- Platform keys: `upload_post_tiktok`, `upload_post_instagram`, `upload_post_youtube`, etc.
- Per-brand config needs `user` (Upload-Post managed-user profile name).
- Publish flow: issue HMAC-signed public URL → `upload_video(platforms=["tiktok"])` → poll `get_status(request_id)` every 3s until the target platform's result block carries `platform_post_id` + `post_url`.

### Zernio (fallback)

- Lives at `src/glitch_signal/platforms/zernio.py`.
- Platform keys: `zernio_tiktok`, `zernio_instagram`, etc.
- Per-brand config needs `account_id` (Zernio internal id from `client.accounts.list()`, **not** the social platform's handle).

### Direct per-platform

- `platforms/tiktok.py` — own TikTok app; audited-app-gated. Currently can only post to private-account users in sandbox mode (`unaudited_client_can_only_post_to_private_accounts`).
- `platforms/youtube.py` — own YouTube Data API OAuth. Requires one-time browser auth via `python -m glitch_signal.platforms.youtube --auth`.
- `platforms/twitter.py`, `platforms/instagram.py` — stubs until respective audits land.

---

## TikTok OAuth flow

TikTok integration ships in two independent modes:

1. **Via Upload-Post or Zernio** — vendor OAuths the creator through their audited app, we just call `upload_video(user=<profile>)`. No OAuth plumbing on our side.
2. **Direct** — we OAuth the creator through our own app. Tokens encrypted at rest (Fernet) in `platform_auth`.

### Direct-mode setup (gated on audit approval)

1. Register the app at https://developers.tiktok.com with redirect URI `https://grow.glitchexecutor.com/oauth/tiktok/callback`
2. Fill `.env`:
   ```
   TIKTOK_CLIENT_KEY=...
   TIKTOK_CLIENT_SECRET=...
   AUTH_ENCRYPTION_KEY=<Fernet.generate_key()>
   ```
3. Add `platforms.tiktok.enabled=true` to the brand config
4. Visit `https://grow.glitchexecutor.com/oauth/tiktok/start?brand=<brand_id>` in a browser signed into the target TikTok account → success page + encrypted row in `platform_auth`
5. Submit the app for Production audit in the TikTok dev portal. Until that clears, direct-post is blocked at the API level for public accounts; `upload_post_*` / `zernio_*` handle the gap.

### Nginx proxy

The OAuth callback and the signed `/media/fetch` endpoint are both served by this service on :3111, reachable through `grow.glitchexecutor.com` via nginx:

```nginx
location /oauth/tiktok/ {
    proxy_pass http://127.0.0.1:3111;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto https;
}

location /media/ {
    proxy_pass http://127.0.0.1:3111;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto https;
    proxy_read_timeout 300s;
    proxy_buffering off;
    client_max_body_size 500m;
}

# Upload-Post webhook callbacks (upload_completed, reauth_required, …)
location /webhooks/upload_post/ {
    proxy_pass http://127.0.0.1:3111;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto https;
}
```

---

## Signed media URLs for vendors

Posting vendors (Upload-Post, Zernio) fetch the video server-side. Rather than re-uploading each 80+ MB file through Python to their CDN, the agent issues a short-lived HMAC-signed URL to the local file and hands that to the vendor.

```
GET https://grow.glitchexecutor.com/media/fetch?token=<hmac-signed-payload>
```

Security properties:
- HMAC signature (shared secret = `AUTH_ENCRYPTION_KEY`) → forged or altered tokens fail
- Token carries absolute filesystem path; resolution confined to `VIDEO_STORAGE_PATH`
- `"k": "media"` kind field separates these tokens from OAuth state tokens
- Default 60-minute TTL
- 403 on any signature / path-escape failure; 404 on missing file

---

## Upload-Post webhooks

The Upload-Post publisher hands the video to the vendor and returns immediately — it no longer blocks on `get_status`. Finalization (writing `PublishedPost`, flipping `scheduled_post.status` to `done`) happens when Upload-Post POSTs the `upload_completed` event to our `/webhooks/upload_post/<secret>` endpoint.

Setup:

1. Generate a random secret:
   ```
   python -c 'import secrets; print(secrets.token_urlsafe(32))'
   ```
2. Put it in `.env` as `UPLOAD_POST_WEBHOOK_SECRET=…` and restart the service.
3. Register the webhook URL with Upload-Post:
   ```
   source .venv/bin/activate
   set -a; source .env; set +a
   python scripts/register_upload_post_webhook.py
   ```

The URL path segment IS the secret (Upload-Post does not sign webhook bodies). If you rotate the secret, re-run the registration script.

**Fallback:** if a webhook doesn't arrive within `UPLOAD_POST_WEBHOOK_RECONCILE_AFTER_S` (default 10 min) after dispatch, the scheduler polls `get_status(request_id)` once per tick and finalizes the row that way. Covers dropped webhooks / us being down during the callback.

---

## ORM guardrails

Hard-stop phrases trigger an **immediate Telegram alert and zero automated response** — no LLM involved, pure rule engine:

- Financial loss mentions (`"lost $"`, `"lost ₹"`, `"money lost"`)
- Regulatory bodies (`SEC`, `SEBI`, `FINRA`)
- Legal threats (`"legal action"`, `"lawsuit"`, `"lawyer"`)
- Return guarantees (`"guarantee"`, `"certain returns"`)

Edit `brand/configs/<brand_id>.json` → `orm_guardrails.hard_stop_phrases` to update without redeploy. Each brand has its own list.

### Response tiers

| Tier | Action |
|---|---|
| `positive` | Auto-respond immediately — warm, brief, brand voice |
| `neutral_faq` | Auto-respond — link to docs + one concrete answer |
| `neutral_technical` | Open GitHub issue, reply with issue link |
| `negative_mild` | Draft → 2h review window → Telegram approval |
| `negative_severe` | Telegram alert only, no response queued |
| `legal_flag` | Telegram alert only, no response queued |
| `spam` | Ignore |

---

## Telegram commands

```
/status           queue depth, last signal, cost this week (per-brand breakdown if >1 brand)
/signals          last 5 discovered signals with novelty score
/preview <id>     re-send a video preview
/approve <id>     publish immediately (skips 48h window)
/veto <id>        cancel a queued post
/orm              last 10 inbound mentions with tier
/orm_approve <id> send a pending ORM response now
/orm_veto <id>    cancel a pending ORM response
```

Preview messages include an inline keyboard for one-tap approve/veto. In multi-brand mode every preview card is prefixed with `[<display_name>]` so operators don't confuse brands.

---

## Cost model

### Per-post (drive_footage path — pattern, vendor-published)

| Line item | Cost |
|---|---|
| Caption generation (Gemini Flash) | ~$0.001 |
| Drive fetch + our bandwidth | ~negligible |
| Vendor publish (Upload-Post Basic $16/mo ÷ posts) | $0.03 at 500 posts/mo, $0.16 at 100 posts/mo |
| **Total** | **~$0.01–0.16 per post** |

### Per-post (ai_generated path — Glitch Executor style)

| Scenario | Per video | At 3×/week |
|---|---|---|
| Phase 1 (Kling 2.0 only, 12 shots × 5s) | ~$1.75 | ~$21/month |
| Phase 2 (2 Runway hero + 10 Kling shots) | ~$4.00 | ~$50/month |

Breakdown: `12 shots × 5s × $0.028/s = $1.68` + LLM `~$0.05` + storage `~negligible`.

### Vendor comparison (cheapest first, for reference)

| Vendor | Entry plan | Cap | Notes |
|---|---|---|---|
| Post for Me | $10/mo | 1k posts, unlimited profiles | Not wired here yet |
| Upload-Post Basic | $16/mo (annual) | unlimited posts, 5 profiles | **Wired, default** |
| Zernio Build | $19/mo | 120 posts, 10 profiles | Wired, fallback |
| Ayrshare | $149+/mo | — | Enterprise-priced |

---

## Roadmap

### Phase 1 — MVP (shipped)
- [x] Scout: GitHub commits + `MILESTONES.md`
- [x] Script + storyboard + video routing (Kling 2.0)
- [x] ffmpeg assembly with brand overlay
- [x] Gemini 2.5 Pro vision QC
- [x] 48h Telegram veto window
- [x] YouTube Shorts publishing
- [x] ORM: Twitter mentions → auto-respond/escalate
- [x] Telegram bot with full approval UX

### Phase 2 — Multi-brand + multi-source + vendor-fanout (shipped 2026-04-17)
- [x] Multi-brand config layer (per-file, schema-validated)
- [x] `drive_footage` content source (Drive scout + caption writer)
- [x] TikTok OAuth flow (direct path wired; audit-gated)
- [x] Upload-Post + Zernio publishers
- [x] Publisher priority resolver (`upload_post` → `zernio` → direct)
- [x] Signed `/media/fetch` for vendor fetches (HMAC + TTL + path confinement)
- [x] Make.com credentials wired (no scenarios yet)
- [x] Encrypted `platform_auth` (Fernet)

### Phase 3 — Quality + analytics + wider ORM
- [ ] Vision-based captioning (Gemini 2.5 Pro reads video → caption from actual content, not filename)
- [ ] ffmpeg pre-publish transform (audio swap / crop / burn-in captions)
- [ ] Analytics digest (weekly Telegram summary using `upload_post.get_analytics`)
- [ ] Webhook receivers for Upload-Post publish/comment events (skip polling)
- [ ] ORM for YouTube comments + Instagram DMs
- [ ] MCP server on port 3112 (`trigger_scout`, `approve_post`, `veto_post`, `orm_summary`)

### Phase 4 — Direct-app audit retirement
- [ ] TikTok Content Posting API audit submission + approval
- [ ] Instagram `instagram_content_publish` audit
- [ ] Flip brands from `upload_post_*` back to direct per-platform publishers
- [ ] Retire vendors for platforms we've audited

---

## Deployment

### systemd

```bash
sudo cp ops/systemd/glitch-social-media-agent.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now glitch-social-media-agent
```

The unit runs `uvicorn glitch_signal.server:app --host 127.0.0.1 --port 3111` under the `support` user, reads `/home/support/glitch-social-media-agent/.env` as EnvironmentFile, and hardens with `NoNewPrivileges`, `PrivateTmp`, `ProtectSystem=strict`, `ProtectHome=read-only` + an explicit `ReadWritePaths` for the repo + video-storage dir.

### Video storage

```bash
sudo mkdir -p /var/lib/glitch-social-media-agent/videos
sudo chown -R support:support /var/lib/glitch-social-media-agent
```

### nginx (reverse proxy + TLS)

See the nginx snippet in [TikTok OAuth flow](#tiktok-oauth-flow) for the `/oauth/tiktok/` and `/media/` location blocks on `grow.glitchexecutor.com`. The main service (`signal.glitchexecutor.com`) uses the baseline config in `ops/nginx/`.

### Flipping to live

Keep `DISPATCH_MODE=dry_run` in `.env` until:
1. A brand config exists with at least one `platforms.*.enabled=true`
2. The publisher path has been exercised once in dry-run against real data
3. Operator has confirmed a preview would look correct

Then set `DISPATCH_MODE=live` and restart. Publishers short-circuit individually on missing credentials — there's no way to "accidentally post" without the relevant brand config flag turned on.

---

## Contributing

```bash
pip install -e ".[dev]"
ruff check src/ tests/
DISPATCH_MODE=dry_run pytest tests/ -v   # no API keys needed
```

Highest-value contributions:
- New video models (subclass `video_models/base.py`)
- New platform publishers (mirror `platforms/upload_post.py` shape)
- New content sources (mirror `agent/nodes/drive_scout.py` shape; add a branch in `agent/graph.py::_entry_router`)

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide.

---

## License

MIT — see [LICENSE](LICENSE).

Brand configs (voice prompts, guardrail lists, watermark assets, Drive folder IDs) are private and not included in this repository.

---

Built by [Glitch Executor](https://glitchexecutor.com).
