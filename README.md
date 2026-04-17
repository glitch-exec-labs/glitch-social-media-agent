# Glitch Social Media Agent

<p align="center">
  <strong>Autonomous social media identity agent for <a href="https://glitchexecutor.com">Glitch Executor</a></strong><br>
  Mines shipped artifacts → generates short-form video → publishes to YouTube Shorts / X / Instagram Reels → manages ORM autonomously
</p>

<p align="center">
  <a href="https://github.com/glitch-exec-labs/glitch-social-media-agent/actions/workflows/ci.yml">
    <img src="https://github.com/glitch-exec-labs/glitch-social-media-agent/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
  <img src="https://img.shields.io/badge/python-3.11%2B-blue" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="MIT License">
  <img src="https://img.shields.io/badge/dispatch-dry__run%20%7C%20live-orange" alt="Dispatch Mode">
  <img src="https://img.shields.io/badge/video-Kling%202.0-purple" alt="Kling 2.0">
</p>

---

**Built in public so you can run this on your own brand.** The Glitch Executor brand config (voice prompts, guardrail lists, watermark assets) is private — everything else is here.

Founder's time budget on social: **<30 min/week** (approvals only, not execution).

---

## Contents

- [What it does](#what-it-does)
- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [ORM guardrails](#orm-guardrails)
- [Telegram commands](#telegram-commands)
- [Cost model](#cost-model)
- [Roadmap](#roadmap)
- [Deployment](#deployment)
- [Contributing](#contributing)

---

## What it does

1. **Scout** — polls GitHub commits and `MILESTONES.md` diffs for novel signals; LLM scores novelty 0–1, queues anything ≥ 0.6
2. **Script + Storyboard** — LLM generates a 60–90s short-form script and breaks it into 5–8 shots with per-shot model hints
3. **Video generation** — routes each shot to the best available model (Kling 2.0 in Phase 1); dispatches jobs async and polls via scheduler — no blocking
4. **Assemble** — ffmpeg concatenates shots, applies brand overlay (cobra watermark + neon color grade), outputs H.264 1080×1920 30fps
5. **QC** — Gemini 2.5 Pro vision checks brand alignment on a 5-point checklist before publish
6. **Telegram preview** — sends video to founder with 48h veto window; auto-publishes if no veto
7. **ORM** — monitors mentions, classifies tier, auto-responds within hard guardrails, escalates legal/severe to Telegram

---

## Architecture

```
GitHub / Metrics / MILESTONES
         │
      [Scout] ──cron──────────────────────────────────────
         │                                               │
  [ScriptWriter]                                  [ORM Monitor]
         │                                               │
   [Storyboard]                                 [Guardrails check]
         │                                               │
  [VideoRouter]                                  [Classifier]
         │                                               │
[VideoGenerator] ──dispatches VideoJob rows──→  [Responder]
         │       scheduler polls for completion        │
  [VideoAssembler]                              auto-send / escalate
         │
  [QualityCheck]
         │
[TelegramPreview] ──48h veto window──→ [Publisher]
                                   YouTube / X / Instagram
```

**LangGraph** owns the synchronous reasoning chain (Scout → QC). The **scheduler** (`scheduler/queue.py`) owns all async operations: shot polling, veto windows, ORM review windows, retry backoff. This means LangGraph is never held open for 30+ minutes of video API polling.

### Stack

| Layer | Library |
|---|---|
| Agent orchestration | LangGraph 0.2+ |
| LLM routing | LiteLLM (Claude Sonnet 4.6, Gemini 2.5 Flash/Pro) |
| HTTP server | FastAPI + uvicorn (port 3111) |
| Database | SQLModel + Alembic + asyncpg (PostgreSQL) |
| Video assembly | ffmpeg-python |
| Telegram | python-telegram-bot 21.6+ |
| Video generation | Kling 2.0 API (Phase 1) |

---

## Quick start

```bash
# 1. Clone + install
git clone https://github.com/glitch-exec-labs/glitch-social-media-agent
cd glitch-social-media-agent
python -m venv .venv && source .venv/bin/activate
pip install -e .

# 2. Configure
cp .env.example .env
# Fill in: KLING_API_KEY, ANTHROPIC_API_KEY, TELEGRAM_BOT_TOKEN_SIGNAL,
#          TELEGRAM_ADMIN_IDS, YOUTUBE_CLIENT_SECRETS_FILE

cp brand/configs.example/glitch_executor.example.json brand/configs/glitch_executor.json
# Edit: watermark_path, competitor_names, model routing, platforms.*.enabled

# 3. Database
createdb glitch_signal
alembic upgrade head

# 4. YouTube auth (one-time browser flow)
python -m glitch_signal.platforms.youtube --auth

# 5. Start in dry-run (zero real API calls)
DISPATCH_MODE=dry_run uvicorn glitch_signal.server:app --port 3111

# 6. Trigger a scout run
curl -X POST http://127.0.0.1:3111/jobs/scout

# 7. Check health
curl http://127.0.0.1:3111/healthz
```

---

## Configuration

### `.env` — the single secrets + credentials file

Every third-party integration this agent talks to (LLM providers, video
models, platform APIs, Telegram, Make.com automation, etc.) gets its
credentials in `.env`. That file is **gitignored** (`.env` and `*.env`) and
lives only on the deployed box. Operators copy `.env.example` → `.env`,
fill in values, and restart the service. No sidecar secret files, no
credentials in `brand/configs/`, no credentials in code.

Layered config pattern:

| File | Contains | In git |
|---|---|---|
| `.env` | Secrets, API tokens, infra endpoints | ❌ (gitignored) |
| `brand/configs/<brand_id>.json` | Per-brand non-secret tunables (hashtags, guardrail lists, platform toggles) | ❌ (gitignored) |
| `brand/schema/brand.config.schema.json` | Schema that validates brand configs | ✅ |
| `brand/configs.example/*.example.json` | Templates showing config shape | ✅ |
| `.env.example` | Template showing every env var the agent reads | ✅ |

| Variable | Required | Description |
|---|---|---|
| `SIGNAL_DB_URL` | yes | `postgresql+asyncpg://user:pass@host/glitch_signal` |
| `ANTHROPIC_API_KEY` | yes | Claude Sonnet (script writer, ORM classifier) |
| `GOOGLE_API_KEY` | yes | Gemini Flash (scout scorer) + Pro (QC vision) |
| `KLING_API_KEY` | yes | Kling 2.0 video generation |
| `TELEGRAM_BOT_TOKEN_SIGNAL` | yes | Telegram bot for approvals + ORM alerts |
| `TELEGRAM_ADMIN_IDS` | yes | Comma-separated Telegram user IDs |
| `YOUTUBE_CLIENT_SECRETS_FILE` | yes | Path to OAuth2 client secrets JSON |
| `GITHUB_TOKEN` | yes | GitHub API token for commit scanning |
| `DISPATCH_MODE` | yes | `dry_run` (no external calls) or `live` |
| `VIDEO_STORAGE_PATH` | yes | Directory for generated videos |
| `MAKE_BASE_URL` | when Make.com is used | Zone-specific dashboard URL (default `https://us2.make.com`) |
| `MAKE_API_BASE` | when Make.com is used | Zone-specific API base (default `https://us2.make.com/api/v2`) |
| `MAKE_ORG_ID` | when Make.com is used | Make.com organisation ID |
| `MAKE_API_TOKEN` | when Make.com is used | Make.com API token — zone-bound, keep secret |

Zone note: Make.com tokens are **zone-bound** (us1 / us2 / eu1 / eu2). A
token issued on `us2` will be rejected by `us1`. If the org moves zones,
both `MAKE_BASE_URL` and `MAKE_API_TOKEN` must be rotated together.

### Brand configs — multi-brand, per-file

One file per brand under `brand/configs/<brand_id>.json`. All files in that
directory are gitignored; real values only live on the deployed box. Committed
templates live in `brand/configs.example/`. Every file is validated against
`brand/schema/brand.config.schema.json`.

```
brand/
  configs/                     # gitignored
    glitch_executor.json       # real values — deployed only
    nmahya.json                # added by Nmahya onboarding
  configs.example/             # committed templates
    glitch_executor.example.json
    nmahya.example.json
  schema/
    brand.config.schema.json
  prompts/                     # gitignored voice guides per brand
    nmahya_voice.md
```

Each config carries:

- `brand_id` — must equal the filename stem (e.g. `glitch_executor`)
- `display_name`, `timezone`, `content_source` (`ai_generated` | `drive_footage`)
- `brand.watermark_path` — watermark image for video overlay
- `video_model_routing.model_map` — per-shot-hint model routing table
- `orm_guardrails.hard_stop_phrases` — phrases that trigger immediate escalation
- `orm_guardrails.competitor_names` — auto-escalate competitor mentions
- `platforms.<youtube|twitter|instagram|tiktok>` — per-platform toggles & metadata

`DEFAULT_BRAND_ID` (env) picks which brand is used when no brand context is
available (legacy Glitch Executor scout runs, ORM monitor mentions, etc.).

**Adding a new brand:** drop a JSON file in `brand/configs/`, make sure its
`brand_id` matches the filename stem, restart the service. No code change,
no redeploy beyond config.

---

## TikTok Content Posting API

TikTok publishing uses the official Content Posting API. OAuth tokens are
stored encrypted at rest (Fernet, key = `AUTH_ENCRYPTION_KEY`) in the
`platform_auth` table.

### Setup (once per brand)

1. Register the app at <https://developers.tiktok.com>. Add redirect URI:
   `https://grow.glitchexecutor.com/oauth/tiktok/callback` (must match
   `TIKTOK_REDIRECT_URI` exactly).
2. Fill `.env`:
   ```
   TIKTOK_CLIENT_KEY=<from dev portal>
   TIKTOK_CLIENT_SECRET=<from dev portal>
   AUTH_ENCRYPTION_KEY=<python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
   ```
3. Set `platforms.tiktok.enabled = true` in `brand/configs/<brand_id>.json`.
4. Visit `https://grow.glitchexecutor.com/oauth/tiktok/start?brand=<brand_id>`
   in a browser signed into the target TikTok account. After approval you
   land on a "TikTok connected" page and a row is written to `platform_auth`.

### Nginx proxy on `grow.glitchexecutor.com`

The OAuth callback is registered on `grow.glitchexecutor.com` but served by
this service on port 3111. Add one `location` block to the nginx config for
that host:

```nginx
location /oauth/tiktok/ {
    proxy_pass http://<signal-host>:3111;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto https;
}
```

`<signal-host>` is whichever internal address resolves to this service
(localhost if co-located, otherwise the signal box's private IP).

### Required scopes

- `user.info.basic` — always granted
- `video.upload` — upload to creator inbox (manual tap to publish)
- `video.publish` — direct post (auto-publish) — **the one this repo uses**

If only `video.upload` is approved, swap `platforms/tiktok.py::_INIT_PATH`
to `/v2/post/publish/inbox/video/init/` — everything else is identical and
the creator will tap Post in the TikTok app.

---

## ORM guardrails

Hard-stop phrases trigger an **immediate Telegram alert and zero automated response** — no LLM involved, pure rule engine:

- Financial loss mentions (`"lost $"`, `"lost ₹"`, `"money lost"`)
- Regulatory bodies (`SEC`, `SEBI`, `FINRA`)
- Legal threats (`"legal action"`, `"lawsuit"`, `"lawyer"`)
- Return guarantees (`"guarantee"`, `"certain returns"`)

Edit `brand/configs/<brand_id>.json` → `orm_guardrails.hard_stop_phrases` to update without redeploy. Each brand has its own guardrail list.

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
/status           queue depth, last signal, cost this week
/signals          last 5 discovered signals with novelty score
/preview <id>     re-send a video preview
/approve <id>     publish immediately (skips 48h window)
/veto <id>        cancel a queued post
/orm              last 10 inbound mentions with tier
/orm_approve <id> send a pending ORM response now
/orm_veto <id>    cancel a pending ORM response
```

Preview messages include an inline keyboard for one-tap approve/veto.

---

## Cost model

| Scenario | Per video | At 3×/week |
|---|---|---|
| Phase 1 (Kling 2.0 only, 12 shots × 5s) | ~$1.75 | ~$21/month |
| Phase 2 (2 Runway hero + 10 Kling shots) | ~$4.00 | ~$50/month |

Cost breakdown: `12 shots × 5s × $0.028/s = $1.68` + LLM `~$0.05` + storage `~negligible`.

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

### Phase 2 — Full distribution
- [ ] Multi-model router: Runway Gen-4, Veo 3, Hailuo
- [ ] X/Twitter video publishing (requires Basic tier)
- [ ] Instagram Reels publishing (requires `instagram_content_publish` approval)
- [ ] ORM: YouTube comments + Instagram comments/DMs
- [ ] Scout: trading metrics from Glitch Executor PostgreSQL

### Phase 3 — Analytics + open ecosystem
- [ ] Weekly Telegram analytics digest
- [ ] Analyst node: content optimisation from MetricsSnapshot data
- [ ] MCP server on port 3112 (`trigger_scout`, `approve_post`, `veto_post`, `orm_summary`)

---

## Deployment

### systemd

```bash
sudo cp ops/systemd/glitch-signal.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now glitch-signal
```

### nginx (reverse proxy + TLS)

```bash
sudo cp ops/nginx/signal.glitchexecutor.com.conf /etc/nginx/sites-available/
sudo ln -s /etc/nginx/sites-available/signal.glitchexecutor.com.conf \
           /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

Set `DISPATCH_MODE=live` in `.env` before enabling the systemd service.

### Video storage

```bash
sudo mkdir -p /var/lib/glitch-signal/videos
sudo chown support:support /var/lib/glitch-signal
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The short version:

```bash
pip install -e ".[dev]"
DISPATCH_MODE=dry_run pytest tests/ -v   # no API keys needed
ruff check src/ tests/
```

New video models and platform publishers are the highest-value contributions — see the guide in CONTRIBUTING.md.

---

## License

MIT — see [LICENSE](LICENSE).

Brand config (voice prompts, guardrail lists, watermark assets) is private and not included in this repository.

---

Built by [Glitch Executor](https://glitchexecutor.com) — algorithmic trading AI platform.
