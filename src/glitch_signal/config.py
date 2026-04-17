"""Central configuration for Glitch Social Media Agent.

All settings are loaded from .env (or environment variables).
Call settings() anywhere — the result is cached after first load.

Brand configs live in brand/configs/<brand_id>.json (gitignored). Each file
is validated against brand/schema/brand.config.schema.json and merged into
settings().brands. Legacy brand.config.json at repo root is still honoured
and registered as the default brand for backward compatibility.
"""
from __future__ import annotations

import json
import logging
import pathlib
from functools import lru_cache
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Databases ---
    signal_db_url: str = "postgresql+asyncpg://signal:changeme@127.0.0.1:5432/glitch_signal"
    # Read-only access to glitchexecutor DB for Phase 2 Scout
    glitch_ro_url: str = ""

    # --- LLMs ---
    anthropic_api_key: str = ""
    google_api_key: str = ""
    vertex_project: str = ""
    vertex_location: str = "us-central1"

    # --- Video models ---
    kling_api_key: str = ""
    kling_api_url: str = "https://api.klingai.com"
    runway_api_key: str = ""    # Phase 2
    veo_api_key: str = ""       # Phase 2
    hailuo_api_key: str = ""    # Phase 2

    # --- Platforms (Phase 1: YouTube) ---
    youtube_client_secrets_file: str = "credentials/youtube_client_secrets.json"
    youtube_channel_id: str = ""
    # Phase 2
    twitter_api_key: str = ""
    twitter_api_secret: str = ""
    twitter_access_token: str = ""
    twitter_access_token_secret: str = ""
    twitter_bearer_token: str = ""
    ig_access_token: str = ""
    ig_user_id: str = ""

    # --- Telegram ---
    telegram_bot_token_signal: str = ""
    telegram_admin_ids: str = ""  # csv of int ids, e.g. "6280075826,123456"

    # --- Storage ---
    video_storage_path: str = "/var/lib/glitch-signal/videos"

    # --- Runtime ---
    public_base_url: str = "https://signal.glitchexecutor.com"
    dispatch_mode: str = "live"   # dry_run | live
    log_level: str = "INFO"
    scheduler_tick_ms: int = 30_000
    scheduler_stuck_after_ms: int = 300_000   # 5 min

    # --- Scout ---
    github_token: str = ""
    github_org: str = "glitch-exec-labs"
    github_repos: str = ""  # csv of repo names; empty = all org repos

    # --- Google Drive (drive_footage content source) ---
    # Service-account JSON path. SA email must have Viewer on each brand's
    # Drive folder. Empty = drive_scout is disabled.
    google_drive_sa_json: str = ""

    # --- Brand ---
    brand_config_path: str = "brand.config.json"          # legacy single-file (still supported)
    brand_configs_dir: str = "brand/configs"              # multi-brand dir
    default_brand_id: str = "glitch_executor"

    # --- OAuth + token storage ---
    # Generate: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    auth_encryption_key: str = ""

    # --- TikTok Content Posting API ---
    tiktok_client_key: str = ""
    tiktok_client_secret: str = ""
    tiktok_redirect_uri: str = "https://grow.glitchexecutor.com/oauth/tiktok/callback"
    tiktok_api_base: str = "https://open.tiktokapis.com"
    tiktok_auth_base: str = "https://www.tiktok.com"
    tiktok_default_scopes: str = "user.info.basic,video.upload,video.publish"
    tiktok_post_status_timeout_s: int = 180

    # --- Make.com (automation platform) ---
    # Zone-bound. us1 / us2 / eu1 / eu2 — do NOT mix zones across base URL
    # and token; a token issued on us2 is rejected by us1 and vice versa.
    make_base_url: str = "https://us2.make.com"
    make_api_base: str = "https://us2.make.com/api/v2"
    make_org_id: str = ""
    make_api_token: str = ""

    # --- Zernio (audited multi-platform social-posting API) ---
    # Used as a parallel publisher ("zernio_tiktok", "zernio_instagram", …)
    # when our own per-platform dev apps are unaudited. See platforms/zernio.py.
    zernio_api_key: str = ""
    zernio_base_url: str = "https://zernio.com/api"

    # --- Upload-Post (alternative audited multi-platform vendor) ---
    # Cheaper than Zernio at real volume. Platform keys: "upload_post_tiktok",
    # "upload_post_instagram", etc. See platforms/upload_post.py.
    upload_post_api_key: str = ""

    # --- Media-serve public base URL ---
    # Zernio fetches videos from this host when posts are published via
    # the zernio_* publishers. An nginx location block on this hostname
    # proxies /media/* to the FastAPI app on 127.0.0.1:3111.
    media_public_base_url: str = "https://grow.glitchexecutor.com"

    # --- Retry windows (ms) ---
    publish_retry_1_ms: int = 1_800_000   # 30 min
    publish_retry_2_ms: int = 7_200_000   # 2 h
    orm_review_window_s: int = 7_200      # 2 h

    @property
    def admin_telegram_ids(self) -> set[int]:
        out: set[int] = set()
        for raw in (self.telegram_admin_ids or "").split(","):
            raw = raw.strip()
            if raw.isdigit():
                out.add(int(raw))
        return out

    @property
    def github_repo_list(self) -> list[str]:
        if not self.github_repos:
            return []
        return [r.strip() for r in self.github_repos.split(",") if r.strip()]

    @property
    def is_dry_run(self) -> bool:
        return self.dispatch_mode.strip().lower() == "dry_run"


@lru_cache
def settings() -> Settings:
    return Settings()


# ---------------------------------------------------------------------------
# Brand config registry — loaded once, keyed by brand_id
# ---------------------------------------------------------------------------

_brand_registry: dict[str, dict] | None = None


def _load_brand_registry() -> dict[str, dict]:
    """Discover and load every brand config file under brand/configs/.

    Precedence (highest first):
      1. Files under brand_configs_dir (one file per brand, stem = brand_id).
      2. Legacy brand.config.json at repo root, registered as default brand.
      3. Built-in defaults (glitch_executor only) — tolerated, warned about.

    Each loaded config is normalised to include a 'brand_id' field matching
    the filename stem. Files whose internal brand_id disagrees with the stem
    are rejected loudly.
    """
    s = settings()
    registry: dict[str, dict] = {}

    configs_dir = pathlib.Path(s.brand_configs_dir)
    if configs_dir.is_dir():
        for path in sorted(configs_dir.glob("*.json")):
            stem = path.stem
            if stem.startswith("."):
                continue
            try:
                data = json.loads(path.read_text())
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"Invalid JSON in brand config {path}: {exc}"
                ) from exc

            internal_id = data.get("brand_id")
            if internal_id and internal_id != stem:
                raise RuntimeError(
                    f"Brand config {path} has brand_id={internal_id!r} "
                    f"but filename stem is {stem!r}. These must match."
                )
            data.setdefault("brand_id", stem)
            registry[stem] = data

    # Legacy single-file fallback (pre-multi-brand deployments).
    legacy_path = pathlib.Path(s.brand_config_path)
    if not registry and legacy_path.exists():
        try:
            legacy = json.loads(legacy_path.read_text())
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Invalid JSON in legacy {legacy_path}: {exc}"
            ) from exc
        legacy.setdefault("brand_id", s.default_brand_id)
        legacy.setdefault("display_name", legacy.get("brand", {}).get("name", s.default_brand_id))
        legacy.setdefault("timezone", "UTC")
        registry[s.default_brand_id] = legacy

    # Built-in safety net so tests and fresh clones run without setup.
    if not registry:
        logger.warning(
            "No brand configs found at %s/*.json or %s — falling back to "
            "built-in Glitch Executor defaults. Drop a real config at "
            "%s/%s.json for production.",
            s.brand_configs_dir,
            s.brand_config_path,
            s.brand_configs_dir,
            s.default_brand_id,
        )
        registry[s.default_brand_id] = _default_brand_config()

    # Default brand must be present.
    if s.default_brand_id not in registry:
        raise RuntimeError(
            f"default_brand_id={s.default_brand_id!r} has no matching config "
            f"file. Available brands: {sorted(registry.keys())}"
        )

    return registry


def _brands() -> dict[str, dict]:
    global _brand_registry
    if _brand_registry is None:
        _brand_registry = _load_brand_registry()
    return _brand_registry


def brand_ids() -> list[str]:
    """All configured brand ids, sorted."""
    return sorted(_brands().keys())


def brand_config(brand_id: str | None = None) -> dict:
    """Return the config dict for brand_id, or the default brand if None.

    Kept backward-compatible: existing callers that pass no argument still
    get the same single-brand config they used to read from brand.config.json.
    """
    registry = _brands()
    key = brand_id or settings().default_brand_id
    if key not in registry:
        raise KeyError(
            f"Unknown brand_id {key!r}. Configured brands: {sorted(registry.keys())}"
        )
    return registry[key]


def _reset_brand_registry_for_tests() -> None:
    """Test-only: force the registry to be reloaded on next access."""
    global _brand_registry
    _brand_registry = None


def _default_brand_config() -> dict[str, Any]:
    s = settings()
    return {
        "brand_id": s.default_brand_id,
        "display_name": "Glitch Social Media Agent",
        "timezone": "UTC",
        "content_source": "ai_generated",
        "brand": {
            "name": "Glitch Social Media Agent",
            "accent_color": "#00ff88",
            "base_color": "#0a0a0f",
            "watermark_path": "assets/brand/mascot-128.png",
            "voice": "technical, direct, no marketing hype, no emoji walls",
        },
        "video_model_routing": {
            "phase": 1,
            "model_map": {
                "cinematic": "kling_2",
                "realistic": "kling_2",
                "text_in_video": "kling_2",
                "fast": "kling_2",
            },
        },
        "orm_guardrails": {
            "hard_stop_phrases": [
                "loss",
                "money lost",
                "lost $",
                "lost ₹",
                "SEC",
                "SEBI",
                "FINRA",
                "regulatory",
                "illegal",
                "guarantee",
                "promise",
                "certain returns",
                "lawyer",
                "legal action",
                "lawsuit",
            ],
            "competitor_names": [],
            "auto_respond_tiers": ["positive", "neutral_faq", "neutral_technical"],
            "review_window_seconds": {"negative_mild": 7200},
            "escalate_tiers": ["negative_severe", "legal_flag"],
            "ignore_tiers": ["spam"],
            "min_confidence_threshold": 0.7,
        },
        "platforms": {
            "youtube": {
                "enabled": True,
                "privacy_status": "public",
                "default_tags": ["shorts", "algotrading", "tradingbot", "glitchexecutor"],
                "category_id": "28",
            }
        },
        "default_hashtags": [],
        "voice_prompt_path": None,
    }
