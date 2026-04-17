"""Upload-Post publisher — dry-run, platform-key routing, extras mapping."""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("DISPATCH_MODE", "dry_run")
os.environ.setdefault("SIGNAL_DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("TELEGRAM_BOT_TOKEN_SIGNAL", "0:test")
os.environ.setdefault("TELEGRAM_ADMIN_IDS", "0")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")
os.environ.setdefault("GOOGLE_API_KEY", "test")
os.environ.setdefault("AUTH_ENCRYPTION_KEY", "l3mgT3MDKZ2g8oh2l8r4e1XaS0o7Q8mT9H5V1v3P2Hk=")


@pytest.fixture(autouse=True)
def _reset_caches():
    from glitch_signal import config as cfg
    cfg._reset_brand_registry_for_tests()
    cfg.settings.cache_clear()
    yield
    cfg._reset_brand_registry_for_tests()
    cfg.settings.cache_clear()


class TestUploadPostPlatformMap:
    def test_known_platform_keys(self):
        from glitch_signal.platforms.upload_post import _PLATFORM_MAP
        assert _PLATFORM_MAP["upload_post_tiktok"] == "tiktok"
        assert _PLATFORM_MAP["upload_post_instagram"] == "instagram"
        assert _PLATFORM_MAP["upload_post_youtube"] == "youtube"


class TestUploadPostDryRun:
    @pytest.mark.asyncio
    async def test_dry_run_returns_fake_id_no_http(self):
        from glitch_signal.platforms import upload_post
        publish_id, url = await upload_post.publish(
            platform="upload_post_tiktok",
            file_path="/does/not/matter.mp4",
            script_id="s1",
            brand_id="nmahya",
        )
        assert publish_id.startswith("uploadpost-dry-")
        assert url is None

    @pytest.mark.asyncio
    async def test_live_rejects_missing_api_key(self, monkeypatch):
        from glitch_signal import config
        from glitch_signal.platforms import upload_post

        monkeypatch.setenv("DISPATCH_MODE", "live")
        monkeypatch.setenv("UPLOAD_POST_API_KEY", "")
        config.settings.cache_clear()

        with pytest.raises(RuntimeError, match="UPLOAD_POST_API_KEY"):
            await upload_post.publish(
                platform="upload_post_tiktok",
                file_path="/tmp/x.mp4",
                script_id="s",
                brand_id="nmahya",
            )

    @pytest.mark.asyncio
    async def test_live_rejects_unknown_platform_key(self, monkeypatch):
        from glitch_signal import config
        from glitch_signal.platforms import upload_post

        monkeypatch.setenv("DISPATCH_MODE", "live")
        monkeypatch.setenv("UPLOAD_POST_API_KEY", "k")
        config.settings.cache_clear()

        with pytest.raises(ValueError, match="unknown platform key"):
            await upload_post.publish(
                platform="upload_post_bogus",
                file_path="/tmp/x.mp4",
                script_id="s",
                brand_id="nmahya",
            )


class TestPlatformExtras:
    def test_tiktok_extras_full_mapping(self):
        from glitch_signal.platforms.upload_post import _platform_extras
        cfg = {
            "default_privacy_level": "PUBLIC_TO_EVERYONE",
            "disable_duet": True,
            "disable_stitch": False,
            "disable_comment": False,
            "video_cover_timestamp_ms": 2500,
            "post_mode": "DIRECT_POST",
            "is_aigc": False,
        }
        e = _platform_extras("tiktok", cfg)
        assert e["privacy_level"] == "PUBLIC_TO_EVERYONE"
        assert e["disable_duet"] is True
        assert e["cover_timestamp"] == 2500
        assert e["post_mode"] == "DIRECT_POST"
        assert e["is_aigc"] is False

    def test_instagram_extras_default_reels(self):
        from glitch_signal.platforms.upload_post import _platform_extras
        e = _platform_extras("instagram", {})
        assert e["media_type"] == "REELS"

    def test_youtube_extras_default_public(self):
        from glitch_signal.platforms.upload_post import _platform_extras
        e = _platform_extras("youtube", {})
        assert e["privacyStatus"] == "public"
        assert e["categoryId"] == "22"

    def test_unknown_target_returns_empty(self):
        from glitch_signal.platforms.upload_post import _platform_extras
        assert _platform_extras("bluesky", {"anything": 1}) == {}


class TestPublisherRoutesUploadPost:
    """publisher._publish_to_platform dispatches upload_post_* → platforms/upload_post."""

    @pytest.mark.asyncio
    async def test_routes_upload_post_tiktok(self, monkeypatch):
        from glitch_signal.agent.nodes import publisher

        captured = {}

        async def fake_publish(platform, file_path, script_id, brand_id=None):
            captured["platform"] = platform
            captured["brand_id"] = brand_id
            return "up-stub-id", "https://www.tiktok.com/@x/video/1"

        monkeypatch.setattr("glitch_signal.platforms.upload_post.publish", fake_publish)
        monkeypatch.setenv("DISPATCH_MODE", "live")
        from glitch_signal import config as cfg
        cfg.settings.cache_clear()

        post_id, url = await publisher._publish_to_platform(
            "upload_post_tiktok", "/x.mp4", "s1", brand_id="nmahya"
        )
        assert post_id == "up-stub-id"
        assert url == "https://www.tiktok.com/@x/video/1"
        assert captured == {"platform": "upload_post_tiktok", "brand_id": "nmahya"}
