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
            brand_id="drive_brand",
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
                brand_id="drive_brand",
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
                brand_id="drive_brand",
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


class TestNoTitleForShortFormPlatforms:
    """TikTok / IG / X etc. don't have a title concept — passing one leaks
    into the caption body. _publish_sync should skip the title kwarg for
    these and only pass it to YouTube / Pinterest / LinkedIn."""

    def test_tiktok_call_omits_title(self, monkeypatch):
        from glitch_signal.platforms import upload_post as up

        captured: dict = {}

        class _C:
            def upload_video(self, **kwargs):
                captured.update(kwargs)
                return {"success": True, "request_id": "r"}
            def get_status(self, request_id):
                return {"status": "completed", "results": [{"platform": "tiktok", "post_url": None, "platform_post_id": None}]}

        import sys
        monkeypatch.setitem(
            sys.modules,
            "upload_post",
            type("M", (), {"UploadPostClient": lambda api_key=None: _C()}),
        )

        up._publish_sync(
            api_key="k", user="MyBrand", target_platform="tiktok",
            video_url="https://x/media/fetch?token=t",
            caption="Body text here #foo", title="Would leak into caption",
            extras={}, poll_timeout_s=5,
        )
        assert "title" not in captured, f"title should NOT be in tiktok upload kwargs, got {list(captured)}"
        assert captured.get("description") == "Body text here #foo"

    def test_youtube_call_includes_title(self, monkeypatch):
        from glitch_signal.platforms import upload_post as up

        captured: dict = {}

        class _C:
            def upload_video(self, **kwargs):
                captured.update(kwargs)
                return {"success": True, "request_id": "r"}
            def get_status(self, request_id):
                return {"status": "completed", "results": [{"platform": "youtube", "post_url": "https://youtu.be/abc", "platform_post_id": "abc"}]}

        import sys
        monkeypatch.setitem(
            sys.modules,
            "upload_post",
            type("M", (), {"UploadPostClient": lambda api_key=None: _C()}),
        )

        up._publish_sync(
            api_key="k", user="MyBrand", target_platform="youtube",
            video_url="https://x/media/fetch?token=t",
            caption="Body text", title="Real YouTube Title",
            extras={}, poll_timeout_s=5,
        )
        assert captured.get("title") == "Real YouTube Title"


class TestPollUntilDone:
    def test_finds_post_url_from_results_list(self):
        from glitch_signal.platforms.upload_post import _poll_until_done

        class _C:
            def get_status(self, request_id):
                return {"status": "completed", "results": [
                    {"platform": "tiktok", "success": True,
                     "platform_post_id": "762976", "post_url": "https://tt/x/video/1"}
                ]}

        ppid, url = _poll_until_done(_C(), "req-123", "tiktok", timeout_s=10)
        assert ppid == "762976"
        assert url == "https://tt/x/video/1"

    def test_timeout_returns_none(self, monkeypatch):
        from glitch_signal.platforms import upload_post as up

        class _C:
            def get_status(self, request_id):
                return {"status": "processing", "results": [{"platform": "tiktok"}]}

        import time
        monkeypatch.setattr(time, "sleep", lambda *_: None)

        ppid, url = up._poll_until_done(_C(), "req", "tiktok", timeout_s=1)
        assert ppid is None and url is None

    def test_platform_error_raises(self):
        from glitch_signal.platforms.upload_post import _poll_until_done

        class _C:
            def get_status(self, request_id):
                return {"status": "completed", "results": [
                    {"platform": "tiktok", "success": False,
                     "error_message": "video rejected by TikTok"}
                ]}

        with pytest.raises(RuntimeError, match="video rejected"):
            _poll_until_done(_C(), "req", "tiktok", timeout_s=10)


class TestResolvePublishPlatform:
    """Brand config drives which publisher wins. Upload-Post beats Zernio
    beats direct. If nothing is enabled, raise clearly."""

    def _write_brand(self, configs_dir, brand_id: str, platforms: dict):
        import json
        (configs_dir / f"{brand_id}.json").write_text(json.dumps({
            "brand_id": brand_id,
            "display_name": brand_id,
            "timezone": "UTC",
            "platforms": platforms,
        }))

    def test_upload_post_beats_zernio(self, tmp_path, monkeypatch):
        configs = tmp_path / "configs"
        configs.mkdir()
        self._write_brand(configs, "drive_brand", {
            "upload_post_tiktok": {"enabled": True, "user": "MyBrand"},
            "zernio_tiktok":      {"enabled": True, "account_id": "z1"},
            "tiktok":             {"enabled": True},
        })
        monkeypatch.setenv("BRAND_CONFIGS_DIR", str(configs))
        monkeypatch.setenv("DEFAULT_BRAND_ID", "drive_brand")

        from glitch_signal import config as cfg
        cfg.settings.cache_clear()
        cfg._reset_brand_registry_for_tests()

        assert cfg.resolve_publish_platform("drive_brand", "tiktok") == "upload_post_tiktok"

    def test_falls_back_to_zernio(self, tmp_path, monkeypatch):
        configs = tmp_path / "configs"
        configs.mkdir()
        self._write_brand(configs, "drive_brand", {
            "upload_post_tiktok": {"enabled": False},
            "zernio_tiktok":      {"enabled": True, "account_id": "z1"},
            "tiktok":             {"enabled": True},
        })
        monkeypatch.setenv("BRAND_CONFIGS_DIR", str(configs))
        monkeypatch.setenv("DEFAULT_BRAND_ID", "drive_brand")

        from glitch_signal import config as cfg
        cfg.settings.cache_clear()
        cfg._reset_brand_registry_for_tests()

        assert cfg.resolve_publish_platform("drive_brand", "tiktok") == "zernio_tiktok"

    def test_falls_back_to_direct(self, tmp_path, monkeypatch):
        configs = tmp_path / "configs"
        configs.mkdir()
        self._write_brand(configs, "drive_brand", {
            "upload_post_tiktok": {"enabled": False},
            "zernio_tiktok":      {"enabled": False},
            "tiktok":             {"enabled": True},
        })
        monkeypatch.setenv("BRAND_CONFIGS_DIR", str(configs))
        monkeypatch.setenv("DEFAULT_BRAND_ID", "drive_brand")

        from glitch_signal import config as cfg
        cfg.settings.cache_clear()
        cfg._reset_brand_registry_for_tests()

        assert cfg.resolve_publish_platform("drive_brand", "tiktok") == "tiktok"

    def test_raises_when_nothing_enabled(self, tmp_path, monkeypatch):
        configs = tmp_path / "configs"
        configs.mkdir()
        self._write_brand(configs, "drive_brand", {
            "tiktok":             {"enabled": False},
            "upload_post_tiktok": {"enabled": False},
        })
        monkeypatch.setenv("BRAND_CONFIGS_DIR", str(configs))
        monkeypatch.setenv("DEFAULT_BRAND_ID", "drive_brand")

        from glitch_signal import config as cfg
        cfg.settings.cache_clear()
        cfg._reset_brand_registry_for_tests()

        with pytest.raises(RuntimeError, match="no enabled publisher"):
            cfg.resolve_publish_platform("drive_brand", "tiktok")


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
            "upload_post_tiktok", "/x.mp4", "s1", brand_id="drive_brand"
        )
        assert post_id == "up-stub-id"
        assert url == "https://www.tiktok.com/@x/video/1"
        assert captured == {"platform": "upload_post_tiktok", "brand_id": "drive_brand"}
