"""Zernio publisher — dry-run, platform-key routing, config validation."""
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


class TestZernioPlatformMap:
    def test_known_platform_keys(self):
        from glitch_signal.platforms.zernio import _PLATFORM_MAP
        assert _PLATFORM_MAP["zernio_tiktok"] == "tiktok"
        assert _PLATFORM_MAP["zernio_instagram"] == "instagram"
        assert _PLATFORM_MAP["zernio_youtube"] == "youtube"


class TestZernioDryRun:
    @pytest.mark.asyncio
    async def test_dry_run_returns_fake_id_no_http(self):
        from glitch_signal.platforms import zernio
        publish_id, url = await zernio.publish(
            platform="zernio_tiktok",
            file_path="/does/not/matter.mp4",
            script_id="s1",
            brand_id="drive_brand",
        )
        assert publish_id.startswith("zernio-dry-")
        assert url is None

    @pytest.mark.asyncio
    async def test_live_rejects_missing_api_key(self, monkeypatch):
        from glitch_signal import config
        from glitch_signal.platforms import zernio

        monkeypatch.setenv("DISPATCH_MODE", "live")
        monkeypatch.setenv("ZERNIO_API_KEY", "")
        config.settings.cache_clear()

        with pytest.raises(RuntimeError, match="ZERNIO_API_KEY"):
            await zernio.publish(
                platform="zernio_tiktok",
                file_path="/tmp/x.mp4",
                script_id="s",
                brand_id="drive_brand",
            )

    @pytest.mark.asyncio
    async def test_live_rejects_unknown_platform_key(self, monkeypatch):
        from glitch_signal import config
        from glitch_signal.platforms import zernio

        monkeypatch.setenv("DISPATCH_MODE", "live")
        monkeypatch.setenv("ZERNIO_API_KEY", "k")
        config.settings.cache_clear()

        with pytest.raises(ValueError, match="unknown platform key"):
            await zernio.publish(
                platform="zernio_bogus",
                file_path="/tmp/x.mp4",
                script_id="s",
                brand_id="drive_brand",
            )


class TestPublisherRoutesZernio:
    """publisher._publish_to_platform must dispatch zernio_* → platforms/zernio."""

    @pytest.mark.asyncio
    async def test_routes_zernio_tiktok(self, monkeypatch):
        from glitch_signal.agent.nodes import publisher

        captured = {}

        async def fake_zernio_publish(platform, file_path, script_id, brand_id=None):
            captured["platform"] = platform
            captured["brand_id"] = brand_id
            return "zernio-stub-id", "https://tiktok.com/@x/video/1"

        monkeypatch.setattr("glitch_signal.platforms.zernio.publish", fake_zernio_publish)
        monkeypatch.setenv("DISPATCH_MODE", "live")
        from glitch_signal import config as cfg
        cfg.settings.cache_clear()

        post_id, url = await publisher._publish_to_platform(
            "zernio_tiktok", "/x.mp4", "s1", brand_id="drive_brand"
        )
        assert post_id == "zernio-stub-id"
        assert url == "https://tiktok.com/@x/video/1"
        assert captured == {"platform": "zernio_tiktok", "brand_id": "drive_brand"}


class TestSignedMediaUrl:
    """_build_signed_media_url produces a token verifiable by /media/fetch."""

    def test_token_round_trips(self, tmp_path):
        from glitch_signal.crypto import verify_state_token
        from glitch_signal.platforms.zernio import _build_signed_media_url

        p = tmp_path / "clip.mp4"
        p.write_bytes(b"x")
        url = _build_signed_media_url(p)
        assert "/media/fetch?token=" in url
        token = url.split("token=")[1]
        payload = verify_state_token(token)
        assert payload["k"] == "media"
        assert payload["p"] == str(p.resolve())

    def test_token_different_per_file(self, tmp_path):
        from glitch_signal.platforms.zernio import _build_signed_media_url
        a = tmp_path / "a.mp4"
        a.write_bytes(b"a")
        b = tmp_path / "b.mp4"
        b.write_bytes(b"b")
        ua = _build_signed_media_url(a).split("token=")[1]
        ub = _build_signed_media_url(b).split("token=")[1]
        assert ua != ub
