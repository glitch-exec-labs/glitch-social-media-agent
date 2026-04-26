"""AI image generation via fal.ai.

Primary use: LinkedIn image posts (and later Twitter/Instagram) for text brands
that want visual pairing. Generates a PNG from a prompt and returns the local
path; the publisher uploads via upload_post.upload_photos().

Default model is `fal-ai/flux/schnell` — fast (~1-2s) and cheap
(~$0.003/image). Swap via FAL_IMAGE_MODEL in .env. All calls go through a
tenacity retry so transient network/503s don't drop an image.

Outputs land under `{settings.video_storage_path}/images/{brand_id}/` with a
UUID filename. Re-runs never overwrite; each call produces a new file.
"""
from __future__ import annotations

import asyncio
import pathlib
import uuid
from typing import Literal

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from glitch_signal.config import settings

log = structlog.get_logger(__name__)

# fal.ai's FLUX models accept a named aspect ratio. Map our semantic names
# to what the SDK expects. These are the three we actually use for social.
_ASPECT_MAP: dict[str, str] = {
    "1:1":  "square_hd",          # 1024x1024 — LinkedIn default, safe everywhere
    "4:5":  "portrait_4_3",       # LinkedIn/IG portrait
    "16:9": "landscape_16_9",     # Twitter/YouTube thumbnail
}

AspectRatio = Literal["1:1", "4:5", "16:9"]


class ImageGenError(RuntimeError):
    """Raised when fal.ai returns no image or the download fails."""


async def generate_image(
    prompt: str,
    brand_id: str,
    aspect: AspectRatio = "1:1",
) -> pathlib.Path:
    """Generate an image via fal.ai, download it, return the local path.

    Raises ImageGenError on failure. DISPATCH_MODE=dry_run short-circuits with
    a placeholder path that doesn't exist on disk (caller should skip upload
    in dry-run mode anyway).
    """
    s = settings()
    if s.is_dry_run:
        log.info("image_gen.dry_run", brand_id=brand_id, prompt=prompt[:80])
        return pathlib.Path(f"/tmp/dry-run-image-{uuid.uuid4().hex[:8]}.png")

    if not s.fal_api_key:
        raise ImageGenError("FAL_API_KEY is not set")

    out_dir = pathlib.Path(s.video_storage_path) / "images" / brand_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{uuid.uuid4().hex}.png"

    image_url = await _generate_via_fal(prompt, aspect, model=s.fal_image_model)
    await _download(image_url, out_path)

    log.info(
        "image_gen.done",
        brand_id=brand_id,
        path=str(out_path),
        size_kb=out_path.stat().st_size // 1024,
    )
    return out_path


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    retry=retry_if_exception_type((httpx.HTTPError, asyncio.TimeoutError, ImageGenError)),
)
async def _generate_via_fal(prompt: str, aspect: AspectRatio, model: str) -> str:
    """Call fal.ai, return the image URL. Retries on network/ImageGenError."""
    # Run the sync fal-client call off the event loop so the graph stays
    # responsive even if fal is slow.
    # fal-client reads FAL_KEY from env; settings loader sets the env var at
    # startup via pydantic_settings, but to be safe we also set it here.
    import os

    import fal_client
    if settings().fal_api_key and not os.environ.get("FAL_KEY"):
        os.environ["FAL_KEY"] = settings().fal_api_key

    image_size = _ASPECT_MAP.get(aspect, "square_hd")

    def _run() -> dict:
        return fal_client.run(
            model,
            arguments={
                "prompt": prompt,
                "image_size": image_size,
                "num_images": 1,
            },
        )

    result = await asyncio.to_thread(_run)
    images = result.get("images") or []
    if not images:
        raise ImageGenError(f"fal.ai returned no images for model={model}")

    url = images[0].get("url") if isinstance(images[0], dict) else None
    if not url:
        raise ImageGenError(f"fal.ai image had no URL: {images[0]!r}")
    return url


async def _download(url: str, out_path: pathlib.Path) -> None:
    """Stream-download an image URL to disk."""
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        out_path.write_bytes(resp.content)


# ---------------------------------------------------------------------------
# Designed images via OpenAI gpt-image-2 (via fal.ai)
#
# Use for posts that need TEXT RENDERED INSIDE the image — quote cards,
# stat reveals, carousel slides. gpt-image-2 nails short-to-medium text
# first-try with strong design composition; FLUX-schnell is better for
# abstract backgrounds where Pillow overlays the text.
#
# Pricing (fal.ai, Apr 2026): low ≈ $0.01, medium ≈ $0.04, high ≈ $0.17.
# ---------------------------------------------------------------------------

# fal.ai's gpt-image-2 aspect ratios use named enums, not pixel dimensions.
_GPT_IMAGE_2_ASPECT: dict[str, str] = {
    "1:1":  "square_hd",
    "4:5":  "portrait_4_3",      # closest to 4:5 offered
    "4:3":  "portrait_4_3",
    "16:9": "landscape_16_9",
}

DesignQuality = Literal["low", "medium", "high"]


async def generate_designed_image(
    prompt: str,
    brand_id: str,
    *,
    aspect: AspectRatio = "1:1",
    quality: DesignQuality = "medium",
    model: str = "openai/gpt-image-2",
) -> pathlib.Path:
    """Generate a fully designed image (text-inside-image) via gpt-image-2.

    Unlike generate_image() which produces bare backgrounds for Pillow
    overlay, this function returns an image with the text already rendered
    in the composition. Use it for quote cards, carousel slides, and any
    single-image post where the typography IS the design.

    Quality tiers roughly (fal.ai):
        low    — $0.01/image — ok for drafts
        medium — $0.04/image — default, ships fine
        high   — $0.17/image — hero posts that need to land on first scroll

    Raises ImageGenError on failure. dry_run returns a placeholder path.
    """
    s = settings()
    if s.is_dry_run:
        log.info(
            "image_gen.designed.dry_run",
            brand_id=brand_id, prompt=prompt[:80], quality=quality,
        )
        return pathlib.Path(f"/tmp/dry-run-designed-{uuid.uuid4().hex[:8]}.png")

    if not s.fal_api_key:
        raise ImageGenError("FAL_API_KEY is not set")

    out_dir = pathlib.Path(s.video_storage_path) / "images" / brand_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{uuid.uuid4().hex}.png"

    image_url = await _generate_via_gpt_image_2(
        prompt=prompt, aspect=aspect, quality=quality, model=model,
    )
    await _download(image_url, out_path)

    log.info(
        "image_gen.designed.done",
        brand_id=brand_id, model=model, quality=quality,
        path=str(out_path), size_kb=out_path.stat().st_size // 1024,
    )
    return out_path


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((httpx.HTTPError, asyncio.TimeoutError, ImageGenError)),
)
async def _generate_via_gpt_image_2(
    *, prompt: str, aspect: AspectRatio, quality: DesignQuality, model: str,
) -> str:
    """fal.ai call for gpt-image-2. gpt-image-2 takes longer than FLUX
    (thinking phase), so we use a generous timeout inside fal_client.run."""
    import os

    import fal_client

    if settings().fal_api_key and not os.environ.get("FAL_KEY"):
        os.environ["FAL_KEY"] = settings().fal_api_key

    # Prefer explicit pixel dimensions over named presets — the named
    # `portrait_4_3` preset on fal returns 768x1024, which downscales blurry
    # text on LinkedIn's 1080+ render. Asking for full-HD portrait gives the
    # model more pixels to render type into.
    px_map: dict[str, dict[str, int]] = {
        "1:1":  {"width": 1024, "height": 1024},
        "4:5":  {"width": 1080, "height": 1350},   # LinkedIn carousel native
        "4:3":  {"width": 1080, "height": 1440},
        "16:9": {"width": 1280, "height": 720},
    }
    image_size: dict | str = px_map.get(aspect) or _GPT_IMAGE_2_ASPECT.get(aspect, "square_hd")

    def _run() -> dict:
        return fal_client.run(
            model,
            arguments={
                "prompt": prompt,
                "image_size": image_size,
                "quality": quality,
                "num_images": 1,
            },
        )

    result = await asyncio.to_thread(_run)
    images = result.get("images") or []
    if not images:
        raise ImageGenError(f"gpt-image-2 returned no images for prompt={prompt[:80]!r}")
    url = images[0].get("url") if isinstance(images[0], dict) else None
    if not url:
        raise ImageGenError(f"gpt-image-2 image had no URL: {images[0]!r}")
    return url
