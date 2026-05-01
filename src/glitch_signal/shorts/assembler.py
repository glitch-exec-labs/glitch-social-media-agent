"""ffmpeg compositor: stills + Ken Burns + ElevenLabs voiceover → 1080x1920 mp4.

Two-pass approach:
  1. Pad each still to 1080x1920 (scale-fit + black bars where needed).
  2. Build per-segment Ken Burns (slow zoom) clips, lengths derived from
     the audio duration evenly distributed.
  3. Concat the clips, mux with the voiceover, output one mp4.

Caption burn-in is optional (off by default) — the spoken voiceover
carries the message, and burning text on top of already-text-heavy
gpt-image-2 stills tends to look noisy. Re-enable via burn_captions=True.
"""
from __future__ import annotations

import asyncio
import json
import pathlib
import shlex
import subprocess
import uuid

import structlog

from glitch_signal.config import settings

log = structlog.get_logger(__name__)

W, H = 1080, 1920


async def assemble(
    *,
    brand_id: str,
    script: dict,
    frame_paths: list[pathlib.Path],
    voice_path: pathlib.Path,
    out_path: pathlib.Path | None = None,
    burn_captions: bool = False,
) -> pathlib.Path:
    """Compose one 1080x1920 mp4 from frames + voiceover. Returns mp4 path."""
    s = settings()
    out_dir = pathlib.Path(s.video_storage_path) / "shorts" / brand_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_path or (out_dir / f"{uuid.uuid4().hex}.mp4")

    if len(frame_paths) < 2:
        raise RuntimeError(f"need ≥2 frames, got {len(frame_paths)}")

    audio_seconds = await _probe_duration(voice_path)
    if audio_seconds <= 0:
        raise RuntimeError(f"voice file has no duration: {voice_path}")

    # Distribute total audio time across frames evenly. The hook + cta
    # frames (first + last) get a slight bonus so they breathe.
    n = len(frame_paths)
    base = audio_seconds / n
    durations = [base] * n
    bonus = min(0.6, base * 0.3)
    durations[0] += bonus
    durations[-1] += bonus
    # Renormalize so total still equals audio_seconds
    total = sum(durations)
    durations = [d * audio_seconds / total for d in durations]

    log.info(
        "shorts.assemble.plan",
        brand_id=brand_id,
        frames=n,
        audio_seconds=round(audio_seconds, 2),
        per_frame=[round(d, 2) for d in durations],
    )

    # Build filtergraph for Ken Burns on each still + concat
    filter_parts: list[str] = []
    inputs: list[str] = []
    for i, (path, dur) in enumerate(zip(frame_paths, durations, strict=True)):
        inputs += ["-loop", "1", "-t", f"{dur:.3f}", "-i", str(path)]
        # zoompan creates the Ken Burns; scale to 1080x1920 with pad
        zoom_steps = max(1, int(dur * 25))  # 25 fps inside the zoompan
        # Alternate zoom direction per frame so it doesn't feel mechanical
        zoom_in = (i % 2 == 0)
        zexpr = "zoom+0.0008" if zoom_in else "if(eq(on,1),1.06,zoom-0.0008)"
        filter_parts.append(
            f"[{i}:v]scale=2160:-1,zoompan=z='{zexpr}'"
            f":d={zoom_steps}:s={W}x{H}:fps=30,"
            f"setpts=PTS-STARTPTS,format=yuv420p[v{i}]"
        )
    concat = "".join(f"[v{i}]" for i in range(n)) + f"concat=n={n}:v=1:a=0[vout]"
    filter_parts.append(concat)

    # Audio is the voiceover, attached as one input after all frames
    inputs += ["-i", str(voice_path)]
    audio_idx = n  # index of audio input

    cmd = [
        "ffmpeg", "-y",
        *inputs,
        "-filter_complex", ";".join(filter_parts),
        "-map", "[vout]",
        "-map", f"{audio_idx}:a:0",
        "-c:v", "libx264", "-preset", "medium", "-crf", "20",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        "-shortest",
        str(out_path),
    ]

    log.info("shorts.assemble.ffmpeg.exec", cmd_preview=" ".join(shlex.quote(x) for x in cmd[:6]))
    await _run(cmd)
    log.info(
        "shorts.assemble.done",
        path=str(out_path),
        size_kb=out_path.stat().st_size // 1024,
        seconds=round(audio_seconds, 2),
    )
    return out_path


# ---------------------------------------------------------------------------
# ffmpeg helpers
# ---------------------------------------------------------------------------

async def _probe_duration(path: pathlib.Path) -> float:
    """ffprobe a media file's duration in seconds."""
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "json",
        str(path),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        return 0.0
    try:
        data = json.loads(stdout.decode())
        return float(data["format"]["duration"])
    except (ValueError, KeyError):
        return 0.0


async def _run(cmd: list[str]) -> None:
    """Run ffmpeg and surface stderr on failure."""
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        # Last 1500 chars of stderr is plenty to see the real error
        tail = (stderr.decode(errors="replace") or "")[-1500:]
        raise subprocess.CalledProcessError(
            proc.returncode, cmd, output=None, stderr=tail,
        )
