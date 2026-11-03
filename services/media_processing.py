"""ffmpeg/ffprobe helpers for local media conversion."""

import asyncio
import contextlib
import logging
import re
from pathlib import Path

from domain.transfer_models import TransferError


log = logging.getLogger("userbot-dm-uploader")
_ARTIST_TITLE_RE = re.compile(r"^(.+?)\s+-\s+(.+)$")
_MOVIE_MARKS_RE = re.compile(
    r"\b(\d{3,4}[pi]|WEB-?DL|WEB-?Rip|BD-?Rip|HD-?Rip|DVD-?Rip|Blu-?Ray|HDTV|[xh]\.?26[45])\b",
    re.IGNORECASE,
)
_EPISODE_IN_NAME_RE = re.compile(
    r"(?<![A-Za-z0-9])S\d{1,2}E\d{1,3}(?![0-9])|(?<![A-Za-z0-9])\d{1,2}x\d{2,3}(?![A-Za-z0-9])",
    re.IGNORECASE,
)


class MediaProcessor:
    def __init__(
        self, *, ffmpeg_bin: str, ffprobe_bin: str, timeout_sec: int,
        music_video_max_minutes: float, unknown_duration_max_bytes: int,
        remove_quietly,
    ):
        self.ffmpeg_bin = ffmpeg_bin
        self.ffprobe_bin = ffprobe_bin
        self.timeout_sec = timeout_sec
        self.music_video_max_minutes = music_video_max_minutes
        self.unknown_duration_max_bytes = unknown_duration_max_bytes
        self.remove_quietly = remove_quietly

    def looks_like_music_video(self, stem: str, duration: float, size: int) -> bool:
        """Suggest music for a short artist-title video unless it looks like a movie or episode."""
        if self.music_video_max_minutes <= 0 or not _ARTIST_TITLE_RE.match(stem):
            return False
        if _MOVIE_MARKS_RE.search(stem) or _EPISODE_IN_NAME_RE.search(stem):
            return False
        if duration and duration > 0:
            return duration <= self.music_video_max_minutes * 60
        return size <= self.unknown_duration_max_bytes

    async def run_tool(self, command: list, timeout: int) -> tuple:
        try:
            proc = await asyncio.create_subprocess_exec(
                *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
            )
        except FileNotFoundError:
            raise TransferError(
                f"в образе бота нет «{command[0]}» — установите ffmpeg, чтобы доставать звук из клипов"
            ) from None
        try:
            raw, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()
            raise TransferError(f"«{command[0]}» не уложился в {timeout} сек") from None
        return proc.returncode, raw.decode("utf-8", errors="replace").strip()

    async def extract_audio(self, src: Path, dst: Path, filename: str) -> None:
        """Extract AAC without re-encoding; otherwise encode audio to AAC 256 kbit/s."""
        rc, output = await self.run_tool(
            [self.ffprobe_bin, "-v", "error", "-select_streams", "a:0", "-show_entries", "stream=codec_name",
             "-of", "csv=p=0", str(src)],
            timeout=60,
        )
        codec = output.splitlines()[0].strip().lower() if rc == 0 and output else ""
        if not codec:
            raise TransferError("в этом видео нет звуковой дорожки")
        audio_args = ["-c:a", "copy"] if codec == "aac" else ["-c:a", "aac", "-b:a", "256k"]

        command = [self.ffmpeg_bin, "-nostdin", "-y", "-v", "error", "-i", str(src), "-vn", "-map", "0:a:0", *audio_args]
        match = _ARTIST_TITLE_RE.match(Path(filename).stem)
        if match:
            command += ["-metadata", f"artist={match.group(1).strip()}", "-metadata", f"title={match.group(2).strip()}"]
        command += ["-movflags", "+faststart", "-f", "ipod", str(dst)]

        rc, output = await self.run_tool(command, timeout=self.timeout_sec)
        if rc != 0 or not dst.exists() or dst.stat().st_size == 0:
            self.remove_quietly(dst)
            raise TransferError(f"не удалось достать звук (ffmpeg: {output[-200:] or 'код ' + str(rc)})")
