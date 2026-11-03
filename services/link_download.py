"""Download one yt-dlp job, finalize its media file, and check for duplicates."""

import contextlib
import json
import logging
from pathlib import Path

from mutagen.mp4 import MP4

from domain.transfer_models import TransferError


log = logging.getLogger("userbot-dm-uploader")


def fix_m4a_tags(path: str, artist: str, title: str, album: str) -> None:
    """Keep the embedded track tags consistent with the final filename."""
    try:
        mp4 = MP4(path)
        if mp4.tags is None:
            mp4.add_tags()
        mp4["\xa9ART"] = [artist]
        mp4["\xa9nam"] = [title]
        if album and not mp4.get("\xa9alb"):
            mp4["\xa9alb"] = [album]
        mp4.save()
    except Exception as exc:
        log.warning("Не удалось записать теги m4a %s: %s", path, exc)


class LinkDownloadService:
    def __init__(
        self, *, ytdlp, tmp_dir, embed_thumbnail, max_video_height,
        retry_backoff_sec, sleep_waiting, duplicate_check, find_duplicate,
        truncate_filename, run_blocking, tag_writer=fix_m4a_tags, logger=log,
    ):
        self.ytdlp = ytdlp
        self.tmp_dir = Path(tmp_dir)
        self.embed_thumbnail = embed_thumbnail
        self.max_video_height = max_video_height
        self.retry_backoff_sec = retry_backoff_sec
        self.sleep_waiting = sleep_waiting
        self.duplicate_check = duplicate_check
        self.find_duplicate = find_duplicate
        self.truncate_filename = truncate_filename
        self.run_blocking = run_blocking
        self.tag_writer = tag_writer
        self.log = logger

    async def download(self, job, local_path: Path, state) -> Path:
        """Download a link job and update ``job.filename`` to the final name."""
        stem = local_path.stem
        args = self.ytdlp.base_args() + [
            "--no-playlist", "--no-mtime", "--newline", "--write-info-json",
            "--progress-template",
            "download:PROG %(progress.downloaded_bytes)s %(progress.total_bytes)s %(progress.total_bytes_estimate)s",
            "-o", str(self.tmp_dir / f"{stem}.%(ext)s"),
        ]
        if job.kind == "music":
            args += ["-f", "bestaudio[ext=m4a]/bestaudio/best", "-x", "--audio-format", "m4a",
                     "--audio-quality", "0", "--embed-metadata"]
            if self.embed_thumbnail:
                args += ["--embed-thumbnail", "--convert-thumbnails", "jpg"]
        else:
            args += ["-S", f"res:{self.max_video_height},vcodec:h264,acodec:m4a", "--merge-output-format", "mp4"]
        args.append(job.url)

        for attempt in (1, 2):
            state.begin("download", attempt, current=0, total=1)
            try:
                await self.ytdlp.run_download(args, state)
                break
            except TransferError as exc:
                self.log.warning("yt-dlp, попытка %s/2: %s", attempt, exc.user_message)
                if attempt == 2 or not exc.retriable:
                    raise
                await self.sleep_waiting(state, self.retry_backoff_sec)

        wanted = (".m4a",) if job.kind == "music" else (".mp4", ".mkv", ".webm", ".mov")
        output = next(
            (self.tmp_dir / f"{stem}{ext}" for ext in wanted
             if (self.tmp_dir / f"{stem}{ext}").exists() and (self.tmp_dir / f"{stem}{ext}").stat().st_size > 0),
            None,
        )
        if output is None:
            raise TransferError("yt-dlp не создал файл")
        size = output.stat().st_size
        state.begin("download", state.attempt, current=size, total=size)

        info = {}
        with contextlib.suppress(OSError, ValueError):
            info = json.loads((self.tmp_dir / f"{stem}.info.json").read_text(encoding="utf-8"))

        if job.kind == "music":
            artist, title, album = self.ytdlp.music_meta_from_info(info, fallback=Path(job.filename).stem)
            job.filename = self.truncate_filename(f"{artist} - {title}.m4a")
            await self.run_blocking(self.tag_writer, str(output), artist, title, album)
        elif Path(job.filename).suffix.lower() != output.suffix.lower():
            job.filename = self.truncate_filename(Path(job.filename).stem + output.suffix.lower())

        if self.duplicate_check and not job.force:
            try:
                duplicate = await self.find_duplicate(job.kind, job.filename, job.series)
            except Exception as exc:
                self.log.warning("Проверка дубликатов не удалась: %s", exc)
                duplicate = None
            if duplicate:
                raise TransferError(
                    "дубликат: " + duplicate.splitlines()[0].rstrip(":.")
                    + " (добавьте --force к сообщению, чтобы загрузить всё равно)"
                )
        return output
