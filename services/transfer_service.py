"""Coordinate download, local media handling, and remote upload for one job."""

from pathlib import Path

from domain.job_model import Job
from domain.transfer_models import TransferState


class TransferService:
    def __init__(
        self, *, targets, download_from_link, ensure_disk_space,
        extract_audio, fix_mp3_tags, run_blocking,
        normalize_subtitle_file, subtitle_recode, subtitle_max_bytes,
        subtitle_fallback_encoding, resolve_subtitle_target, resolve_series_dirs,
        upload_with_retry, remove_quietly,
    ):
        self.targets = targets
        self.download_from_link = download_from_link
        self.telegram_downloads = None
        self.ensure_disk_space = ensure_disk_space
        self.extract_audio = extract_audio
        self.fix_mp3_tags = fix_mp3_tags
        self.run_blocking = run_blocking
        self.normalize_subtitle_file = normalize_subtitle_file
        self.subtitle_recode = subtitle_recode
        self.subtitle_max_bytes = subtitle_max_bytes
        self.subtitle_fallback_encoding = subtitle_fallback_encoding
        self.resolve_subtitle_target = resolve_subtitle_target
        self.resolve_series_dirs = resolve_series_dirs
        self.upload_with_retry = upload_with_retry
        self.remove_quietly = remove_quietly

    def attach_telegram_downloads(self, download_service) -> None:
        """Attach the runtime-created Telegram downloader before accepting updates."""
        self.telegram_downloads = download_service

    async def transfer_file(self, message, job: Job, local_path: Path, state: TransferState) -> tuple:
        """Download, process locally, and upload. Return existence, location, and remote name."""
        upload_path = local_path
        if job.url:
            upload_path = await self.download_from_link(job, local_path, state)
        else:
            if self.telegram_downloads is None:
                raise RuntimeError("Telegram download service has not been initialized")
            already = (
                self.telegram_downloads.read_resume_point(local_path, message.document)
                if local_path.exists() else 0
            )
            self.ensure_disk_space(max((message.document.size or 0) - already, 0), state)
            await self.telegram_downloads.download_with_retry(message, local_path, state)

        audio_path = local_path.with_suffix(".m4a")
        try:
            if job.extract:
                state.begin("convert", 1, current=0, total=1)
                await self.extract_audio(local_path, audio_path, job.filename)
                upload_path = audio_path

            if job.kind == "music" and Path(job.filename).suffix.lower() == ".mp3":
                await self.run_blocking(self.fix_mp3_tags, str(upload_path))

            url, user, pw = self.targets[job.kind]
            remote_name = job.filename
            where = ""
            if job.subtitle:
                await self.run_blocking(
                    self.normalize_subtitle_file,
                    str(upload_path),
                    self.subtitle_recode,
                    self.subtitle_max_bytes,
                    self.subtitle_fallback_encoding,
                )
                remote_name, where = await self.resolve_subtitle_target(url, user, pw, job)
            elif job.kind == "movie" and job.series:
                # Resolve just before upload, since another task may have created the folders meanwhile.
                directories = await self.resolve_series_dirs(
                    url, user, pw, job.series["show"], int(job.series["season"])
                )
                remote_name = "/".join(directories + [job.filename])
                where = "/".join(directories)
            existed = await self.upload_with_retry(url, user, pw, remote_name, upload_path, state)
            return existed, where, remote_name
        finally:
            self.remove_quietly(audio_path)
