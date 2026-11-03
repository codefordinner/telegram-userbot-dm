"""Construct long-lived application services from settings and adapters."""

import asyncio
import shutil

from domain.choice_rules import KIND_ALIASES, KIND_LABELS, VIDEO_KIND_ALIASES, YES_ALIASES
from domain.filename_utils import normalize_text
from domain.transfer_models import ACTIVE_TRANSFERS, TransferError
from integrations.copyparty_directories import CopypartyDirectories
from integrations.copyparty_gateway import CopypartyGateway
from integrations.copyparty_files import CopypartyFiles
from integrations.media_servers import refresh_jellyfin, scan_navidrome
from integrations.ytdlp_client import YtDlpClient
from services.library_runtime import LibraryRuntime
from services.media_transfer_pipeline import MediaTransferPipeline
from services.job_runtime import JobRuntime
from services.status_format import format_size
from services.telegram_interactions import TelegramInteractions
from services.transfer_support import TransferSupport
from storage.history_store import HistoryStore
from storage.job_journal import JobJournal


class ApplicationServices:
    """Own configured service instances that are shared across application flows."""

    def __init__(
        self, *, settings, runtime_state, logger, safe_edit, spawn, remove_quietly,
        request_timeout, ok_mkcol_statuses,
        run_blocking, sleep_waiting, fix_mp3_tags, normalize_subtitle_file,
        progress_edit_interval_sec=4, upload_retries=3,
        download_retries=3, max_batch_lines=20,
    ):
        self.settings = settings
        self.job_journal = JobJournal(
            settings.tmp_dir,
            settings.tmp_dir / "jobs",
            settings.resume_max_age_sec,
            3,
        )
        self.telegram_interactions = TelegramInteractions(
            choice_timeout=settings.choice_timeout_sec,
            allowed_senders=settings.allowed_sender_ids,
            session_path=settings.session_path,
            api_id=settings.api_id,
            api_hash=settings.api_hash,
            helper_token=settings.buttons_bot_token,
            safe_edit=safe_edit,
        )

        self.copyparty = CopypartyGateway(
            directories_type=CopypartyDirectories,
            series_lock=asyncio.Lock(),
            remote_url=CopypartyFiles.remote_url,
            ok_mkcol_statuses=ok_mkcol_statuses,
            request_timeout=request_timeout,
        )
        self.transfer_support = TransferSupport(
            tmp_dir=settings.tmp_dir,
            check_disk=settings.check_disk,
            min_free_disk_after_bytes=settings.min_free_disk_after_bytes,
            format_size=format_size,
            transfer_error=TransferError,
            active_transfers=ACTIVE_TRANSFERS,
            disk_usage=shutil.disk_usage,
            logger=logger,
            spawn=spawn,
            refresh_jellyfin=lambda: refresh_jellyfin(
                runtime_state.http_client, settings.jellyfin_url, settings.jellyfin_api_key
            ),
            safe_edit=safe_edit,
            batch_grace_sec=settings.batch_grace_sec,
            progress_edit_interval_sec=progress_edit_interval_sec,
            max_batch_lines=max_batch_lines,
            upload_retries=upload_retries,
            download_retries=download_retries,
            ffmpeg_bin=settings.ffmpeg_bin,
            ffprobe_bin=settings.ffprobe_bin,
            ffmpeg_timeout_sec=settings.ffmpeg_timeout_sec,
            music_video_max_minutes=settings.music_video_max_min,
            unknown_duration_max_bytes=150 * 1024 * 1024,
            remove_quietly=remove_quietly,
        )
        self.library = LibraryRuntime(
            beets_bin=settings.beets_bin,
            beets_config=settings.beets_config,
            beets_plugins=settings.beets_plugins,
            beets_timeout_sec=settings.beets_timeout_sec,
            library_admin=settings.library_admin,
            normalize_text=normalize_text,
            ask_choice=self.telegram_interactions.ask_choice,
            yes_aliases=YES_ALIASES,
            navidrome_scan=lambda: scan_navidrome(
                runtime_state.http_client, settings.navidrome_url,
                settings.navidrome_user, settings.navidrome_pass,
            ),
        )
        self.history_store = HistoryStore(settings.history_db)
        self.ytdlp_client = YtDlpClient(
            settings.ytdlp_bin,
            settings.ytdlp_cookies,
            settings.ytdlp_extra_args,
            settings.ytdlp_timeout_sec,
        )
        self.media_transfer_pipeline = MediaTransferPipeline(
            settings=settings,
            targets=settings.targets,
            caption_naming=settings.caption_naming,
            kind_labels=KIND_LABELS,
            kind_aliases=KIND_ALIASES,
            video_kind_aliases=VIDEO_KIND_ALIASES,
            copyparty=self.copyparty,
            library_runtime=self.library,
            telegram_interactions=self.telegram_interactions,
            ytdlp_client=self.ytdlp_client,
            transfer_support=self.transfer_support,
            run_blocking=run_blocking,
            sleep_waiting=sleep_waiting,
            fix_mp3_tags=fix_mp3_tags,
            normalize_subtitle_file=normalize_subtitle_file,
            remove_quietly=remove_quietly,
            job_journal=self.job_journal,
            retry_backoff_sec=5,
            subtitle_max_bytes=20 * 1024 * 1024,
            logger=logger,
        )

    def create_job_runtime(
        self, *, temp_dir, job_type, resume_on_start, active_jobs,
        upload_semaphore, run_blocking, describe_error, spawn,
        get_client, logger,
    ):
        """Compose job execution and restart recovery from configured services."""
        return JobRuntime(
            transfer_service=self.media_transfer_pipeline.transfer,
            history_store=self.history_store,
            job_journal=self.job_journal,
            temp_dir=temp_dir,
            job_type=job_type,
            resume_on_start=resume_on_start,
            active_jobs=active_jobs,
            upload_semaphore=upload_semaphore,
            join_batch=self.transfer_support.join_batch,
            run_blocking=run_blocking,
            describe_error=describe_error,
            spawn=spawn,
            get_client=get_client,
            logger=logger,
        )

