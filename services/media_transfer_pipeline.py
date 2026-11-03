"""Compose media planning, duplicate checks, link downloads, and transfers."""

import logging
import re

from domain.filename_utils import normalize_text, sanitize_name, truncate_filename
from domain.link_rules import extract_urls, is_ignored_url, link_kind
from domain.naming_rules import (
    build_series_filename,
    clean_show_name,
    detect_series,
    is_generic_name,
    parse_episode_from_name,
    series_key,
)
from domain.subtitle_rules import pick_episode_video, pick_movie_video, split_sub_name
from integrations.beets_client import LibraryError
from integrations.library import item_line
from services.duplicate_check import DuplicateCheckService
from services.link_download import LinkDownloadService
from services.link_ingestion import LinkIngestionService
from services.media_planning import MediaPlanningService
from services.status_format import format_size, short_name
from services.transfer_service import TransferService


class MediaTransferPipeline:
    def __init__(
        self, *, settings, targets, caption_naming, kind_labels, kind_aliases,
        video_kind_aliases, copyparty, library_runtime,
        telegram_interactions, ytdlp_client, transfer_support, run_blocking,
        fix_mp3_tags, normalize_subtitle_file, remove_quietly, sleep_waiting,
        job_journal, retry_backoff_sec, subtitle_max_bytes,
        logger: logging.Logger,
    ):
        self.settings = settings
        self.job_journal = job_journal
        self.ytdlp_client = ytdlp_client
        self.telegram_interactions = telegram_interactions
        self.duplicate_check = DuplicateCheckService(
            targets=targets,
            copyparty=copyparty,
            beets_ready=library_runtime.beets_ready,
            find_items=library_runtime.find_items,
            library_error=LibraryError,
            artist_title_pattern=re.compile(r"^(.+?)\s+-\s+(.+)$"),
            series_key=series_key,
            item_line=item_line,
            pick_episode_video=pick_episode_video,
            short_name=short_name,
            format_size=format_size,
            logger=logger,
        )
        self.link_download = LinkDownloadService(
            ytdlp=ytdlp_client,
            tmp_dir=settings.tmp_dir,
            embed_thumbnail=settings.link_embed_thumb,
            max_video_height=settings.link_video_max_height,
            retry_backoff_sec=retry_backoff_sec,
            sleep_waiting=sleep_waiting,
            duplicate_check=settings.duplicate_check,
            find_duplicate=self.duplicate_check.find_duplicate,
            truncate_filename=truncate_filename,
            run_blocking=run_blocking,
        )
        self.media_planner = MediaPlanningService(
            caption_naming=caption_naming,
            kind_labels=kind_labels,
            kind_aliases=kind_aliases,
            video_kind_aliases=video_kind_aliases,
            ask_choice=telegram_interactions.ask_choice,
            ask_text=telegram_interactions.ask_text,
            detect_series=detect_series,
            is_generic_name=is_generic_name,
            parse_episode_from_name=parse_episode_from_name,
            normalize_text=normalize_text,
            sanitize_name=sanitize_name,
            truncate_filename=truncate_filename,
            clean_show_name=clean_show_name,
            build_series_filename=build_series_filename,
            split_sub_name=split_sub_name,
            pick_episode_video=pick_episode_video,
            pick_movie_video=pick_movie_video,
            copyparty=copyparty,
            short_name=short_name,
        )
        self.transfer = TransferService(
            targets=targets,
            download_from_link=self.link_download.download,
            ensure_disk_space=transfer_support.ensure_disk_space,
            extract_audio=transfer_support.media_processor.extract_audio,
            fix_mp3_tags=fix_mp3_tags,
            run_blocking=run_blocking,
            normalize_subtitle_file=normalize_subtitle_file,
            subtitle_recode=settings.subtitle_recode,
            subtitle_max_bytes=subtitle_max_bytes,
            subtitle_fallback_encoding=settings.subtitle_fallback_encoding,
            resolve_subtitle_target=self.media_planner.resolve_subtitle_target,
            resolve_series_dirs=copyparty.resolve_series_dirs,
            upload_with_retry=copyparty.upload_with_retry,
            remove_quietly=remove_quietly,
        )

    def is_link_message(self, event) -> bool:
        if not self.settings.link_ingest or event.is_reply:
            return False
        text = event.raw_text or ""
        if not text or text.startswith("/"):
            return False
        return any(not is_ignored_url(url) for url in extract_urls(text))

    def create_link_ingestion(self, *, run_job, describe_error, safe_edit, kind_aliases, yes_aliases):
        self.link_ingestion = LinkIngestionService(
            probe_link=self.ytdlp_client.probe_link,
            max_entries=self.settings.link_max_entries,
            link_kind=link_kind,
            default_kind=self.settings.link_default_kind,
            ask_choice=self.telegram_interactions.ask_choice,
            kind_aliases=kind_aliases,
            link_kind_note=self._link_kind_note,
            yes_aliases=yes_aliases,
            duplicate_note=self.duplicate_check.duplicate_note,
            music_meta_from_info=self.ytdlp_client.music_meta_from_info,
            plan_video=self.media_planner.plan_video,
            journal_write=self.job_journal.write,
            run_job=run_job,
            describe_error=describe_error,
            safe_edit=safe_edit,
        )
        return self.link_ingestion

    @staticmethod
    def _link_kind_note(choice):
        if choice == "music":
            return "✅ Скачиваю аудио."
        if choice == "movie":
            return "✅ Скачиваю видео."
        if choice is None:
            return "⌛ Ответа не дождался — ссылка пропущена."
        return "✖️ Отменено."
