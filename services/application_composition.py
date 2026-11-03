"""Compose command handlers and the Telegram message pipeline from services."""

from dataclasses import dataclass

from domain.choice_rules import KIND_ALIASES, YES_ALIASES
from domain.filename_utils import clean_filename, truncate_filename
from domain.media_types import (
    MP4_FAMILY_EXTS,
    VIDEO_EXTS,
    align_extension,
    classify,
    ensure_extension,
)
from domain.naming_rules import (
    build_filename_from_music_caption,
    build_filename_from_video_caption,
    parse_music_caption,
    parse_video_caption,
    should_use_caption,
)
from domain.subtitle_rules import normalize_subtitle_file
from integrations.beets_client import LibraryError
from integrations.library import LIB_MAX_LIST, item_line
from services.application_pipeline import ApplicationPipeline


@dataclass
class ApplicationFlows:
    job_runtime: object
    link_ingestion: object
    pipeline: ApplicationPipeline


def compose_application_flows(
    *, application_services, runtime_state, active_jobs, spawn,
    junk_attribute_types, job_type,
    describe_error, safe_edit, run_blocking, logger,
):
    """Build job recovery, link handling, commands, and incoming message flows."""
    settings = application_services.settings
    media = application_services.media_transfer_pipeline
    job_runtime = application_services.create_job_runtime(
        temp_dir=settings.tmp_dir,
        job_type=job_type,
        resume_on_start=settings.resume_on_start,
        active_jobs=active_jobs,
        upload_semaphore=lambda: runtime_state.upload_semaphore,
        run_blocking=run_blocking,
        describe_error=describe_error,
        spawn=spawn,
        get_client=lambda: runtime_state.client,
        logger=logger,
    )
    link_ingestion = media.create_link_ingestion(
        run_job=job_runtime.run,
        describe_error=describe_error,
        safe_edit=safe_edit,
        kind_aliases=KIND_ALIASES,
        yes_aliases=YES_ALIASES,
    )
    pipeline = build_application_pipeline(
        application_services=application_services,
        active_jobs=active_jobs,
        junk_attribute_types=junk_attribute_types,
        job_type=job_type,
        run_job=job_runtime.run,
        describe_error=describe_error,
        run_blocking=run_blocking,
        logger=logger,
    )
    return ApplicationFlows(job_runtime, link_ingestion, pipeline)


def build_application_pipeline(
    *, application_services, active_jobs,
    junk_attribute_types, job_type, run_job, describe_error, run_blocking,
    logger,
):
    """Wire business services into command handlers and incoming DM routing."""
    settings = application_services.settings
    media = application_services.media_transfer_pipeline
    history = application_services.history_store
    library = application_services.library
    interactions = application_services.telegram_interactions
    copyparty = application_services.copyparty
    planner = media.media_planner
    duplicate = media.duplicate_check
    media_processor = application_services.transfer_support.media_processor
    job_journal = application_services.job_journal
    targets = settings.targets

    def is_active(key: tuple) -> bool:
        return any(job_key[:2] == key for job_key in active_jobs)

    return ApplicationPipeline(
        history_options=dict(
            history_list=history.list,
            history_get=history.get,
            history_mark_undone=history.mark_undone,
            run_blocking=run_blocking,
            max_list=LIB_MAX_LIST,
            targets=targets,
            get_remote_size=copyparty.get_remote_size,
            find_library_matches=duplicate.find_library_matches,
            beets_ready=library.beets_ready,
            item_line=item_line,
            ask_choice=interactions.ask_choice,
            yes_aliases=YES_ALIASES,
            run_beet=library.run_beet,
            library_error=LibraryError,
            navidrome_scan=library.navidrome_scan,
            scan_note=library.scan_note,
            delete_remote_quietly=copyparty.delete_remote_quietly,
            prune_empty_dirs=copyparty.prune_empty_dirs,
            refresh_jellyfin=application_services.transfer_support.refresh_jellyfin,
        ),
        library_commands=library.commands,
        document_options=dict(
            junk_attribute_types=junk_attribute_types,
            job_type=job_type,
            clean_filename=clean_filename,
            classify=classify,
            ambiguous_min_bytes=settings.ambiguous_min_bytes,
            plan_subtitle=planner.plan_subtitle,
            truncate_filename=truncate_filename,
            journal_write=job_journal.write,
            run_job=run_job,
            ask_kind=planner.ask_kind,
            video_extensions=VIDEO_EXTS,
            looks_like_music_video=media_processor.looks_like_music_video,
            ask_choice=interactions.ask_choice,
            kind_aliases=KIND_ALIASES,
            ensure_extension=ensure_extension,
            align_extension=align_extension,
            mp4_family_extensions=MP4_FAMILY_EXTS,
            parse_video_caption=parse_video_caption,
            build_video_filename=build_filename_from_video_caption,
            parse_music_caption=parse_music_caption,
            build_music_filename=build_filename_from_music_caption,
            should_use_caption=should_use_caption,
            caption_naming=planner.caption_naming,
            plan_video=planner.plan_video,
            duplicate_check=settings.duplicate_check,
            find_duplicate=duplicate.find_duplicate,
            yes_aliases=YES_ALIASES,
            duplicate_note=duplicate.duplicate_note,
        ),
        router_options=dict(
            allowed_sender_ids=settings.allowed_sender_ids,
            active_jobs=active_jobs,
            is_active=is_active,
            is_link_message=media.is_link_message,
            library_admin=settings.library_admin,
            command_disabled_note="🔒 Управление библиотекой выключено (LIBRARY_ADMIN=0).",
            library_error=LibraryError,
            describe_error=describe_error,
            resolve_text_choice=interactions.resolve_text_choice,
            handle_links=media.link_ingestion.handle,
            logger=logger,
        ),
    )
