"""Application entry point for the Telegram uploader."""
import asyncio
import logging

import httpx
from config import Settings
from integrations.copyparty_files import CopypartyFiles
from services.application_services import ApplicationServices
from services.application_composition import compose_application_flows
from services.runtime_resources import create_runtime_resources
from services.application_runtime import ApplicationRuntime, RuntimeState
from services.telegram_interactions import safe_edit
from domain.job_model import Job
from domain.subtitle_rules import normalize_subtitle_file
from services.status_format import (
    format_size,
)
from services.telegram_downloads import TelegramDownloadService
from domain.transfer_models import (
    ACTIVE_TRANSFERS,
    StallError,
    TransferError,
)
from telethon import TelegramClient, events
from telethon.tl import types as tl_types
from services.mp3_tags import fix_mp3_tags
from services.error_messages import describe_error
from services.stall_watchdog import StallWatchdog
from services.runtime_support import (
    BackgroundTaskSupervisor,
    remove_quietly,
    run_blocking,
    sleep_waiting,
)
from services.runtime_configuration import (
    DOWNLOAD_RETRIES,
    HTTP_REASONS,
    UPLOAD_RETRIES,
    build_runtime_resource_options,
)
from services.startup_checks import StartupChecks

logging.basicConfig(level=logging.INFO)
# httpx на уровне INFO пишет в лог полные URL запросов, включая ?api_key=... для Jellyfin.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("userbot-dm-uploader")


SETTINGS = Settings.from_environment()

# Контейнеры, в которых бывает и звук без картинки (трек, присланный как «.mp4»): для музыки меняем на .m4a.
# «.ts» — это и MPEG-TS, и TypeScript (Telegram даже присылает его как video/mp2t), поэтому
# видео считаем только при наличии видео-атрибутов или достаточного размера.

# GIF-анимации и стикеры (в т.ч. видеостикеры и кастомные эмодзи) — это тоже mp4/webm, но не медиатека.
JUNK_ATTR_TYPES = tuple(
    getattr(tl_types, name)
    for name in ("DocumentAttributeAnimated", "DocumentAttributeSticker", "DocumentAttributeCustomEmoji")
    if hasattr(tl_types, name)
)

PROGRESS_EDIT_INTERVAL_SEC = 4

# Зависанием считаем отсутствие прогресса дольше этого времени (держит слот в семафоре).
WATCHDOG_TICK_SEC = 5
# MKCOL: 201 — создано, 405 — папка уже есть (оба случая нас устраивают).
OK_MKCOL_STATUSES = {200, 201, 204, 405}
BOT_VERSION = "2026-10-17-history-subs-dupes-links"  # пишется в лог при старте: по нему видно, какая версия реально запущена

HTTP_DEFAULT_TIMEOUT = httpx.Timeout(30.0)
UPLOAD_TIMEOUT = httpx.Timeout(connect=30.0, read=300.0, write=120.0, pool=30.0)

MAX_BATCH_LINES = 20

RUNTIME_STATE = RuntimeState()

# Сообщения, которые уже в работе (или будут возобновлены после перезапуска): защита от повторной обработки,
# например когда catch_up заново присылает сообщение, из-за которого бот упал.
ACTIVE_JOBS: set = set()
# Фоновые задачи держим по ссылке: asyncio хранит на них только слабые ссылки.
BACKGROUND_TASKS = BackgroundTaskSupervisor()
spawn = BACKGROUND_TASKS.spawn


with_stall_watchdog = StallWatchdog(SETTINGS.stall_timeout_sec, WATCHDOG_TICK_SEC, StallError)


def describe_status(code: int) -> str:
    reason = HTTP_REASONS.get(code)
    return f"сервер вернул {code}" + (f" ({reason})" if reason else "")


APPLICATION_SERVICES = ApplicationServices(
    settings=SETTINGS,
    runtime_state=RUNTIME_STATE,
    logger=log,
    safe_edit=safe_edit,
    spawn=spawn,
    remove_quietly=remove_quietly,
    request_timeout=HTTP_DEFAULT_TIMEOUT,
    run_blocking=run_blocking,
    sleep_waiting=sleep_waiting,
    fix_mp3_tags=fix_mp3_tags,
    normalize_subtitle_file=normalize_subtitle_file,
    ok_mkcol_statuses=OK_MKCOL_STATUSES,
    progress_edit_interval_sec=PROGRESS_EDIT_INTERVAL_SEC,
    upload_retries=UPLOAD_RETRIES,
    download_retries=DOWNLOAD_RETRIES,
    max_batch_lines=MAX_BATCH_LINES,
)
JOB_JOURNAL = APPLICATION_SERVICES.job_journal
TELEGRAM_INTERACTIONS = APPLICATION_SERVICES.telegram_interactions
COPYPARTY = APPLICATION_SERVICES.copyparty


# ======================================================================
# Новое: история загрузок и /undo · субтитры · проверка дубликатов · ссылки (yt-dlp)
#
#   HISTORY_DB          — файл истории (SQLite). По умолчанию TMP_DIR/state/history.sqlite3;
#                         примонтируйте TMP_DIR как volume, чтобы история переживала пересоздание контейнера
#   DUPLICATE_CHECK     — спрашивать, если такой трек/фильм/серия уже есть (1/0, по умолчанию 1)
#   LINK_INGEST         — принимать ссылки (YouTube, YouTube Music, SoundCloud, Bandcamp …) через yt-dlp (1/0, по умолчанию 1)
#   YTDLP_BIN           — команда yt-dlp (по умолчанию yt-dlp)
#   YTDLP_COOKIES       — путь к cookies.txt (нужен для части видео YouTube: «Sign in to confirm you're not a bot»)
#   YTDLP_EXTRA_ARGS    — дополнительные аргументы yt-dlp одной строкой (например, --js-runtimes node)
#   YTDLP_TIMEOUT_SEC   — потолок на одно скачивание по ссылке (по умолчанию 1800)
#   LINK_MAX_ENTRIES    — сколько элементов плейлиста/альбома брать максимум (по умолчанию 100)
#   LINK_VIDEO_MAX_HEIGHT — потолок разрешения для видео по ссылке (по умолчанию 1080)
#   LINK_EMBED_THUMB    — вшивать обложку в m4a (1/0, по умолчанию 1)
#   SUBTITLE_RECODE     — приводить текстовые субтитры к UTF-8 (1/0, по умолчанию 1)
#   SUBTITLE_FALLBACK_ENCODING — кодировка «не UTF-8» субтитров (по умолчанию cp1251)
# ======================================================================
# ---------- история загрузок (SQLite) ----------
# ---------- проверка дубликатов ----------
# ---------- субтитры ----------


TRANSFER_SERVICE = APPLICATION_SERVICES.media_transfer_pipeline.transfer


APPLICATION_FLOWS = compose_application_flows(
    application_services=APPLICATION_SERVICES,
    runtime_state=RUNTIME_STATE,
    active_jobs=ACTIVE_JOBS,
    spawn=spawn,
    junk_attribute_types=JUNK_ATTR_TYPES,
    job_type=Job,
    describe_error=describe_error,
    safe_edit=safe_edit,
    run_blocking=run_blocking,
    logger=log,
)
JOB_RUNTIME = APPLICATION_FLOWS.job_runtime
APPLICATION_PIPELINE = APPLICATION_FLOWS.pipeline
JOB_RECOVERY = JOB_RUNTIME.recovery
HISTORY_COMMANDS = APPLICATION_PIPELINE.history_commands
LIBRARY_COMMANDS = APPLICATION_PIPELINE.command_handlers
INCOMING_PIPELINE = APPLICATION_PIPELINE.incoming
handle_library_command = INCOMING_PIPELINE.handle_library_command
handle_dm = INCOMING_PIPELINE.handle_dm
process_message = INCOMING_PIPELINE.process


STARTUP_CHECKS = StartupChecks(settings=SETTINGS, logger=log)


RUNTIME_RESOURCE_OPTIONS = build_runtime_resource_options(
    settings=SETTINGS,
    event_handler=handle_dm,
    telegram_client_type=TelegramClient,
    event_type=events.NewMessage,
    download_service_type=TelegramDownloadService,
    httpx_module=httpx,
    http_timeout=HTTP_DEFAULT_TIMEOUT,
    upload_timeout=UPLOAD_TIMEOUT,
    copyparty_files_type=CopypartyFiles,
    job_journal=JOB_JOURNAL,
    stall_watchdog=with_stall_watchdog,
    remove_quietly=remove_quietly,
    format_size=format_size,
    run_blocking=run_blocking,
    sleep_waiting=sleep_waiting,
    logger=log,
)

APPLICATION = ApplicationRuntime(
    resource_factory=create_runtime_resources,
    resource_options=RUNTIME_RESOURCE_OPTIONS,
    transfer_service=TRANSFER_SERVICE,
    copyparty_gateway=COPYPARTY,
    job_recovery=JOB_RECOVERY,
    startup_warnings=STARTUP_CHECKS.run,
    telegram_interactions=TELEGRAM_INTERACTIONS,
    allowed_senders=SETTINGS.allowed_sender_ids,
    version=BOT_VERSION,
    logger=log,
    state=RUNTIME_STATE,
)



async def main():
    await APPLICATION.run()


if __name__ == "__main__":
    asyncio.run(main())
