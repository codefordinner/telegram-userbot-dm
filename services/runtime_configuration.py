"""Build keyword arguments for runtime resources from application dependencies."""


UPLOAD_RETRIES = 3
DOWNLOAD_RETRIES = 3
MAX_FLOOD_WAITS = 5
RETRY_BACKOFF_SEC = 5
UPLOAD_CHUNK_SIZE = 1024 * 1024
DOWNLOAD_SEGMENT_SIZE = 4 * 1024 * 1024
MAX_CONCURRENT_TRANSFERS = 2
PERMS_TTL_SEC = 3600

OK_MOVE_STATUSES = {200, 201, 204}
MOVE_DENIED_STATUSES = {401, 403, 405, 501}
OK_UPLOAD_STATUSES = {200, 201, 204}
RETRY_UPLOAD_STATUSES = {408, 425, 429, 500, 502, 503, 504}

HTTP_REASONS = {
    400: "сервер отклонил запрос",
    401: "неверный логин или пароль copyparty",
    403: "нет прав на запись в эту папку",
    404: "папка не найдена на сервере",
    405: "сервер не разрешает загрузку по этому адресу",
    409: "конфликт имён на сервере",
    413: "файл слишком большой для сервера",
    429: "сервер просит снизить частоту запросов",
    500: "внутренняя ошибка сервера",
    502: "прокси не смог связаться с сервером",
    503: "сервер временно недоступен",
    504: "прокси не дождался ответа сервера",
    507: "на сервере закончилось место",
}


def build_runtime_resource_options(
    *, settings, event_handler, telegram_client_type, event_type,
    download_service_type, httpx_module, http_timeout,
    upload_timeout, copyparty_files_type, job_journal, stall_watchdog,
    remove_quietly, format_size, run_blocking, sleep_waiting, logger,
):
    """Return the resource factory options without creating clients or tasks."""
    download_options = {
        "request_size": settings.download_request_size,
        "workers": settings.download_workers,
        "segment_size": DOWNLOAD_SEGMENT_SIZE,
        "retries": DOWNLOAD_RETRIES,
        "max_flood_waits": MAX_FLOOD_WAITS,
        "max_flood_wait_sec": settings.max_flood_wait_sec,
        "retry_backoff_sec": RETRY_BACKOFF_SEC,
        "stall_watchdog": stall_watchdog,
        "meta_path": job_journal.meta_path,
        "remove_quietly": remove_quietly,
        "format_size": format_size,
    }
    copyparty_options = {
        "upload_via_temp": settings.upload_via_temp,
        "upload_retries": UPLOAD_RETRIES,
        "retry_backoff_sec": RETRY_BACKOFF_SEC,
        "perms_ttl_sec": PERMS_TTL_SEC,
        "chunk_size": UPLOAD_CHUNK_SIZE,
        "default_timeout": http_timeout,
        "upload_timeout": upload_timeout,
        "ok_move_statuses": OK_MOVE_STATUSES,
        "ok_upload_statuses": OK_UPLOAD_STATUSES,
        "retry_upload_statuses": RETRY_UPLOAD_STATUSES,
        "move_denied_statuses": MOVE_DENIED_STATUSES,
        "http_reasons": HTTP_REASONS,
        "stall_watchdog": stall_watchdog,
        "run_blocking": run_blocking,
        "sleep_waiting": sleep_waiting,
        "format_size": format_size,
    }
    return {
        "telegram_client_type": telegram_client_type,
        "event_type": event_type,
        "event_handler": event_handler,
        "catch_up": settings.catch_up,
        "session_path": settings.session_path,
        "api_id": settings.api_id,
        "api_hash": settings.api_hash,
        "download_service_type": download_service_type,
        "download_options": download_options,
        "httpx_module": httpx_module,
        "http_timeout": http_timeout,
        "copyparty_files_type": copyparty_files_type,
        "copyparty_options": copyparty_options,
        "max_transfers": MAX_CONCURRENT_TRANSFERS,
        "logger": logger,
    }
