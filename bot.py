"""
Userbot: принимает в личку музыку/видео и заливает на copyparty (+ обновляет Jellyfin).

Переменные окружения (новые помечены *):
  TG_API_ID, TG_API_HASH, SESSION_PATH, ALLOWED_SENDER_IDS
  MUSIC_COPYPARTY_URL/USER/PASS, MOVIES_COPYPARTY_URL/USER/PASS
  JELLYFIN_URL, JELLYFIN_API_KEY
  * TMP_DIR            — куда качать временные файлы (по умолчанию /tmp/userbot-uploader).
                         Чтобы докачка переживала не только перезапуск, но и пересоздание контейнера
                         (docker compose up -d после обновления образа), примонтируйте сюда volume
  * CHECK_DISK         — проверять свободное место перед скачиванием (1/0, по умолчанию 1)
  * MIN_FREE_DISK_GB   — сколько ГБ должно остаться свободным после записи (по умолчанию 1.5)
  * STALL_TIMEOUT_SEC  — сколько секунд без прогресса считать зависанием (по умолчанию 180, 0 = выкл.);
                         зависшая попытка перезапускается (с докачкой), а не валит всю передачу
  * UPLOAD_VIA_TEMP    — грузить на copyparty во временный файл и переименовывать (WebDAV MOVE) после проверки
                         размера (1/0, по умолчанию 1). Нужны права move (а для замены существующего
                         файла ещё и delete) у пользователя copyparty; права бот узнаёт сам через ?ls
  * CAPTION_NAMING     — откуда брать имя файла: auto (по умолчанию: подпись, если имя из Telegram
                         неинформативно), always (всегда подпись), never (всегда имя из Telegram)
  * MAX_FLOOD_WAIT_SEC — суммарно сколько секунд ждать FloodWait на одну загрузку (по умолчанию 600)
  * CATCH_UP           — подтягивать сообщения, пришедшие, пока бот был выключен (1/0, по умолчанию 1)
  * AMBIGUOUS_MIN_MB   — «.ts» считаем видео (а не TypeScript), только если файл не меньше N МБ
                         или у него есть видео-атрибуты Telegram (по умолчанию 5)
  * BUTTONS_BOT_TOKEN  — токен бота-помощника (@BotFather). Юзербот не умеет слать inline-кнопки, поэтому,
                         когда тип файла непонятен, кнопки «Музыка / Фильм / Отмена» присылает этот бот
                         (один раз нажмите в нём /start). Без токена бот спрашивает текстом: ответьте на его
                         вопрос «1» (музыка), «2» (фильм) или «0» (отмена)
  * CHOICE_TIMEOUT_SEC — сколько секунд ждать выбора/ответа, прежде чем пропустить файл (по умолчанию 300)
  * LIBRARY_ADMIN      — команды управления музыкальной библиотекой в личке: /find /last /info /rename /set
                         /del /stats /rescan (1/0, по умолчанию 1). Нужен beets В ТОЙ ЖЕ ВЕРСИИ, что и в контейнере
                         импорта, и общие папки библиотеки и конфига по тем же путям, что и там
  * MUSIC_VIDEO_MAX_MIN — короткое видео («Исполнитель - Название», не дольше N минут, по умолчанию 12) считаем
                         возможным музыкальным клипом: бот спрашивает, куда его, и при выборе «Музыка» достаёт звук
                         в .m4a (нужен ffmpeg в образе; 0 — не спрашивать, всё видео идёт в фильмы)
  * FFMPEG_BIN / FFPROBE_BIN — команды ffmpeg и ffprobe (по умолчанию ffmpeg и ffprobe)
  * BEETS_BIN          — команда beets (по умолчанию beet)
  * BEETS_CONFIG       — конфиг beets (по умолчанию /beets-cfg/config.yaml)
  * BEETS_PLUGINS      — плагины beets для команд бота (по умолчанию scrub; пусто = без плагинов)
  * BEETS_TIMEOUT_SEC  — сколько секунд ждать одну команду beets (по умолчанию 120)
  * NAVIDROME_URL/USER/PASS — после правок библиотеки запускать пересканирование Navidrome
                         (по умолчанию http://navidrome:4533; без логина и пароля пересканирование отключено)

  Новое в этой версии:
  * DOWNLOAD_WORKERS   — сколько кусков файла качать из Telegram параллельно (по умолчанию 4; 1 = по одному)
  * DOWNLOAD_REQUEST_KB — размер одного запроса к Telegram, КБ (по умолчанию 1024; допустимо 512, 1024)
  * RESUME_ON_START    — после перезапуска бота продолжать недокачанные/неотправленные файлы (1/0, по умолчанию 1)
  * RESUME_MAX_AGE_HOURS — задания старше этого срока после перезапуска не возобновляются (по умолчанию 24)
  * BATCH_GRACE_SEC    — сколько секунд после последнего файла ждать новых, прежде чем закрыть общий статус
                         пачки/альбома и запустить один скан Jellyfin (по умолчанию 4)

  Ещё новое: HISTORY_DB, DUPLICATE_CHECK, LINK_INGEST, YTDLP_BIN, YTDLP_COOKIES, YTDLP_EXTRA_ARGS,
  YTDLP_TIMEOUT_SEC, LINK_MAX_ENTRIES, LINK_VIDEO_MAX_HEIGHT, LINK_EMBED_THUMB, SUBTITLE_RECODE,
  SUBTITLE_FALLBACK_ENCODING — описаны в блоке «Новое» перед командами библиотеки.

Сериалы (видео с «S01E02» / «1x02» в имени или «N сезон M серия» в подписи) раскладываются по папкам:
  <Название сериала>/Season 01/<файл>. Год в названии папки не используется; если папка сериала уже есть
  (с другим регистром, знаками или годом — «Dune (2021)»), серия кладётся в неё. Если не удаётся понять,
  что за файл, бот спрашивает в чате.
"""
import asyncio
import contextlib
import errno
import functools
import hashlib
import inspect
import json
import logging
import mimetypes
import os
import re
import shlex
import shutil
import sqlite3
import time
import unicodedata
import uuid
import weakref
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, quote, unquote, urlparse

import httpx
from telethon import Button, TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.tl import types as tl_types
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
)
from mutagen.id3 import ID3, ID3NoHeaderError
from mutagen.mp4 import MP4

logging.basicConfig(level=logging.INFO)
# httpx на уровне INFO пишет в лог полные URL запросов, включая ?api_key=... для Jellyfin.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("userbot-dm-uploader")


def env_flag(name: str, default: bool = True) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in ("0", "false", "no", "off", "")


API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_PATH = os.environ.get("SESSION_PATH", "/session/uploader")

ALLOWED_SENDER_IDS = {
    int(x) for x in os.environ.get("ALLOWED_SENDER_IDS", "").split(",") if x.strip()
}

MUSIC_URL = os.environ.get("MUSIC_COPYPARTY_URL", "http://copyparty:3923")
MUSIC_USER = os.environ["MUSIC_COPYPARTY_USER"]
MUSIC_PASS = os.environ["MUSIC_COPYPARTY_PASS"]

MOVIES_URL = os.environ.get("MOVIES_COPYPARTY_URL", "http://copyparty-movies:3923")
MOVIES_USER = os.environ["MOVIES_COPYPARTY_USER"]
MOVIES_PASS = os.environ["MOVIES_COPYPARTY_PASS"]

JELLYFIN_URL = os.environ.get("JELLYFIN_URL", "http://jellyfin:8096")
JELLYFIN_API_KEY = os.environ.get("JELLYFIN_API_KEY", "")

TARGETS = {
    "music": (MUSIC_URL, MUSIC_USER, MUSIC_PASS),
    "movie": (MOVIES_URL, MOVIES_USER, MOVIES_PASS),
}

AUDIO_EXTS = {".mp3", ".flac", ".m4a", ".ogg", ".opus", ".wav", ".aac", ".mka"}
VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"}
SUBTITLE_EXTS = {".srt", ".ass", ".ssa", ".vtt", ".sub"}
# Контейнеры, в которых бывает и звук без картинки (трек, присланный как «.mp4»): для музыки меняем на .m4a.
MP4_FAMILY_EXTS = {".mp4", ".m4v", ".mov"}
# «.ts» — это и MPEG-TS, и TypeScript (Telegram даже присылает его как video/mp2t), поэтому
# видео считаем только при наличии видео-атрибутов или достаточного размера.
AMBIGUOUS_VIDEO_EXTS = {".ts"}
AMBIGUOUS_MIN_BYTES = int(float(os.environ.get("AMBIGUOUS_MIN_MB", "5")) * 1024 * 1024)

# GIF-анимации и стикеры (в т.ч. видеостикеры и кастомные эмодзи) — это тоже mp4/webm, но не медиатека.
JUNK_ATTR_TYPES = tuple(
    getattr(tl_types, name)
    for name in ("DocumentAttributeAnimated", "DocumentAttributeSticker", "DocumentAttributeCustomEmoji")
    if hasattr(tl_types, name)
)

MIME_EXT = {
    "audio/mpeg": ".mp3", "audio/mp3": ".mp3", "audio/mp4": ".m4a", "audio/x-m4a": ".m4a",
    "audio/aac": ".aac", "audio/ogg": ".ogg", "audio/opus": ".opus", "audio/flac": ".flac",
    "audio/x-flac": ".flac", "audio/wav": ".wav", "audio/x-wav": ".wav",
    "video/mp4": ".mp4", "video/x-matroska": ".mkv", "video/webm": ".webm",
    "video/quicktime": ".mov", "video/x-msvideo": ".avi", "video/mp2t": ".ts",
}

UPLOAD_RETRIES = 3
DOWNLOAD_RETRIES = 3
MAX_FLOOD_WAITS = 5
# Суммарный потолок ожидания FloodWait на одну загрузку: пока бот ждёт, он занимает слот очереди.
MAX_FLOOD_WAIT_SEC = int(os.environ.get("MAX_FLOOD_WAIT_SEC", "600"))
RETRY_BACKOFF_SEC = 5
PROGRESS_EDIT_INTERVAL_SEC = 4
SPEED_WINDOW_SEC = 15  # скорость и ETA считаем по скользящему окну, а не по среднему с начала
UPLOAD_CHUNK_SIZE = 1024 * 1024


def _pick_request_size() -> int:
    """Размер одного запроса upload.getFile. Telegram требует: кратно 4 КБ и нацело делит 1 МБ
    (запрос не должен пересекать границу мегабайта). Всё остальное заменяем безопасными 512 КБ."""
    try:
        size = int(float(os.environ.get("DOWNLOAD_REQUEST_KB", "1024")) * 1024)
    except ValueError:
        return 512 * 1024
    if size <= 0 or size % 4096 or (1024 * 1024) % size:
        return 512 * 1024
    return size


DOWNLOAD_REQUEST_SIZE = _pick_request_size()
# Файл режется на сегменты; воркеры берут сегменты по очереди и качают их параллельно (несколько запросов
# «в полёте» разом — именно это и ускоряет закачку: один поток упирается в задержку каждого запроса).
DOWNLOAD_WORKERS = max(1, int(os.environ.get("DOWNLOAD_WORKERS", "4")))
DOWNLOAD_SEGMENT = 4 * 1024 * 1024  # кратно DOWNLOAD_REQUEST_SIZE (а значит и 1 МБ)

# Не больше N файлов одновременно скачиваем/грузим — бережём диск и канал.
MAX_CONCURRENT_TRANSFERS = 2
UPLOAD_SEMAPHORE: Optional[asyncio.Semaphore] = None  # создаётся в main(), в уже запущенном loop

# Зависанием считаем отсутствие прогресса дольше этого времени (держит слот в семафоре).
STALL_TIMEOUT_SEC = int(os.environ.get("STALL_TIMEOUT_SEC", "180"))
WATCHDOG_TICK_SEC = 5

# Загрузка на copyparty: сначала во временный файл, потом переименование (иначе обрыв оставляет на
# сервере битый файл, а повторная загрузка поверх существующего создаёт копию «имя-<время>-<id>.mp3»).
UPLOAD_VIA_TEMP = env_flag("UPLOAD_VIA_TEMP", True)
PERMS_TTL_SEC = 3600  # как долго помнить права пользователя copyparty
OK_MOVE_STATUSES = {200, 201, 204}
# copyparty на отсутствие права move/delete отвечает 401 (не 403!); 405/501 — метод не поддерживается.
MOVE_DENIED_STATUSES = {401, 403, 405, 501}
# MKCOL: 201 — создано, 405 — папка уже есть (оба случая нас устраивают).
OK_MKCOL_STATUSES = {200, 201, 204, 405}

# Откуда брать имя файла: подпись к сообщению или имя из Telegram (см. should_use_caption).
CAPTION_NAMING = os.environ.get("CAPTION_NAMING", "auto").strip().lower()
if CAPTION_NAMING not in ("auto", "always", "never"):
    CAPTION_NAMING = "auto"

# Догонять сообщения, пришедшие, пока бот был выключен.
CATCH_UP = env_flag("CATCH_UP", True)

# Вопрос «музыка или фильм?», когда тип файла определить не удалось.
BUTTONS_BOT_TOKEN = os.environ.get("BUTTONS_BOT_TOKEN", "").strip()
CHOICE_TIMEOUT_SEC = int(os.environ.get("CHOICE_TIMEOUT_SEC", "300"))

# Музыкальные клипы: короткое видео вида «Исполнитель - Название» — спрашиваем, не музыка ли это.
MUSIC_VIDEO_MAX_MIN = float(os.environ.get("MUSIC_VIDEO_MAX_MIN", "12"))
MUSIC_VIDEO_UNKNOWN_DURATION_MAX_BYTES = 150 * 1024 * 1024  # если Telegram не прислал длительность — судим по размеру
FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.environ.get("FFPROBE_BIN", "ffprobe")
FFMPEG_TIMEOUT_SEC = int(os.environ.get("FFMPEG_TIMEOUT_SEC", "600"))
BOT_VERSION = "2026-10-17-history-subs-dupes-links"  # пишется в лог при старте: по нему видно, какая версия реально запущена

# Управление библиотекой через beets (команды /find /rename /del …).
LIBRARY_ADMIN = env_flag("LIBRARY_ADMIN", True)
BEETS_BIN = os.environ.get("BEETS_BIN", "beet")
BEETS_CONFIG = os.environ.get("BEETS_CONFIG", "/beets-cfg/config.yaml")
# Плагины для команд бота. «-p» заменяет plugins из конфига: бот не должен тянуть chroma/fetchart и их зависимости.
BEETS_PLUGINS = os.environ.get("BEETS_PLUGINS", "scrub")
BEETS_TIMEOUT_SEC = int(os.environ.get("BEETS_TIMEOUT_SEC", "120"))
# Navidrome: после правок библиотеки запускаем пересканирование (если заданы логин и пароль).
NAVIDROME_URL = os.environ.get("NAVIDROME_URL", "http://navidrome:4533")
NAVIDROME_USER = os.environ.get("NAVIDROME_USER", "")
NAVIDROME_PASS = os.environ.get("NAVIDROME_PASS", "")

# Какие HTTP-статусы считаем успешной загрузкой и какие ошибки имеет смысл повторять.
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

HTTP_DEFAULT_TIMEOUT = httpx.Timeout(30.0)
UPLOAD_TIMEOUT = httpx.Timeout(connect=30.0, read=300.0, write=120.0, pool=30.0)

# Имя файла на сервере (ограничение ФС — 255 байт, оставляем запас).
MAX_FILENAME_BYTES = 200
MAX_DIRNAME_BYTES = 120

# Временные файлы. Папка выделенная: при старте из неё удаляется всё, что не относится к заданиям,
# которые можно продолжить (см. prepare_tmp_dir).
TMP_DIR = Path(os.environ.get("TMP_DIR", "/tmp/userbot-uploader"))
# Журнал заданий: пока файл не залит, о нём остаётся запись, и после перезапуска бот продолжает с места обрыва.
JOURNAL_DIR = TMP_DIR / "jobs"
RESUME_ON_START = env_flag("RESUME_ON_START", True)
RESUME_MAX_AGE_SEC = int(float(os.environ.get("RESUME_MAX_AGE_HOURS", "24")) * 3600)
RESUME_MAX_STARTS = 3  # задание, на котором бот падает раз за разом, через N запусков выбрасываем

# Пачки и альбомы: файлы, пришедшие друг за другом, делят одно статусное сообщение и один скан Jellyfin.
BATCH_GRACE_SEC = float(os.environ.get("BATCH_GRACE_SEC", "4"))
MAX_BATCH_LINES = 20
MAX_MESSAGE_CHARS = 3900  # лимит Telegram — 4096

# Проверка места на диске: после записи файла должно остаться не меньше MIN_FREE_DISK_AFTER_BYTES.
CHECK_DISK = env_flag("CHECK_DISK", True)
MIN_FREE_DISK_AFTER_BYTES = int(float(os.environ.get("MIN_FREE_DISK_GB", "1.5")) * 1024 ** 3)

_client_kwargs = {}
if CATCH_UP:
    if "catch_up" in inspect.signature(TelegramClient.__init__).parameters:
        _client_kwargs["catch_up"] = True
    else:
        log.warning("Эта версия Telethon не поддерживает catch_up — пропущенные сообщения догонять не буду")

client = TelegramClient(
    SESSION_PATH,
    API_ID,
    API_HASH,
    connection_retries=-1,  # переподключаться бесконечно, а не сдаваться после 5 попыток
    retry_delay=2,
    auto_reconnect=True,
    **_client_kwargs,
)
# Статусы показывают имена файлов «как есть»: [Remix](2020), __x__ и т.п. не должны разбираться как Markdown.
client.parse_mode = None

# Общий httpx-клиент (создаётся в main()): переиспользует соединения вместо нового на каждый запрос.
http: Optional[httpx.AsyncClient] = None

# Бот-помощник для inline-кнопок (создаётся в main(), если задан BUTTONS_BOT_TOKEN).
bot_client: Optional[TelegramClient] = None
# Ожидающие выбора: ключ ("cb", token) или ("reply", chat_id, id_вопроса) → (Future с ответом, текстовые синонимы
# или None, если ждём произвольный текст).
PENDING_CHOICES: dict = {}

# Сообщения, которые уже в работе (или будут возобновлены после перезапуска): защита от повторной обработки,
# например когда catch_up заново присылает сообщение, из-за которого бот упал.
ACTIVE_JOBS: set = set()
# Фоновые задачи держим по ссылке: asyncio хранит на них только слабые ссылки.
BACKGROUND_TASKS: set = set()


def spawn(coro):
    task = asyncio.create_task(coro)
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)
    return task


# ---------- состояние передачи и ошибки ----------
@dataclass(eq=False)
class TransferState:
    filename: str
    total: int = 1
    phase: str = "download"
    current: int = 0
    attempt: int = 1
    waiting: bool = False  # True, пока спим перед повтором / FloodWait — это не «зависание»
    disk_reserved: int = 0  # сколько байт ещё предстоит дописать на диск (для проверки места)
    phase_started_at: float = field(default_factory=time.monotonic)
    phase_start_current: int = 0

    def begin(self, phase: str, attempt: int, current: int = 0, total: Optional[int] = None) -> None:
        """Начало новой фазы/попытки — от этой точки считаются скорость и ETA."""
        self.phase = phase
        self.attempt = attempt
        self.current = current
        self.phase_start_current = current
        self.phase_started_at = time.monotonic()
        if total is not None:
            self.total = max(total, 1)


class TransferError(Exception):
    """Ошибка с понятной пользователю причиной."""

    def __init__(self, user_message: str, retriable: bool = False, retry_after: Optional[float] = None):
        super().__init__(user_message)
        self.user_message = user_message
        self.retriable = retriable
        self.retry_after = retry_after  # пауза перед повтором, если её потребовал сервер


class StallError(TransferError):
    """Нет прогресса дольше STALL_TIMEOUT_SEC. Повторяемая: зависшую попытку просто перезапускаем."""

    def __init__(self, seconds: float):
        super().__init__(f"нет прогресса больше {int(seconds)} сек (зависание)", retriable=True)


class RenameUnsupported(TransferError):
    """copyparty не разрешил переименовать временный файл (нет права move/delete или метод не поддерживается)."""


ACTIVE_TRANSFERS = set()


@dataclass
class Job:
    """Одно принятое к загрузке сообщение. Всё, что нужно, чтобы довести его до конца после перезапуска
    (вопросы пользователю уже заданы, имя и раскладка определены)."""
    chat_id: int
    sender_id: int
    message_id: int
    kind: str  # "music" | "movie"
    filename: str  # имя файла на сервере (без папок)
    size: int = 0
    extract: bool = False  # достать звук из клипа
    series: Optional[dict] = None  # {"show": «Название», "season": 1} — серия сериала
    starts: int = 0  # сколько раз задание возобновляли после перезапуска
    url: Optional[str] = None  # скачивать не из Telegram, а по ссылке (yt-dlp)
    part: int = 0  # номер элемента в плейлисте (у заданий по ссылке ≥ 1)
    subtitle: bool = False  # файл субтитров: имя подбирается под видео на сервере
    force: bool = False  # не проверять дубликаты

    @property
    def key(self) -> tuple:
        if self.part:
            return (self.chat_id, self.message_id, self.part)
        return (self.chat_id, self.message_id)

    @property
    def local_path(self) -> Path:
        # Имя временного файла не зависит от названия (длинные/странные имена не ломают ФС),
        # а пара sender_id + message.id уникальна между разными отправителями.
        tail = f"_{self.part}" if self.part else ""
        return TMP_DIR / f"{self.sender_id}_{self.message_id}{tail}.part"

    @property
    def journal_path(self) -> Path:
        tail = f"_{self.part}" if self.part else ""
        return JOURNAL_DIR / f"{self.sender_id}_{self.message_id}{tail}.json"


def describe_status(code: int) -> str:
    reason = HTTP_REASONS.get(code)
    return f"сервер вернул {code}" + (f" ({reason})" if reason else "")


def describe_error(exc: BaseException) -> str:
    """Короткая причина сбоя для сообщения пользователю."""
    if isinstance(exc, TransferError):
        return exc.user_message
    if isinstance(exc, FloodWaitError):
        return f"Telegram просит подождать {exc.seconds} сек (FloodWait)"
    if isinstance(exc, (asyncio.TimeoutError, httpx.TimeoutException)):
        return "таймаут: сервер не отвечает"
    if isinstance(exc, httpx.ConnectError):
        return "не удалось подключиться к серверу"
    if isinstance(exc, httpx.TransportError):
        return f"сетевая ошибка ({type(exc).__name__})"
    if isinstance(exc, OSError):
        if exc.errno == errno.ENOSPC:
            return "на диске бота закончилось место"
        return f"ошибка диска ({exc.strerror or exc})"
    return f"внутренняя ошибка ({type(exc).__name__})"


# ---------- вспомогательное ----------
async def run_blocking(func, *args):
    """Выполняет блокирующий вызов (диск, mutagen) в потоке, чтобы не вешать event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args))


async def sleep_waiting(state: TransferState, seconds: float) -> None:
    state.waiting = True
    try:
        await asyncio.sleep(seconds)
    finally:
        state.waiting = False


def remove_quietly(path: Path) -> None:
    with contextlib.suppress(OSError):
        path.unlink()


def classify(
    filename: str,
    mime_type: Optional[str] = None,
    size: int = 0,
    has_video_attr: bool = False,
    has_audio_attr: bool = False,
):
    """Музыка или фильм. Расширению доверяем не слепо: трек в контейнере mp4 (Telegram-аудио с именем
    «Artist - Title.mp4») по расширению выглядит как видео, поэтому сначала смотрим на атрибуты и mime."""
    ext = Path(filename).suffix.lower()
    mime = (mime_type or "").lower()
    if ext in SUBTITLE_EXTS:
        return "subtitle"
    if ext in AUDIO_EXTS:
        return "music"
    if ext in VIDEO_EXTS:
        if has_video_attr:
            return "movie"
        # Нет видео-атрибута, но Telegram считает файл аудио (атрибут Audio или mime audio/*) → это трек.
        if has_audio_attr or mime.startswith("audio/"):
            return "music"
        return "movie"
    if ext in AMBIGUOUS_VIDEO_EXTS:
        # По mime тут не отличить (TypeScript тоже «video/mp2t»), поэтому смотрим на атрибуты и размер.
        return "movie" if (has_video_attr or size >= AMBIGUOUS_MIN_BYTES) else None
    # Расширение не распознали — подстрахуемся по mime_type, если Telegram его прислал.
    if mime_type:
        if mime_type.startswith("audio/"):
            return "music"
        if mime_type.startswith("video/"):
            return "movie"
    return None


def ext_from_mime(mime_type: Optional[str]) -> str:
    if not mime_type:
        return ""
    mime_type = mime_type.lower()
    return MIME_EXT.get(mime_type) or mimetypes.guess_extension(mime_type) or ""


_VALID_EXT_RE = re.compile(r"\.[A-Za-z0-9]{1,5}")


def ensure_extension(filename: str, kind: str, mime_type: Optional[str]) -> tuple:
    """Гарантирует нормальное расширение. Возвращает (имя_файла, расширение).

    Если «расширения» нет или это обрывок названия («Mr. Brightside» → суффикс «. Brightside»),
    расширение ДОПИСЫВАЕТСЯ к имени целиком, а не подменяет суффикс — иначе часть названия теряется."""
    ext = Path(filename).suffix
    if _VALID_EXT_RE.fullmatch(ext):
        return filename, ext
    ext = ext_from_mime(mime_type) or (".mp4" if kind == "movie" else ".mp3")
    return filename + ext, ext


# ---------- имена файлов и кодировки ----------
def normalize_text(text: str) -> str:
    """Убирает управляющие/невидимые символы и приводит Юникод к NFC.
    (iOS/macOS нередко присылают «й» как «и» + комбинируемый символ — в плеерах это выглядит как мусор.)"""
    text = re.sub(r"[\x00-\x1f\x7f]", " ", text)
    text = re.sub(r"[\u200b-\u200f\u202a-\u202e\u2060\ufeff]", "", text)
    return unicodedata.normalize("NFC", text)


def sanitize_name(name: str) -> str:
    name = normalize_text(name)
    name = re.sub(r'[\\/:*?"<>|]', "", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip(" .")


def safe_basename(name: str) -> str:
    """Убираем путь и потенциальный path traversal (../, /, \\) из имени файла,
    пришедшего от Telegram — оно используется как часть URL и имени на сервере."""
    name = os.path.basename(name.replace("\\", "/"))
    name = name.lstrip(".")  # чтобы не получить скрытый файл или пустое имя через ведущие точки
    return name or "file"


def truncate_filename(name: str, max_bytes: int = MAX_FILENAME_BYTES) -> str:
    """Укорачивает имя до max_bytes (в UTF-8), сохраняя расширение."""
    if len(name.encode("utf-8")) <= max_bytes:
        return name
    ext = Path(name).suffix
    stem = name[: -len(ext)] if ext else name
    budget = max(max_bytes - len(ext.encode("utf-8")), 1)
    stem = stem.encode("utf-8")[:budget].decode("utf-8", errors="ignore").rstrip(" .")
    return (stem or "file") + ext


def truncate_dirname(name: str, max_bytes: int = MAX_DIRNAME_BYTES) -> str:
    """Укорачивает имя папки (у неё нет расширения, которое надо беречь)."""
    if len(name.encode("utf-8")) <= max_bytes:
        return name
    cut = name.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore").rstrip(" .")
    return cut or "folder"


_CYRILLIC_RE = re.compile(r"[А-Яа-яЁёІіЇїЄєҐґЎў]")
_CYRILLIC_VOWEL_RE = re.compile(r"[аеёиоуыэюяіїє]", re.IGNORECASE)


def _decode_cp1251_word(word: str) -> Optional[tuple]:
    """Слово из cp1251-байтов, прочитанных как Latin-1 («Èãîðü» → «Игорь»).
    Возвращает (исправленное_слово, число_букв) или None, если слово не похоже на такой мусор."""
    letters = [c for c in word if c.isalpha()]
    if len(letters) < 2 or not all(0x80 <= ord(c) <= 0xFF for c in letters):
        return None  # есть ASCII-буквы или «настоящие» не-latin1 символы — не наш случай
    try:
        fixed = word.encode("latin-1").decode("cp1251")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return None
    fixed_letters = [c for c in fixed if c.isalpha()]
    if not fixed_letters or not all(_CYRILLIC_RE.fullmatch(c) for c in fixed_letters):
        return None
    core = "".join(fixed_letters)
    # У настоящего русского слова из 3+ букв есть гласная; «äöü» → «дць» — это просто немецкие умлауты.
    if len(core) >= 3 and not _CYRILLIC_VOWEL_RE.search(core):
        return None
    return fixed, len(core)


def _fix_cp1251_text(text: str) -> str:
    fixes = {}
    for m in re.finditer(r"\S+", text):
        word = m.group(0)
        if word not in fixes:
            res = _decode_cp1251_word(word)
            if res:
                fixes[word] = res
    # Кракозябры от cp1251 — это целые фразы. Без хотя бы одного «длинного» слова (3+ буквы) ничего
    # не трогаем: одинокое «ñó» — вполне законный текст, а не «су». Короткие слова («íå» → «не»)
    # чиним только рядом с таким длинным.
    if not any(n >= 3 for _, n in fixes.values()):
        return text
    return re.sub(r"\S+", lambda m: fixes[m.group(0)][0] if m.group(0) in fixes else m.group(0), text)


def fix_mojibake(text: str) -> str:
    """Чинит типичные «кракозябры»: UTF-8 или cp1251, прочитанные как Latin-1/cp1252.
    Нормальный текст возвращается без изменений."""
    if not text or text.isascii():
        return text
    for wrong in ("latin-1", "cp1252"):
        try:
            fixed = text.encode(wrong).decode("utf-8")  # строго: случайно валидный UTF-8 — редкость
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
        if fixed != text:
            return fixed
    return _fix_cp1251_text(text)


def clean_filename(name: str) -> str:
    """Имя файла из Telegram: чиним кодировку, убираем путь и запрещённые символы."""
    name = sanitize_name(safe_basename(normalize_text(fix_mojibake(name))))
    return name or "file"


def guess_filename(message, doc) -> str:
    """Имя для документа без атрибута file_name (например, аудио от ботов)."""
    attrs = doc.attributes or []
    audio = next((a for a in attrs if isinstance(a, DocumentAttributeAudio)), None)
    if audio and (audio.performer or audio.title):
        base = " - ".join(p for p in (audio.performer, audio.title) if p)
    else:
        stamp = message.date.strftime("%Y%m%d_%H%M%S") if message.date else "file"
        base = f"telegram_{stamp}_{message.id}"
    return base + ext_from_mime(getattr(doc, "mime_type", None))


# ---------- диск и журнал заданий ----------
def meta_path(local_path: Path) -> Path:
    """Рядом с недокачанным файлом лежит «паспорт»: сколько байт с начала файла точно скачано."""
    return local_path.with_name(local_path.name + ".meta")


def drop_job_files(job: Job) -> None:
    """Убирает всё, что осталось на диске от задания (временные файлы и запись в журнале)."""
    remove_quietly(job.local_path)
    remove_quietly(meta_path(job.local_path))
    remove_quietly(job.local_path.with_suffix(".m4a"))
    if job.url:  # файлы yt-dlp: «<имя>.mp4/.m4a/.info.json/.webm.part …»
        for p in TMP_DIR.glob(job.local_path.stem + ".*"):
            remove_quietly(p)
    remove_quietly(job.journal_path)


def journal_write(job: Job) -> None:
    try:
        JOURNAL_DIR.mkdir(parents=True, exist_ok=True)
        tmp = job.journal_path.with_name(job.journal_path.name + ".tmp")
        tmp.write_text(json.dumps(asdict(job), ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, job.journal_path)
    except OSError as e:
        log.warning("Не удалось записать задание в журнал (после перезапуска докачки не будет): %s", e)


def load_journal() -> list:
    """Задания, которые можно продолжить. Битые, старые и «падающие» записи выбрасываем."""
    jobs = []
    now = time.time()
    for p in sorted(JOURNAL_DIR.glob("*.json")):
        try:
            job = Job(**json.loads(p.read_text(encoding="utf-8")))
            fresh = now - p.stat().st_mtime < RESUME_MAX_AGE_SEC
        except (OSError, ValueError, TypeError):
            remove_quietly(p)
            continue
        if not fresh or job.starts >= RESUME_MAX_STARTS:
            log.info("Задание «%s» не возобновляю (старое или слишком часто падало) — удаляю", job.filename)
            drop_job_files(job)
            continue
        jobs.append(job)
    return jobs


def prepare_tmp_dir() -> list:
    """Чистит временную папку и возвращает задания, которые надо продолжить после перезапуска.
    Недокачанные файлы остаются только если у них есть запись в журнале; всё остальное — мусор."""
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    JOURNAL_DIR.mkdir(parents=True, exist_ok=True)
    if RESUME_ON_START:
        jobs = load_journal()
    else:
        jobs = []
        for p in JOURNAL_DIR.glob("*"):
            remove_quietly(p)

    for p in JOURNAL_DIR.glob("*.tmp"):  # недописанные записи журнала от аварийной остановки
        remove_quietly(p)

    keep = set()
    for job in jobs:
        keep.add(job.local_path.name)
        keep.add(meta_path(job.local_path).name)

    removed = 0
    for p in TMP_DIR.iterdir():
        if p.is_file() and p.name not in keep:
            remove_quietly(p)
            removed += 1
    if removed:
        log.info("Удалено %d ненужных временных файлов из %s", removed, TMP_DIR)
    for job in jobs:
        log.info("К продолжению после перезапуска: «%s» (сообщение %s)", job.filename, job.message_id)
    return jobs


def ensure_disk_space(needed_bytes: int, state: TransferState) -> None:
    """Проверяем место ПЕРЕД скачиванием (уже внутри семафора — тогда данные актуальны).
    Учитываем и то, что параллельные загрузки ещё допишут на диск.
    Требуем запас 10% и жёсткий минимум MIN_FREE_DISK_AFTER_BYTES после записи."""
    state.disk_reserved = max(needed_bytes, 0)
    if not CHECK_DISK or needed_bytes <= 0:
        return
    try:
        usage = shutil.disk_usage(TMP_DIR)
    except OSError as e:
        log.warning("Не удалось проверить свободное место в %s: %s", TMP_DIR, e)
        return
    pending_elsewhere = sum(s.disk_reserved for s in ACTIVE_TRANSFERS if s is not state)
    free = usage.free - pending_elsewhere
    if free < needed_bytes * 1.1 or free - needed_bytes < MIN_FREE_DISK_AFTER_BYTES:
        state.disk_reserved = 0
        raise TransferError(
            f"на диске бота мало места: нужно {format_size(needed_bytes)}, "
            f"свободно {format_size(max(free, 0))} "
            f"(должно остаться не меньше {format_size(MIN_FREE_DISK_AFTER_BYTES)})"
        )


# ---------- визуал статусных сообщений ----------
def format_size(num_bytes: float) -> str:
    if num_bytes >= 1024 ** 3:
        return f"{num_bytes / 1024 ** 3:.2f} ГБ"
    return f"{num_bytes / 1024 ** 2:.1f} МБ"


def format_speed(bytes_per_sec: float) -> str:
    if bytes_per_sec <= 0:
        return "— КБ/с"
    if bytes_per_sec >= 1024 ** 2:
        return f"{bytes_per_sec / 1024 ** 2:.1f} МБ/с"
    return f"{bytes_per_sec / 1024:.0f} КБ/с"


def format_eta(seconds: float) -> str:
    if seconds <= 0 or seconds == float("inf"):
        return "—"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h} ч {m} мин"
    if m:
        return f"{m} мин {s} сек"
    return f"{s} сек"


def make_progress_bar(pct: int, width: int = 20) -> str:
    pct = max(0, min(100, pct))
    filled = int(width * pct / 100)
    return "█" * filled + "░" * (width - filled)


def fit_text(text: str, limit: int = MAX_MESSAGE_CHARS) -> str:
    return text if len(text) <= limit else text[: limit - 20].rstrip() + "\n…(обрезано)"


def short_name(name: str, limit: int = 60) -> str:
    return name if len(name) <= limit else name[: limit - 1] + "…"


async def safe_edit(message, text: str) -> bool:
    try:
        await message.edit(text)
        return True
    except Exception as e:
        log.warning("Не удалось обновить статусное сообщение: %s", e)
        return False


# ---------- парсинг подписи для видео (фильм/сериал) ----------
# Формат непредсказуем по содержанию, но структура обычно такая:
#   🆔 `id`                              (опционально, игнорируем)
#   Название (доп.инфо [качество])       (обязательная первая содержательная строка)
#   N сезон M серия                      (опционально, только для сериалов)
#   «Название серии»                     (опционально)
#   [ссылки/реклама источника]           (игнорируем — строки со скобками-ссылками/URL)
_QUALITY_RE = re.compile(r"\[(\d{3,4}[pi]|[48]k)\]", re.IGNORECASE)
_YEAR_RE = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")

# Рекламная/служебная строка источника: @канал, ссылки, #хэштег, «pesni.fm», «скачать…». Её нельзя
# принимать за название, иначе файл превратится в «Скачать тут - @channel.mp3».
_AD_RE = re.compile(
    r"@\w|https?://|t\.me/|www\.|(?<!\w)#[^\W\d_]"
    r"|[\w-]{2,}\.(?:com|ru|net|org|fm|me|su|ua|by|io|cc|ly|tv)\b"
    r"|скача[йт]|подпис(?:ывайся|ывайтесь|ись)",
    re.IGNORECASE,
)


def looks_like_ad(line: str) -> bool:
    return bool(_AD_RE.search(line))


def parse_video_caption(text: str):
    if not text:
        return None

    lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]
    lines = [ln for ln in lines if not ln.startswith("🆔")]
    if not lines:
        return None

    title_line = lines[0]
    extra_lines = lines[1:]
    if looks_like_ad(title_line):
        return None  # первая строка — реклама, а не название: оставляем имя файла из Telegram

    # Качество вида [1080p] может стоять и внутри скобок, и после них — вырезаем его в любом случае.
    q = _QUALITY_RE.search(title_line)
    quality = q.group(1) if q else None
    if q:
        title_line = (title_line[: q.start()] + title_line[q.end():]).strip()

    year = None
    m = re.match(r"^(.*?)\s*\((.*)\)\s*$", title_line)
    if m:
        title = sanitize_name(m.group(1))
        y = _YEAR_RE.search(m.group(2))  # год из скобок нужен Jellyfin: «Dune (2021)»
        year = int(y.group(1)) if y else None
    else:
        # нет скобок с доп.инфой — берём строку как есть, это тоже нормальное название
        title = sanitize_name(title_line)
    if not title:
        return None

    season = episode = None
    episode_title = None
    for line in extra_lines:
        # пропускаем строки-ссылки/рекламу источника (markdown-ссылки, голые URL, emoji-подписи каналов)
        if line.startswith("[") or line.startswith("http") or re.search(r"https?://", line):
            continue
        se = re.search(r"(\d+)\s*сезон\D+(\d+)\s*серия", line, re.IGNORECASE)
        if se:
            season, episode = int(se.group(1)), int(se.group(2))
            continue
        et = re.match(r"^[«\"](.+)[»\"]$", line)
        if et:
            episode_title = sanitize_name(et.group(1))

    return {
        "title": title,
        "year": year,
        "quality": quality,
        "season": season,
        "episode": episode,
        "episode_title": episode_title,
    }


def build_filename_from_video_caption(parsed: dict, ext: str) -> str:
    base = parsed["title"]
    if parsed.get("year"):
        base += f" ({parsed['year']})"
    # `is not None`, а не truthiness: сезон 0 — это спецвыпуски, а не «нет сезона»
    if parsed["season"] is not None and parsed["episode"] is not None:
        base += f" - S{parsed['season']:02d}E{parsed['episode']:02d}"
        if parsed["episode_title"]:
            base += f" - {parsed['episode_title']}"
    if parsed["quality"]:
        base += f" [{parsed['quality']}]"
    return f"{base}{ext}"


# ---------- парсинг подписи для музыки ----------
# Формат обычно: первая строка "Исполнитель - Название" (иногда с пометкой типа "(в файле)"),
# дальше — реклама/ссылки бота-источника, которую игнорируем.
def parse_music_caption(text: str):
    if not text:
        return None

    lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]
    if not lines:
        return None

    first = lines[0]
    # убираем служебные пометки в скобках вроде "(в файле)", не трогая содержательные
    first = re.sub(r"\(\s*в\s*файле\s*\)", "", first, flags=re.IGNORECASE).strip()
    if looks_like_ad(first):
        return None  # первая строка — реклама, а не «Исполнитель - Название»

    m = re.match(r"^(.+?)\s+-\s+(.+)$", first)
    if not m:
        return None

    artist = sanitize_name(m.group(1))
    title = sanitize_name(m.group(2))
    if not artist or not title:
        return None

    return {"artist": artist, "title": title}


def build_filename_from_music_caption(parsed: dict, ext: str) -> str:
    return f"{parsed['artist']} - {parsed['title']}{ext}"


# ---------- что победит: подпись или имя файла из Telegram ----------
_GENERIC_NAME_WORDS = {
    "", "audio", "audiofile", "video", "videofile", "file", "document", "doc", "track", "music", "song",
    "movie", "film", "voice", "telegram", "tg", "vid", "mov", "untitled", "unknown", "noname",
    "аудио", "видео", "файл", "трек", "музыка", "фильм", "безназвания",
}
_FILE_ID_RE = re.compile(r"[A-Za-z0-9_-]{20,}")
_EPISODE_IN_NAME_RE = re.compile(r"(?<![A-Za-z0-9])S\d{1,2}E\d{1,3}(?![0-9])|(?<![A-Za-z0-9])\d{1,2}x\d{2,3}(?![A-Za-z0-9])", re.IGNORECASE)
_LETTER_RE = re.compile(r"[^\W\d_]")


def is_generic_name(stem: str) -> bool:
    """«audio_123», «Telegram 2024-01-01 12.00.00», «AgADxxxxxxxxxxxxxxxxx» — имя, из которого ничего не узнать."""
    core = re.sub(r"[\d\W_]+", "", stem.lower())
    if core in _GENERIC_NAME_WORDS:
        return True
    return bool(_FILE_ID_RE.fullmatch(stem) and re.search(r"\d", stem))


def should_use_caption(kind: str, orig_filename: str, parsed: dict) -> bool:
    """Подпись должна перебивать имя из Telegram не всегда: если файл уже называется нормально
    («Исполнитель - Название.mp3», «Film.2021.1080p.mkv»), подпись с рекламой или шумом его только испортит.
    Режим задаётся CAPTION_NAMING: auto (по умолчанию) / always / never."""
    if CAPTION_NAMING == "never":
        return False
    if CAPTION_NAMING == "always":
        return True
    stem = Path(orig_filename).stem
    if is_generic_name(stem):
        return True
    if kind == "music":
        m = re.match(r"^(.+?)\s+-\s+(.+)$", stem)
        informative = bool(m and _LETTER_RE.search(m.group(1)) and _LETTER_RE.search(m.group(2)))
        return not informative
    # фильм/сериал
    has_episode = bool(_EPISODE_IN_NAME_RE.search(stem))
    if parsed.get("season") is not None and parsed.get("episode") is not None:
        return not has_episode  # подпись знает серию, а имя файла — нет
    if has_episode:
        return False
    return not (_YEAR_RE.search(stem) and len(_LETTER_RE.findall(stem)) >= 3)


# ---------- сериалы: распознавание и имена папок ----------
# «Show.S01E02», «Show - S01E02 - Title», «Show 1x02». Номер сезона обязателен: «E05» без сезона сериалом не считаем.
_SERIES_MARK_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:S(\d{1,2})[ ._-]?E(\d{1,3})|(\d{1,2})x(\d{2,3}))(?![0-9])", re.IGNORECASE
)
_PAREN_YEAR_RE = re.compile(r"[\(\[]\s*(?:19|20)\d{2}\s*[\)\]]")
_TRAILING_YEAR_RE = re.compile(r"[\s._-]+(?:19|20)\d{2}\s*$")


def clean_show_name(raw: str) -> str:
    """Название сериала из «Show.Name.2021.» / «Show Name (2021) - » → «Show Name». Год в имени папки не нужен."""
    s = normalize_text(raw or "").strip()
    if " " not in s:
        s = re.sub(r"[._]+", " ", s)  # «Show.Name.» — точки вместо пробелов
    else:
        s = s.replace("_", " ")
    s = _PAREN_YEAR_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip(" .-–—_")
    s = _TRAILING_YEAR_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip(" .-–—_")
    return sanitize_name(s)


def parse_episode_from_name(stem: str):
    """Сезон/серия/название сериала из имени файла или введённого вручную текста. None — это не серия."""
    m = _SERIES_MARK_RE.search(stem)
    if not m:
        return None
    return {
        "show": clean_show_name(stem[: m.start()]),
        "season": int(m.group(1) or m.group(3)),
        "episode": int(m.group(2) or m.group(4)),
    }


def detect_series(orig_filename: str, parsed: Optional[dict]):
    """Серия сериала или нет. Возвращает dict {show, season, episode, episode_title, quality, name_ok} или None.
    name_ok=True — имя файла уже хорошее и годится как есть; иначе имя надо собрать (build_series_filename).
    show может быть пустым — тогда вызывающий спросит пользователя."""
    stem = Path(orig_filename).stem
    from_name = parse_episode_from_name(stem)
    cap = None
    if (
        parsed and CAPTION_NAMING != "never"
        and parsed.get("season") is not None and parsed.get("episode") is not None
    ):
        cap = parsed

    if cap and (from_name is None or should_use_caption("movie", orig_filename, cap)):
        return {
            "show": clean_show_name(cap["title"]), "season": cap["season"], "episode": cap["episode"],
            "episode_title": cap.get("episode_title"), "quality": cap.get("quality"), "name_ok": False,
        }
    if from_name:
        show = from_name["show"]
        name_ok = True
        if (not show or is_generic_name(show)) and parsed and parsed.get("title") and CAPTION_NAMING != "never":
            show = clean_show_name(parsed["title"])  # в имени файла названия нет, а в подписи есть
            name_ok = False
        return {
            "show": show, "season": from_name["season"], "episode": from_name["episode"],
            "episode_title": None, "quality": None, "name_ok": name_ok,
        }
    return None


def build_series_filename(info: dict, ext: str) -> str:
    """«Show - S01E02 - Название серии [1080p].mkv» — без года (имя папки сериала тоже без него)."""
    base = f"{info['show']} - S{info['season']:02d}E{info['episode']:02d}"
    if info.get("episode_title"):
        base += f" - {info['episode_title']}"
    if info.get("quality"):
        base += f" [{info['quality']}]"
    return f"{base}{ext}"


def series_key(name: str) -> str:
    """Ключ для сопоставления папок сериала: без регистра, знаков препинания и года в скобках.
    «Dune: Prophecy», «dune prophecy» и «Dune Prophecy (2024)» дают один и тот же ключ."""
    s = unicodedata.normalize("NFKC", name).casefold().replace("ё", "е")
    s = _PAREN_YEAR_RE.sub(" ", s)
    return re.sub(r"[\W_]+", "", s)


_SEASON_DIR_RE = re.compile(r"(?:season|сезон|s)\s*0*(\d+)", re.IGNORECASE)


def season_number(dirname: str) -> Optional[int]:
    """«Season 01», «Season 1», «Сезон 2», «S03» → номер сезона."""
    m = _SEASON_DIR_RE.fullmatch(dirname.strip())
    return int(m.group(1)) if m else None


# ---------- ID3-теги mp3 ----------
def _has_id3v1(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            f.seek(-128, os.SEEK_END)
            return f.read(3) == b"TAG"
    except OSError:
        return False


def fix_mp3_tags(path: str) -> None:
    """Приводит теги mp3 в порядок (синхронная; вызывать через run_blocking):

    1. убирает мусорные ID3-фреймы (пустые USLT/UFID от pesni.fm), которые ломают tagr (node-taglib-sharp);
    2. чинит «кракозябры» в тексте тегов (cp1251/UTF-8, прочитанные как Latin-1);
    3. явно помечает не-ASCII текст как Unicode (UTF-16 для ID3v2.3, UTF-8 для v2.4). Теги с кириллицей
       в «Latin-1» плееры (особенно Android) угадывают сами — так и появляются иероглифы;
    4. удаляет ID3v1 — он не умеет Unicode, а часть плееров предпочитает его ID3v2.
    """
    try:
        tag = ID3(path)
    except ID3NoHeaderError:
        return  # тегов нет вообще — чинить нечего
    except Exception as e:
        # Битые/нестандартные mp3 могут кидать не только MutagenError
        # (KeyError, struct.error и т.п.) — это не критичный путь, просто пропускаем.
        log.warning("Не удалось прочитать ID3-теги для очистки: %s: %s", path, e)
        return

    target_v23 = tuple(tag.version[:2]) == (2, 3)
    unicode_encoding = 1 if target_v23 else 3  # 1 = UTF-16, 3 = UTF-8
    changed = False

    for key in list(tag.keys()):
        frame = tag[key]

        if key.startswith("USLT") and (not frame.text or getattr(frame, "lang", "") == "XXX"):
            del tag[key]
            changed = True
            continue
        if key.startswith("UFID") and not frame.data:
            del tag[key]
            changed = True
            continue

        text = getattr(frame, "text", None)
        if isinstance(text, str):
            new_text, texts = fix_mojibake(text), None
            texts = [new_text]
        elif isinstance(text, list) and all(isinstance(t, str) for t in text):
            new_text = [fix_mojibake(t) for t in text]
            texts = new_text
        else:
            continue

        if new_text != text:
            log.info("ID3 %s: %r -> %r", key, text, new_text)
            frame.text = new_text
            changed = True

        encoding = getattr(frame, "encoding", None)
        if encoding is not None and any(not t.isascii() for t in texts):
            if (target_v23 and encoding != 1) or (not target_v23 and encoding == 0):
                frame.encoding = unicode_encoding
                changed = True

    if _has_id3v1(path):
        changed = True
    if not changed:
        return

    try:
        if target_v23:
            tag.update_to_v23()
        tag.save(path, v1=0, v2_version=3 if target_v23 else 4)
        log.info("Теги приведены в порядок: %s", path)
    except Exception as e:
        log.warning("Не удалось сохранить очищенные ID3-теги: %s: %s", path, e)


# ---------- прогресс и общий статус пачки ----------
class SpeedMeter:
    """Скорость и ETA по скользящему окну (а не по среднему с начала)."""

    def __init__(self):
        self.samples = deque()  # (время, байт)
        self.key = None

    def measure(self, state: TransferState, now: float) -> tuple:
        current = state.current
        total = max(state.total, 1)
        # Новая фаза/попытка → окно считаем заново (иначе после ретрая скорость «врёт»)
        key = (state.phase, state.attempt, state.phase_started_at)
        if key != self.key:
            self.key = key
            self.samples.clear()
            self.samples.append((state.phase_started_at, state.phase_start_current))
        self.samples.append((now, current))
        while len(self.samples) > 2 and self.samples[1][0] <= now - SPEED_WINDOW_SEC:
            self.samples.popleft()

        t0, c0 = self.samples[0]
        dt = now - t0
        speed = max((current - c0) / dt, 0.0) if dt > 0.5 else 0.0
        eta = (max(total - current, 0) / speed) if speed > 0 else float("inf")
        return speed, eta


@dataclass(eq=False)
class BatchItem:
    job: Job
    state: TransferState
    status: str = "queued"  # queued → running → ok / error
    replaced: bool = False
    where: str = ""  # папка на сервере (для сериалов)
    error: str = ""
    final_text: str = ""  # итоговый текст, когда пачка из одного файла
    meter: SpeedMeter = field(default_factory=SpeedMeter)

    def mark_ok(self, existed: bool, where: str) -> None:
        self.status = "ok"
        self.replaced = existed
        self.where = where
        note = "\n⚠️ Файл с таким именем уже был на сервере — заменён." if existed else ""
        name = self.job.filename
        if self.job.kind == "music":
            self.final_text = (
                f"✅ Готово: «{name}»\n"
                f"📁 Загружено в музыку — теги проставятся автоматически в течение ~10 минут.{note}"
            )
        else:
            place = f": {where}" if where else ""
            self.final_text = f"✅ Готово: «{name}»\n📁 Загружено в фильмы{place}.{note}"

    def mark_error(self, reason: str) -> None:
        self.status = "error"
        self.error = reason
        self.final_text = f"❌ Не удалось загрузить «{self.job.filename}»: {reason}."


# Открытые пачки по чатам. Файлы, пришедшие, пока в чате есть незавершённая пачка (и ещё BATCH_GRACE_SEC после
# неё), попадают в неё же: у альбома из 10 треков получается одно сообщение и один скан Jellyfin, а не десять.
BATCHES: dict = {}


class Batch:
    def __init__(self, chat_id: int, send):
        self.chat_id = chat_id
        self.send = send  # async (текст) -> сообщение; у возобновлённых заданий нет исходного события
        self.items: list = []
        self.status = None
        self.pending = 0
        self.closed = False
        self.lock = asyncio.Lock()
        self.stop = asyncio.Event()
        self.updater_task = None
        self.close_task = None

    # --- жизненный цикл ---
    def add(self, item: BatchItem) -> None:
        self.items.append(item)
        self.pending += 1
        if self.close_task is not None and not self.close_task.done():
            self.close_task.cancel()  # пришёл ещё файл — закрытие откладывается
        self.close_task = None

    def item_finished(self) -> None:
        self.pending -= 1
        if self.pending <= 0 and not self.closed and self.close_task is None:
            self.close_task = spawn(self._close_later())

    async def ensure_status(self) -> None:
        async with self.lock:
            if self.status is None:
                self.status = await self.send(fit_text(self.render()))
                self.updater_task = spawn(self._run_updater())

    async def _run_updater(self) -> None:
        last = None
        while True:
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=PROGRESS_EDIT_INTERVAL_SEC)
            except asyncio.TimeoutError:
                pass
            if self.stop.is_set():
                break
            text = fit_text(self.render())
            if text != last:
                try:
                    await self.status.edit(text)
                    last = text
                except Exception as e:
                    log.debug("Не удалось обновить прогресс: %s", e)

    async def _close_later(self) -> None:
        try:
            await asyncio.sleep(BATCH_GRACE_SEC)
        except asyncio.CancelledError:
            return
        if self.pending > 0 or self.closed:
            return
        self.closed = True  # дальше новые файлы пойдут уже в новую пачку
        if BATCHES.get(self.chat_id) is self:
            del BATCHES[self.chat_id]
        try:
            await self.finish()
        except Exception:
            log.exception("Не удалось завершить пачку")

    async def finish(self) -> None:
        self.stop.set()
        if self.updater_task is not None:
            with contextlib.suppress(Exception):
                await self.updater_task  # чтобы он не затёр итог старым прогрессом
        if self.status is None:
            return
        suffix = ""
        if any(i.status == "ok" and i.job.kind == "movie" for i in self.items):
            scan = await refresh_jellyfin()  # один скан на всю пачку
            if scan is True:
                suffix = "\n🔄 Сканирование Jellyfin запущено."
            elif scan is False:
                suffix = "\n⚠️ Jellyfin не ответил — запустите сканирование библиотеки вручную."
        await safe_edit(self.status, fit_text(self.render(final_suffix=suffix)))

    # --- отрисовка ---
    def render(self, final_suffix: str = "") -> str:
        now = time.monotonic()
        items = list(self.items)
        if len(items) == 1:
            item = items[0]
            return self._render_single(item, now) + (final_suffix if item.status == "ok" else "")

        total = len(items)
        ok = sum(1 for i in items if i.status == "ok")
        bad = sum(1 for i in items if i.status == "error")
        all_done = ok + bad == total
        head = f"📦 Готово: {ok} из {total}" if all_done else f"📦 Загружаю файлы: {total} · готово {ok}"
        if bad:
            head += f" · ошибок {bad}"

        if total > MAX_BATCH_LINES:
            lines = [f"✅ Загружено: {ok}"] + [self._line(i, now) for i in items if i.status != "ok"][:MAX_BATCH_LINES]
        else:
            lines = [self._line(i, now) for i in items]
        text = head + "\n" + "\n".join(lines)

        if all_done:
            notes = []
            if any(i.status == "ok" and i.job.kind == "music" for i in items):
                notes.append("📁 Музыка: теги проставятся автоматически в течение ~10 минут.")
            if any(i.status == "ok" and i.job.kind == "movie" for i in items):
                notes.append("📁 Видео загружено в фильмы.")
            if notes:
                text += "\n\n" + "\n".join(notes)
            text += final_suffix
        return text

    def _render_single(self, item: BatchItem, now: float) -> str:
        job, st = item.job, item.state
        if item.status == "queued":
            size_note = f" ({job.size / 1024 / 1024:.0f} МБ)" if job.size else ""
            return f"⏳ Принял «{job.filename}»{size_note}, жду очереди…"
        if item.status in ("ok", "error"):
            return item.final_text
        if st.phase == "convert":
            return f"🎧 Достаю звук из видео «{job.filename}»…"
        if st.phase == "upload" and st.current >= st.total:
            return f"📤 «{job.filename}» отправлен — жду подтверждения сервера и проверяю размер…"
        speed, eta = item.meter.measure(st, now)
        total = max(st.total, 1)
        pct = min(100, int(st.current / total * 100))
        icon = "📥" if st.phase == "download" else "📤"
        label = "Скачиваю" if st.phase == "download" else "Загружаю на сервер"
        retries = UPLOAD_RETRIES if st.phase == "upload" else DOWNLOAD_RETRIES
        attempt_note = f" (попытка {st.attempt}/{retries})" if st.attempt > 1 else ""
        pause_note = "\n⏸ Жду перед повторной попыткой…" if st.waiting else ""
        return (
            f"{icon} {label} «{job.filename}»{attempt_note}\n"
            f"[{make_progress_bar(pct)}] {pct}%\n"
            f"{format_size(st.current)} / {format_size(total)} · {format_speed(speed)} · ~{format_eta(eta)}"
            f"{pause_note}"
        )

    def _line(self, item: BatchItem, now: float) -> str:
        name = short_name(item.job.filename)
        st = item.state
        if item.status == "queued":
            return f"⏳ {name}"
        if item.status == "ok":
            line = f"✅ {name}"
            if item.where:
                line += f" → {short_name(item.where, 50)}"
            if item.replaced:
                line += " (заменён)"
            return line
        if item.status == "error":
            return f"❌ {name} — {item.error}"
        if st.phase == "convert":
            return f"🎧 {name} — достаю звук"
        if st.phase == "upload" and st.current >= st.total:
            return f"📤 {name} — проверяю на сервере"
        speed, _eta = item.meter.measure(st, now)
        pct = min(100, int(st.current / max(st.total, 1) * 100))
        icon = "📥" if st.phase == "download" else "📤"
        pause = " ⏸" if st.waiting else ""
        return f"{icon} {name} — {pct}% · {format_speed(speed)}{pause}"


def join_batch(chat_id: int, send, item: BatchItem) -> Batch:
    batch = BATCHES.get(chat_id)
    if batch is None or batch.closed:
        batch = Batch(chat_id, send)
        BATCHES[chat_id] = batch
    batch.add(item)
    return batch


async def with_stall_watchdog(coro, state: TransferState):
    """Выполняет coro; если прогресс не меняется дольше STALL_TIMEOUT_SEC — отменяет её и бросает StallError.
    Вызывается на КАЖДУЮ попытку (скачать / отправить), а не на всю передачу: StallError повторяемая,
    поэтому зависшая попытка просто перезапускается (скачивание — с докачкой), а не валит передачу целиком.
    Так подвисший Telegram/copyparty не держит слот в семафоре вечно."""
    if STALL_TIMEOUT_SEC <= 0:
        return await coro

    task = asyncio.ensure_future(coro)
    last_current = state.current
    last_move = time.monotonic()
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=WATCHDOG_TICK_SEC)
            if done:
                return task.result()
            now = time.monotonic()
            # Данные ушли целиком и мы ждём ответ сервера — это ограничивает read-таймаут httpx.
            awaiting_response = state.phase == "upload" and state.current >= state.total
            if state.current != last_current or state.waiting or awaiting_response:
                last_current = state.current
                last_move = now
                continue
            if now - last_move > STALL_TIMEOUT_SEC:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
                raise StallError(STALL_TIMEOUT_SEC)
    finally:
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task


# ---------- скачивание из Telegram (параллельно, с докачкой) ----------
async def refresh_message(message):
    """Перечитываем сообщение: file reference в старом объекте мог протухнуть."""
    try:
        fresh = await asyncio.wait_for(client.get_messages(message.chat_id, ids=message.id), timeout=30)
        return fresh or message
    except Exception as e:
        log.debug("Не удалось обновить сообщение: %s", e)
        return message


def read_resume_point(local_path: Path, doc) -> int:
    """Сколько байт с НАЧАЛА файла уже точно скачано (кратно DOWNLOAD_SEGMENT либо весь файл).
    Сегменты качаются параллельно и ложатся в файл вразнобой, поэтому по размеру файла о докачке судить нельзя —
    надёжная граница хранится в «паспорте» рядом с файлом."""
    try:
        data = json.loads(meta_path(local_path).read_text(encoding="utf-8"))
        have = local_path.stat().st_size
        done = int(data.get("done", 0))
    except (OSError, ValueError, TypeError):
        return 0
    total = doc.size or 0
    if (
        data.get("doc_id") != doc.id or data.get("size") != total
        or data.get("segment") != DOWNLOAD_SEGMENT or done < 0 or done > total or done > have
    ):
        return 0
    return done


def write_resume_point(local_path: Path, doc, done: int) -> None:
    payload = {"doc_id": doc.id, "size": doc.size or 0, "segment": DOWNLOAD_SEGMENT, "done": done}
    target = meta_path(local_path)
    tmp = target.with_name(target.name + ".tmp")
    try:
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, target)
    except OSError as e:
        log.debug("Не удалось записать прогресс докачки: %s", e)


async def _pwrite_all(fd: int, data: bytes, pos: int) -> None:
    view = memoryview(data)
    while len(view):
        written = await run_blocking(os.pwrite, fd, view, pos)
        view = view[written:]
        pos += written


async def download_unknown_size(message, local_path: Path, state: TransferState, attempt: int) -> None:
    """Запасной путь: Telegram не сообщил размер документа — качаем одним потоком без докачки."""
    remove_quietly(meta_path(local_path))
    state.begin("download", attempt, current=0, total=1)
    with open(local_path, "wb") as f:
        async for chunk in client.iter_download(message.document, request_size=DOWNLOAD_REQUEST_SIZE):
            await run_blocking(f.write, chunk)
            state.current += len(chunk)
            state.total = max(state.total, state.current)


async def download_resumable(message, local_path: Path, state: TransferState, attempt: int) -> None:
    """Качает документ сегментами по DOWNLOAD_SEGMENT, DOWNLOAD_WORKERS штук одновременно, и продолжает с места
    обрыва: в «паспорте» (файл .meta) хранится граница, до которой все сегменты подряд скачаны полностью.
    Это работает и между попытками, и между перезапусками бота."""
    doc = message.document
    total = doc.size or 0
    if total <= 0:
        await download_unknown_size(message, local_path, state, attempt)
        return

    nseg = (total + DOWNLOAD_SEGMENT - 1) // DOWNLOAD_SEGMENT
    done = read_resume_point(local_path, doc)
    first = nseg if done >= total else done // DOWNLOAD_SEGMENT
    if done:
        log.info("Докачиваю «%s» с %s из %s", state.filename, format_size(done), format_size(total))
    else:
        remove_quietly(local_path)  # без паспорта содержимому не верим

    state.begin("download", attempt, current=done, total=total)
    state.disk_reserved = max(total - done, 0)

    pending = deque(range(first, nseg))
    finished = set()
    prefix = first  # сегменты [0, prefix) скачаны целиком
    persist_lock = asyncio.Lock()

    async def persist() -> None:
        async with persist_lock:
            await run_blocking(write_resume_point, local_path, doc, min(prefix * DOWNLOAD_SEGMENT, total))

    fd = os.open(local_path, os.O_WRONLY | os.O_CREAT, 0o644)
    try:
        async def fetch_segment(idx: int) -> None:
            start = idx * DOWNLOAD_SEGMENT
            end = min(start + DOWNLOAD_SEGMENT, total)
            chunks = (end - start + DOWNLOAD_REQUEST_SIZE - 1) // DOWNLOAD_REQUEST_SIZE
            pos = start
            async for chunk in client.iter_download(
                doc, offset=start, limit=chunks, request_size=DOWNLOAD_REQUEST_SIZE, file_size=total
            ):
                chunk = chunk[: end - pos]
                if not chunk:
                    break
                await _pwrite_all(fd, chunk, pos)
                pos += len(chunk)
                state.current += len(chunk)
                state.disk_reserved = max(total - state.current, 0)
                if pos >= end:
                    break
            if pos != end:
                raise TransferError(
                    f"Telegram отдал неполный фрагмент файла ({format_size(pos - start)} из {format_size(end - start)})",
                    retriable=True,
                )

        async def worker() -> None:
            nonlocal prefix
            while pending:
                idx = pending.popleft()
                await fetch_segment(idx)
                finished.add(idx)
                advanced = False
                while prefix in finished:
                    finished.discard(prefix)
                    prefix += 1
                    advanced = True
                if advanced:
                    await persist()

        tasks = [asyncio.create_task(worker()) for _ in range(min(DOWNLOAD_WORKERS, len(pending)))]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            with contextlib.suppress(Exception):
                await persist()  # то, что успели докачать подряд, сохраняем для следующей попытки
            raise
    finally:
        os.close(fd)

    state.disk_reserved = 0
    got = local_path.stat().st_size
    if got != total:
        remove_quietly(local_path)  # битый результат — следующая попытка начнёт с нуля
        remove_quietly(meta_path(local_path))
        raise TransferError(
            f"скачанный файл неполный ({format_size(got)} из {format_size(total)})", retriable=True
        )
    # Паспорт с «done = всё» остаётся рядом: если бот упадёт во время отправки на сервер, после перезапуска
    # файл не придётся качать заново. Уберёт его уже завершённое задание (drop_job_files).
    await run_blocking(write_resume_point, local_path, doc, total)


async def download_with_retry(message, local_path: Path, state: TransferState) -> None:
    """Скачивание из Telegram с повторными попытками.
    * Каждая попытка идёт под своим watchdog-ом: зависание — это повторяемая ошибка. Недокачанный файл
      остаётся на диске, и следующая попытка продолжает с последней сохранённой границы.
    * FloodWait ждём ровно столько, сколько просит Telegram, не тратя обычный лимит попыток, но суммарно
      не дольше MAX_FLOOD_WAIT_SEC — на время ожидания мы занимаем слот очереди."""
    last_exc = None
    attempt = 0
    flood_waits = 0
    flood_budget = MAX_FLOOD_WAIT_SEC
    need_refresh = False  # после сбоя/долгого ожидания file reference мог протухнуть
    while attempt < DOWNLOAD_RETRIES:
        attempt += 1
        state.attempt = attempt
        try:
            if need_refresh:
                message = await refresh_message(message)
            await with_stall_watchdog(download_resumable(message, local_path, state, attempt), state)
            return
        except asyncio.CancelledError:
            raise
        except FloodWaitError as e:
            flood_waits += 1
            wait = e.seconds + 1
            if flood_waits > MAX_FLOOD_WAITS or wait > flood_budget:
                raise TransferError(
                    f"Telegram просит подождать {e.seconds} сек (FloodWait) — слишком долго или слишком "
                    f"часто, попробуйте позже"
                ) from e
            flood_budget -= wait
            log.warning(
                "Telegram просит подождать %s сек (FloodWait), ждём и пробуем снова (бюджет ожидания: ещё %s сек)",
                e.seconds, flood_budget,
            )
            attempt -= 1  # честный FloodWait не считаем за неудачную попытку
            last_exc = e
            need_refresh = True
            await sleep_waiting(state, wait)
        except Exception as e:
            last_exc = e
            need_refresh = True
            log.warning("Попытка скачивания %s/%s не удалась: %s", attempt, DOWNLOAD_RETRIES, e)
            if attempt < DOWNLOAD_RETRIES:
                await sleep_waiting(state, RETRY_BACKOFF_SEC * attempt)
    raise last_exc


# ---------- загрузка на copyparty ----------
def remote_url(base: str, filename: str) -> str:
    """Имя (или путь «папка/подпапка/файл») обязательно кодируем по частям: иначе «#», «?», «%» в названии
    ломают URL, а «/» между папками должен остаться разделителем."""
    return f"{base.rstrip('/')}/" + "/".join(quote(part, safe="") for part in filename.split("/"))


async def upload_stream(path: Path, state: TransferState):
    sent = 0
    with open(path, "rb") as f:
        while True:
            chunk = await run_blocking(f.read, UPLOAD_CHUNK_SIZE)
            if not chunk:
                break
            sent += len(chunk)
            state.current = sent
            yield chunk


async def get_remote_size(url, user, pw, filename) -> Optional[int]:
    """Размер файла на сервере по HEAD.
    None — файла нет или проверить не удалось (например, у аккаунта нет прав на чтение);
    -1 — файл есть, но сервер не сообщил размер."""
    try:
        r = await http.head(remote_url(url, filename), auth=(user, pw), timeout=10)
    except httpx.HTTPError as e:
        log.warning("Не удалось проверить файл «%s» на сервере: %s", filename, e)
        return None
    if r.status_code != 200:
        return None
    length = r.headers.get("content-length")
    return int(length) if length and length.isdigit() else -1


def parse_retry_after(resp: httpx.Response) -> Optional[float]:
    value = resp.headers.get("retry-after")
    try:
        return min(float(value), 60.0) if value else None
    except ValueError:
        return None


# Права пользователя copyparty (из «?ls»): нужны, чтобы заранее понять, можно ли переименовывать/заменять файлы.
_PERMS_CACHE: dict = {}
# Параллельные загрузки в одно и то же имя выстраиваем в очередь (ключ — URL файла); записи живут,
# пока кто-то держит замок.
_NAME_LOCKS: "weakref.WeakValueDictionary" = weakref.WeakValueDictionary()
# Поиск/создание папок сериала — под одним замком, иначе две серии нового сериала, пришедшие одновременно,
# создадут две папки («Show» и «show»).
_SERIES_LOCK = asyncio.Lock()


def name_lock(key: str) -> asyncio.Lock:
    lock = _NAME_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _NAME_LOCKS[key] = lock
    return lock


def new_temp_name(final_name: str = "") -> str:
    """Временный файл лежит В ТОЙ ЖЕ папке, что и итоговый (иначе MOVE пойдёт между папками).
    Короткое ASCII-имя без точки в начале: не упирается в лимит длины и не прячется как dotfile.
    Расширение «.part» не распознают ни Jellyfin, ни индексатор тегов."""
    folder = final_name.rpartition("/")[0]
    leaf = f"upload-{uuid.uuid4().hex[:12]}.part"
    return f"{folder}/{leaf}" if folder else leaf


async def get_remote_perms(url, user, pw) -> Optional[frozenset]:
    """Права пользователя в этой папке copyparty: {'read','write','move','delete',...}.
    None — узнать не удалось (тогда действуем оптимистично и полагаемся на ответы сервера)."""
    now = time.monotonic()
    hit = _PERMS_CACHE.get(url)
    if hit and now - hit[0] < PERMS_TTL_SEC:
        return hit[1]
    perms = None
    try:
        resp = await http.get(f"{url.rstrip('/')}/?ls", auth=(user, pw), timeout=30)
        if resp.status_code == 200:
            raw = resp.json().get("perms")
            if isinstance(raw, list):
                perms = frozenset(str(p) for p in raw)
    except (httpx.HTTPError, ValueError) as e:
        log.debug("Не удалось узнать права на %s: %s", url, e)
    _PERMS_CACHE[url] = (now, perms)
    return perms


async def list_remote_dirs(url, user, pw, segments: list) -> Optional[list]:
    """Имена подпапок в папке на сервере (по «?ls»). [] — папки нет; None — узнать не удалось."""
    path = "/".join(quote(s, safe="") for s in segments)
    base = f"{url.rstrip('/')}/" + (f"{path}/" if path else "")
    try:
        resp = await http.get(base + "?ls", auth=(user, pw), timeout=30)
    except httpx.HTTPError as e:
        log.warning("Не удалось получить список папок «%s»: %s", "/".join(segments) or "/", e)
        return None
    if resp.status_code == 404:
        return []
    if resp.status_code != 200:
        log.warning("Список папок «%s»: сервер вернул %s", "/".join(segments) or "/", resp.status_code)
        return None
    try:
        data = resp.json()
    except ValueError:
        return None
    names = []
    for entry in data.get("dirs") or []:
        href = entry.get("href") if isinstance(entry, dict) else entry
        if not isinstance(href, str):
            continue
        name = unquote(href).strip("/").rsplit("/", 1)[-1]
        if name:
            names.append(name)
    return names


async def ensure_remote_dirs(url, user, pw, segments: list) -> None:
    """Создаёт вложенные папки (WebDAV MKCOL). Сбой не фатален: возможно, copyparty создаст папки сам при
    загрузке, а если нет — загрузка честно упадёт со своей ошибкой."""
    path = ""
    for seg in segments:
        path = f"{path}/{seg}" if path else seg
        try:
            resp = await http.request(
                "MKCOL", remote_url(url, path) + "/", auth=(user, pw), timeout=HTTP_DEFAULT_TIMEOUT
            )
        except httpx.HTTPError as e:
            log.warning("Не удалось создать папку «%s»: %s: %s", path, type(e).__name__, e)
            return
        if resp.status_code in OK_MKCOL_STATUSES:
            continue
        log.warning(
            "Не удалось создать папку «%s» (%s %s) — проверьте, что у пользователя copyparty есть право write",
            path, resp.status_code, resp.text[:200],
        )
        return


async def resolve_series_dirs(url, user, pw, show: str, season: int) -> list:
    """Папки для серии: [«Название сериала», «Season 01»]. Если такой сериал уже лежит в другой записи
    («dune prophecy», «Dune: Prophecy», «Dune Prophecy (2024)») — берём существующую папку, а не заводим новую.
    То же для сезона («Season 1», «Сезон 01»). Недостающие папки создаются."""
    show = truncate_dirname(show)
    season_dir = f"Season {season:02d}"
    async with _SERIES_LOCK:
        show_dir = show
        found = False
        root = await list_remote_dirs(url, user, pw, [])
        if root:
            if show in root:
                found = True
            else:
                key = series_key(show)
                if key:
                    for name in sorted(root, key=str.casefold):
                        if series_key(name) == key:
                            show_dir, found = name, True
                            break
        if found:
            seasons = await list_remote_dirs(url, user, pw, [show_dir]) or []
            if season_dir not in seasons:
                for name in sorted(seasons, key=str.casefold):
                    if season_number(name) == season:
                        season_dir = name
                        break
        await ensure_remote_dirs(url, user, pw, [show_dir, season_dir])
    return [show_dir, season_dir]


async def delete_remote_quietly(url, user, pw, name) -> bool:
    """Best-effort удаление с сервера (чистим недозагруженные файлы). True — файла там больше нет."""
    try:
        resp = await http.delete(remote_url(url, name), auth=(user, pw), timeout=15)
    except httpx.HTTPError as e:
        log.warning("Не удалось удалить «%s» с сервера: %s", name, e)
        return False
    if resp.status_code in (200, 202, 204, 404):
        return True
    log.warning(
        "Не удалось удалить «%s» с сервера (%s) — без права delete у пользователя copyparty такие файлы "
        "придётся убирать вручную", name, resp.status_code,
    )
    return False


async def _put_file(url, user, pw, name, local_path: Path, file_size: int, state: TransferState) -> httpx.Response:
    """Один PUT под watchdog-ом: если отправка зависла, он бросит повторяемый StallError."""
    stream = upload_stream(local_path, state)
    try:
        return await with_stall_watchdog(
            http.put(
                remote_url(url, name),
                auth=(user, pw),
                content=stream,
                # Явно указываем размер, чтобы не уходить в Transfer-Encoding: chunked —
                # некоторые связки copyparty/прокси плохо с ним работают.
                headers={"Content-Length": str(file_size)},
                timeout=UPLOAD_TIMEOUT,
            ),
            state,
        )
    finally:
        await stream.aclose()


async def move_remote(url, user, pw, src_name, dst_name, expected_size: int) -> None:
    """Атомарно (WebDAV MOVE) ставит загруженный временный файл на место, заменяя существующий.
    Повторяет только само переименование — заново гнать файл ради этого незачем."""
    last_error: Optional[BaseException] = None
    for attempt in range(1, 4):
        try:
            resp = await http.request(
                "MOVE",
                remote_url(url, src_name),
                auth=(user, pw),
                headers={"Destination": remote_url(url, dst_name), "Overwrite": "T"},
                timeout=HTTP_DEFAULT_TIMEOUT,
            )
        except httpx.TransportError as e:
            last_error = e
            log.warning("Переименование: попытка %s/3 не удалась: %s: %s", attempt, type(e).__name__, e)
        else:
            code = resp.status_code
            if code in OK_MOVE_STATUSES:
                return
            if code == 404 and attempt > 1:
                # Предыдущий MOVE мог пройти, а ответ потеряться: источника уже нет, а цель на месте.
                if await get_remote_size(url, user, pw, dst_name) == expected_size:
                    return
            (log.warning if code in MOVE_DENIED_STATUSES else log.error)("MOVE failed: %s %s", code, resp.text[:300])
            if code in MOVE_DENIED_STATUSES:
                if "delete" in resp.text.lower():
                    raise TransferError(
                        "файл с таким именем уже есть на сервере, а заменить его нельзя: "
                        "у пользователя copyparty нет права delete"
                    )
                raise RenameUnsupported("copyparty не разрешает переименование (нужно право move)")
            last_error = TransferError(
                f"не удалось переименовать временный файл: {describe_status(code)}",
                retriable=code in RETRY_UPLOAD_STATUSES,
            )
            if not last_error.retriable:
                raise last_error
        if attempt < 3:
            await asyncio.sleep(2 * attempt)
    raise last_error


async def _upload_once(url, user, pw, put_name, final_name, local_path: Path, file_size: int, state: TransferState) -> None:
    """Одна попытка: PUT (во временное или сразу в итоговое имя), проверка размера, при необходимости MOVE.
    Успех — возврат; сбой — TransferError (retriable или нет) / httpx.TransportError."""
    resp = await _put_file(url, user, pw, put_name, local_path, file_size, state)
    if resp.status_code not in OK_UPLOAD_STATUSES:
        log.error("upload failed: %s %s", resp.status_code, resp.text[:300])
        raise TransferError(
            describe_status(resp.status_code),
            retriable=resp.status_code in RETRY_UPLOAD_STATUSES,
            retry_after=parse_retry_after(resp),
        )
    remote_size = await get_remote_size(url, user, pw, put_name)
    if remote_size is not None and remote_size >= 0 and remote_size != file_size:
        log.warning("Размер после загрузки не совпал: сервер=%s, локально=%s", remote_size, file_size)
        raise TransferError(
            f"размер на сервере ({format_size(remote_size)}) не совпал с отправленным ({format_size(file_size)})",
            retriable=True,
        )
    if put_name != final_name:
        await move_remote(url, user, pw, put_name, final_name, file_size)


async def upload_with_retry(url, user, pw, filename, local_path: Path, state: TransferState) -> bool:
    """Заливает файл на copyparty. Возвращает True, если файл с таким именем там уже был (и заменён).
    filename — путь относительно корня: «Файл.mp3» или «Сериал/Season 01/Файл.mkv» (папки уже должны существовать).

    Схема (UPLOAD_VIA_TEMP=1, по умолчанию): PUT во временный «upload-<id>.part» (в той же папке) → проверка
    размера → MOVE на итоговое имя. Так на сервере никогда не появляется обрезанный файл под настоящим именем,
    а существующий файл заменяется целиком или остаётся нетронутым. (Прямой PUT поверх существующего copyparty
    не перезаписывает, а создаёт копию «имя-<время>-<id>.ext»; обрыв оставляет обрезок, и повтор плодит копии.)
    Если прав move нет — честно грузим напрямую, предварительно убрав обрывки.
    Повторяем сетевые ошибки, зависания и временные статусы (5xx, 408, 429); остальные 4xx — сразу ошибка."""
    file_size = local_path.stat().st_size
    state.begin("upload", 1, current=0, total=file_size)
    last_error: Optional[BaseException] = None
    committed = False
    partial_name: Optional[str] = None  # что стереть с сервера, если загрузка не удалась
    force_direct = False

    # Одноимённые загрузки идут по очереди: иначе две копии пишут в один путь одновременно.
    async with name_lock(remote_url(url, filename)):
        existed = await get_remote_size(url, user, pw, filename) is not None
        if existed:
            log.warning("Файл «%s» уже есть на сервере — будет заменён", filename)

        perms = await get_remote_perms(url, user, pw) if UPLOAD_VIA_TEMP else None
        can_move = perms is None or "move" in perms
        can_delete = perms is None or "delete" in perms
        if UPLOAD_VIA_TEMP and existed and not (can_move and can_delete):
            raise TransferError(
                "файл с таким именем уже есть на сервере, а заменить его нельзя: "
                "у пользователя copyparty нет прав move и delete"
            )
        use_temp_ok = UPLOAD_VIA_TEMP and can_move
        if UPLOAD_VIA_TEMP and not can_move:
            log.warning("У пользователя copyparty нет права move — гружу напрямую, без временного файла")

        try:
            attempt = 0
            while attempt < UPLOAD_RETRIES:
                attempt += 1
                state.begin("upload", attempt, current=0, total=file_size)
                delay = RETRY_BACKOFF_SEC * attempt
                use_temp = use_temp_ok and not force_direct
                put_name = new_temp_name(filename) if use_temp else filename
                # Временный файл стираем всегда; «настоящий» — только если до нас его не было.
                partial_name = put_name if (use_temp or not existed) else None
                try:
                    await _upload_once(url, user, pw, put_name, filename, local_path, file_size, state)
                    committed = True
                    return existed
                except RenameUnsupported as e:
                    # Права, которые показал ?ls, не подтвердились (или поменялись).
                    _PERMS_CACHE.pop(url, None)
                    if existed:
                        raise TransferError(
                            "файл с таким именем уже есть на сервере, а заменить его нельзя: "
                            "у пользователя copyparty нет прав move и delete"
                        ) from e
                    log.warning("%s — перехожу на прямую загрузку", e)
                    force_direct = True
                    await delete_remote_quietly(url, user, pw, put_name)
                    partial_name = None
                    attempt -= 1  # это не «неудачная попытка», а смена способа
                    continue
                except TransferError as e:
                    if not e.retriable:
                        raise
                    last_error = e
                    delay = e.retry_after or delay
                    log.warning("Попытка загрузки %s/%s не удалась: %s", attempt, UPLOAD_RETRIES, e)
                except httpx.TransportError as e:
                    last_error = e
                    log.warning("Попытка загрузки %s/%s не удалась: %s: %s", attempt, UPLOAD_RETRIES, type(e).__name__, e)

                # Попытка не удалась. Обрывок надо убрать ДО повтора: copyparty не перезаписывает файл, а
                # создаёт рядом копию «имя-<время>-<id>.ext», и размер на сервере так и останется обрезком.
                if partial_name:
                    cleaned = await delete_remote_quietly(url, user, pw, partial_name)
                    if cleaned:
                        partial_name = None
                    elif not use_temp:
                        partial_name = None  # удалить не вышло, повторять бессмысленно
                        raise last_error  # повтор в то же имя только наплодит копий
                if attempt < UPLOAD_RETRIES:
                    await sleep_waiting(state, delay)
            raise last_error
        finally:
            if not committed and partial_name:
                await delete_remote_quietly(url, user, pw, partial_name)


async def refresh_jellyfin() -> Optional[bool]:
    """None — Jellyfin не настроен; True/False — получилось ли запустить скан.
    Сбой Jellyfin не должен превращать успешную загрузку в «ошибку», поэтому исключения глотаем."""
    if not JELLYFIN_API_KEY:
        return None
    try:
        r = await http.post(
            f"{JELLYFIN_URL}/Library/Refresh", params={"api_key": JELLYFIN_API_KEY}, timeout=15
        )
    except Exception as e:
        log.warning("Jellyfin не ответил на запрос сканирования: %s", e)
        return False
    ok = r.status_code in (200, 204)
    if not ok:
        log.warning("Jellyfin вернул %s на запрос сканирования", r.status_code)
    return ok


# ---------- музыкальные клипы: звук из видео ----------
_ARTIST_TITLE_RE = re.compile(r"^(.+?)\s+-\s+(.+)$")
_MOVIE_MARKS_RE = re.compile(r"\b(\d{3,4}[pi]|WEB-?DL|WEB-?Rip|BD-?Rip|HD-?Rip|DVD-?Rip|Blu-?Ray|HDTV|[xh]\.?26[45])\b", re.IGNORECASE)


def looks_like_music_video(stem: str, duration: float, size: int) -> bool:
    """Короткое видео с названием «Исполнитель - Название» и без киношных пометок (1080p, WEB-DL, S01E02) —
    скорее клип, чем фильм. Это не приговор: бот только спрашивает, куда класть."""
    if MUSIC_VIDEO_MAX_MIN <= 0 or not _ARTIST_TITLE_RE.match(stem):
        return False
    if _MOVIE_MARKS_RE.search(stem) or _EPISODE_IN_NAME_RE.search(stem):
        return False
    if duration and duration > 0:
        return duration <= MUSIC_VIDEO_MAX_MIN * 60
    return size <= MUSIC_VIDEO_UNKNOWN_DURATION_MAX_BYTES


async def _run_tool(cmd: list, timeout: int) -> tuple:
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
    except FileNotFoundError:
        raise TransferError(f"в образе бота нет «{cmd[0]}» — установите ffmpeg, чтобы доставать звук из клипов") from None
    try:
        raw, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        await proc.wait()
        raise TransferError(f"«{cmd[0]}» не уложился в {timeout} сек") from None
    return proc.returncode, raw.decode("utf-8", errors="replace").strip()


async def extract_audio(src: Path, dst: Path, filename: str) -> None:
    """Достаёт звук из видео в .m4a. AAC копируется без перекодирования, остальное кодируется в AAC 256 кбит/с.
    Исполнитель и название берутся из имени «Исполнитель - Название», чтобы beets не импортировал трек без тегов."""
    rc, out = await _run_tool(
        [FFPROBE_BIN, "-v", "error", "-select_streams", "a:0", "-show_entries", "stream=codec_name",
         "-of", "csv=p=0", str(src)],
        timeout=60,
    )
    codec = out.splitlines()[0].strip().lower() if rc == 0 and out else ""
    if not codec:
        raise TransferError("в этом видео нет звуковой дорожки")
    audio_args = ["-c:a", "copy"] if codec == "aac" else ["-c:a", "aac", "-b:a", "256k"]

    cmd = [FFMPEG_BIN, "-nostdin", "-y", "-v", "error", "-i", str(src), "-vn", "-map", "0:a:0", *audio_args]
    m = _ARTIST_TITLE_RE.match(Path(filename).stem)
    if m:
        cmd += ["-metadata", f"artist={m.group(1).strip()}", "-metadata", f"title={m.group(2).strip()}"]
    cmd += ["-movflags", "+faststart", "-f", "ipod", str(dst)]

    rc, out = await _run_tool(cmd, timeout=FFMPEG_TIMEOUT_SEC)
    if rc != 0 or not dst.exists() or dst.stat().st_size == 0:
        remove_quietly(dst)
        raise TransferError(f"не удалось достать звук (ffmpeg: {out[-200:] or 'код ' + str(rc)})")


# ---------- обработка одного файла ----------
async def transfer_file(message, job: Job, local_path: Path, state: TransferState) -> tuple:
    """Скачать → (клип) достать звук → (для mp3) починить теги → (сериал) найти/создать папки → залить.
    Возвращает (файл_с_таким_именем_уже_был, папка_на_сервере_или_пустая_строка)."""
    upload_path = local_path
    if job.url:
        upload_path = await download_from_link(job, local_path, state)
    else:
        already = read_resume_point(local_path, message.document) if local_path.exists() else 0
        ensure_disk_space(max((message.document.size or 0) - already, 0), state)
        await download_with_retry(message, local_path, state)
    audio_path = local_path.with_suffix(".m4a")
    try:
        if job.extract:
            state.begin("convert", 1, current=0, total=1)
            await extract_audio(local_path, audio_path, job.filename)
            upload_path = audio_path

        if job.kind == "music" and Path(job.filename).suffix.lower() == ".mp3":
            await run_blocking(fix_mp3_tags, str(upload_path))

        url, user, pw = TARGETS[job.kind]
        remote_name = job.filename
        where = ""
        if job.subtitle:
            await run_blocking(normalize_subtitle_file, str(upload_path))
            remote_name, where = await resolve_subtitle_target(url, user, pw, job)
        elif job.kind == "movie" and job.series:
            # Папки ищем/создаём только сейчас, перед отправкой: за время скачивания их мог создать другой файл.
            dirs = await resolve_series_dirs(url, user, pw, job.series["show"], int(job.series["season"]))
            remote_name = "/".join(dirs + [job.filename])
            where = "/".join(dirs)
        existed = await upload_with_retry(url, user, pw, remote_name, upload_path, state)
        return existed, where, remote_name
    finally:
        remove_quietly(audio_path)


async def run_job(job: Job, message, send) -> None:
    """Ставит файл в общую пачку, ждёт своей очереди, переносит и записывает результат.
    Временные файлы и запись в журнале удаляются, только когда задание ДОВЕДЕНО до конца (успехом или
    окончательной ошибкой). При остановке бота они остаются — после запуска задание продолжится."""
    item = BatchItem(job=job, state=TransferState(filename=job.filename, total=job.size or 1))
    batch = join_batch(job.chat_id, send, item)
    finished = False
    try:
        await batch.ensure_status()
        async with UPLOAD_SEMAPHORE:
            item.status = "running"
            ACTIVE_TRANSFERS.add(item.state)
            try:
                # Watchdog на зависания работает внутри transfer_file — на каждой попытке скачивания/отправки.
                existed, where, remote_name = await transfer_file(message, job, job.local_path, item.state)
                item.mark_ok(existed, where)
                await record_history(job, remote_name, existed)
            except Exception as e:
                log.exception("upload failed")
                item.mark_error(describe_error(e))
            finally:
                ACTIVE_TRANSFERS.discard(item.state)
        finished = True
    finally:
        if finished:
            drop_job_files(job)
        batch.item_finished()


# ---------- вопросы пользователю (кнопки у бота-помощника или текстовый ответ) ----------
KIND_LABELS = {"music": "🎵 музыку", "movie": "🎬 фильмы"}
KIND_ALIASES = {
    "1": "music", "м": "music", "музыка": "music", "music": "music",
    "2": "movie", "ф": "movie", "фильм": "movie", "видео": "movie", "movie": "movie",
    "0": "cancel", "отмена": "cancel", "нет": "cancel", "cancel": "cancel",
}
VIDEO_KIND_ALIASES = {
    "1": "movie", "ф": "movie", "фильм": "movie", "movie": "movie",
    "2": "series", "с": "series", "сериал": "series", "series": "series",
    "0": "cancel", "отмена": "cancel", "нет": "cancel", "cancel": "cancel",
}
YES_ALIASES = {
    "да": "yes", "д": "yes", "yes": "yes", "y": "yes", "1": "yes",
    "нет": "cancel", "н": "cancel", "no": "cancel", "n": "cancel", "0": "cancel",
    "отмена": "cancel", "cancel": "cancel",
}
CANCEL_WORDS = {"0", "отмена", "cancel"}


def sender_allowed(sender_id) -> bool:
    return not ALLOWED_SENDER_IDS or sender_id in ALLOWED_SENDER_IDS


def resolve_text_choice(event) -> bool:
    """Текстовый ответ на вопрос бота (reply на его сообщение). True — ответ принят."""
    if not event.is_reply or not event.raw_text:
        return False
    entry = PENDING_CHOICES.get(("reply", event.chat_id, event.reply_to_msg_id))
    if entry is None:
        return False
    fut, aliases = entry
    text = event.raw_text.strip()
    if aliases is None:
        # ждём произвольный текст; отмена — пустая строка (None оставляем для «не дождались»)
        value = "" if text.lower() in CANCEL_WORDS else (text or None)
    else:
        value = aliases.get(text.lower())
    if value is None or fut.done():
        return False
    fut.set_result(value)
    return True


def align_extension(filename: str, ext: str, kind: str, mime_type: Optional[str]) -> tuple:
    """После ручного выбора: если расширение не подходит выбранному типу, меняем его на mime-овое
    (только если оно подходит); иначе оставляем как есть — не выдаём неизвестный файл за «.mp3»."""
    allowed = AUDIO_EXTS if kind == "music" else (VIDEO_EXTS | AMBIGUOUS_VIDEO_EXTS)
    if ext.lower() in allowed:
        return filename, ext
    new_ext = ext_from_mime(mime_type)
    if new_ext in allowed:
        return filename[: -len(ext)] + new_ext, new_ext
    return filename, ext


async def ask_choice(event, question: str, rows: list, aliases: dict, hint: str, done_note) -> Optional[str]:
    """Задаёт вопрос и ждёт выбор. rows — кнопки [[(подпись, значение), …], …]; aliases — какие текстовые
    ответы что значат; done_note(значение или None) — итоговый текст вопроса. Возвращает выбранное значение
    или None, если ответа не дождались. С ботом-помощником шлёт кнопки; текстовый ответ работает всегда."""
    fut = asyncio.get_running_loop().create_future()
    token = uuid.uuid4().hex[:10]
    keys = []
    helper_msg = None

    if bot_client is not None:
        try:
            helper_msg = await bot_client.send_message(
                event.sender_id,
                question,
                buttons=[
                    [Button.inline(label, f"ask:{token}:{value}".encode()) for label, value in row]
                    for row in rows
                ],
            )
            keys.append(("cb", token))
        except Exception as e:
            # Чаще всего — вы ещё не нажимали /start у бота-помощника, он не может вам писать.
            log.warning("Бот-помощник не смог отправить кнопки (нажмите в нём /start): %s", e)

    how = "Нажмите кнопку в чате с ботом-помощником или ответьте" if helper_msg else "Ответьте на это сообщение"
    prompt = await event.reply(f"{question}\n{how}: {hint}.")
    keys.append(("reply", event.chat_id, prompt.id))
    for key in keys:
        PENDING_CHOICES[key] = (fut, aliases)

    try:
        choice = await asyncio.wait_for(fut, timeout=CHOICE_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        choice = None
    finally:
        for key in keys:
            PENDING_CHOICES.pop(key, None)

    note = done_note(choice)
    await safe_edit(prompt, note)
    if helper_msg is not None:
        with contextlib.suppress(Exception):
            await helper_msg.edit(note)  # заодно убирает кнопки
    return choice


async def ask_text(event, question: str, hint: str) -> Optional[str]:
    """Просит ввести текст (ответом на сообщение бота). Возвращает текст; «» — пользователь отменил;
    None — не дождались ответа."""
    fut = asyncio.get_running_loop().create_future()
    prompt = await event.reply(f"{question}\nОтветьте на это сообщение: {hint}. (0 — отмена)")
    key = ("reply", event.chat_id, prompt.id)
    PENDING_CHOICES[key] = (fut, None)
    try:
        answer = await asyncio.wait_for(fut, timeout=CHOICE_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        answer = None
    finally:
        PENDING_CHOICES.pop(key, None)

    if answer is None:
        note = "⌛ Ответа не дождался — файл пропущен."
    elif answer == "":
        note = "✖️ Отменено."
    else:
        note = f"✅ Принял: {answer}"
    await safe_edit(prompt, note)
    return answer


def _kind_note(choice: Optional[str]) -> str:
    if choice in KIND_LABELS:
        return f"✅ Понял, кладу в {KIND_LABELS[choice]}."
    if choice is None:
        return "⌛ Ответа не дождался — файл пропущен."
    return "✖️ Отменено."


def _video_kind_note(choice: Optional[str]) -> str:
    if choice == "movie":
        return "✅ Понял: это фильм."
    if choice == "series":
        return "✅ Понял: это серия сериала."
    if choice is None:
        return "⌛ Ответа не дождался — файл пропущен."
    return "✖️ Отменено."


async def ask_kind(event, filename: str) -> Optional[str]:
    """Музыка или фильм? Возвращает 'music' / 'movie' или None (отмена / нет ответа)."""
    choice = await ask_choice(
        event,
        f"🤔 Не понимаю, что это за файл: «{filename}».\nКуда его положить?",
        [[("🎵 Музыка", "music"), ("🎬 Фильм", "movie")], [("✖️ Отмена", "cancel")]],
        KIND_ALIASES,
        "1 — музыка, 2 — фильм, 0 — отмена",
        _kind_note,
    )
    return choice if choice in KIND_LABELS else None


async def plan_video(event, orig_filename: str, ext: str, parsed: Optional[dict], filename: str):
    """Фильм это или серия сериала. Если из имени и подписи не понять — спрашивает пользователя.
    Возвращает (series, имя_файла): series — {"show", "season"} для серии, None для фильма;
    либо None, если файл надо пропустить (отмена, нет ответа, ответ не разобрали)."""
    info = detect_series(orig_filename, parsed)

    if info is None and is_generic_name(Path(filename).stem):
        # Ни имя файла, ни подпись ничего не говорят («video_123.mp4») — что это, пусть скажет пользователь.
        choice = await ask_choice(
            event,
            f"🤔 Не понимаю, что за видео: «{orig_filename}».\nЭто фильм или серия сериала?",
            [[("🎬 Фильм", "movie"), ("📺 Сериал", "series")], [("✖️ Отмена", "cancel")]],
            VIDEO_KIND_ALIASES,
            "1 — фильм, 2 — сериал, 0 — отмена",
            _video_kind_note,
        )
        if choice == "movie":
            text = await ask_text(
                event, f"🎬 Как называется фильм? (файл «{orig_filename}»)",
                "название, лучше с годом — например: Dune (2021)",
            )
            name = sanitize_name(text or "")
            if not name:
                return None
            return None, truncate_filename(name + ext)
        if choice == "series":
            text = await ask_text(
                event, f"📺 Что это за серия? (файл «{orig_filename}»)",
                "название, сезон и серия — например: Dune Prophecy S01E02",
            )
            parsed_answer = parse_episode_from_name(normalize_text(text or ""))
            if not parsed_answer or not parsed_answer["show"] or is_generic_name(parsed_answer["show"]):
                if text:
                    await event.reply("🤷 Не разобрал название, сезон и серию (нужен вид «Название S01E02») — файл пропущен.")
                return None
            info = {
                "show": parsed_answer["show"], "season": parsed_answer["season"],
                "episode": parsed_answer["episode"], "episode_title": None, "quality": None, "name_ok": False,
            }
        else:
            return None

    if info is None:
        return None, filename  # обычный фильм

    show = info["show"]
    if not show or is_generic_name(show):
        text = await ask_text(
            event,
            f"📺 «{orig_filename}» — серия сериала (сезон {info['season']}, серия {info['episode']}), "
            f"но название сериала я не нашёл.",
            "название сериала — например: Dune Prophecy",
        )
        show = clean_show_name(text or "")
        if not show or is_generic_name(show):
            if text:
                await event.reply("🤷 Не разобрал название сериала — файл пропущен.")
            return None
        info["name_ok"] = False

    info["show"] = show
    leaf = orig_filename if info["name_ok"] else build_series_filename(info, ext)
    return {"show": show, "season": info["season"], "episode": info["episode"]}, truncate_filename(leaf)


async def on_choice_button(event):
    """Нажатие кнопки у бота-помощника."""
    if not sender_allowed(event.sender_id):
        await event.answer("Нет доступа", alert=True)
        return
    try:
        _, token, value = event.data.decode().split(":", 2)
    except ValueError:
        return
    entry = PENDING_CHOICES.get(("cb", token))
    if entry is None or entry[0].done():
        await event.answer("Этот вопрос уже неактуален")
        return
    entry[0].set_result(value)
    await event.answer("Принято")


async def on_helper_start(event):
    if event.is_private and sender_allowed(event.sender_id):
        await event.reply("Готово ✅ Когда юзербот не поймёт тип файла, кнопки придут сюда.")


async def start_buttons_bot() -> None:
    """Запускает бота-помощника, если задан токен. Сбой не мешает основной работе — останется текстовый вопрос."""
    global bot_client
    if not BUTTONS_BOT_TOKEN:
        return
    candidate = TelegramClient(
        SESSION_PATH + "_buttons", API_ID, API_HASH,
        connection_retries=-1, retry_delay=2, auto_reconnect=True,
    )
    candidate.parse_mode = None
    candidate.add_event_handler(on_choice_button, events.CallbackQuery(pattern=rb"^ask:"))
    candidate.add_event_handler(on_helper_start, events.NewMessage(pattern=r"^/start", incoming=True))
    try:
        await candidate.start(bot_token=BUTTONS_BOT_TOKEN)
        me = await candidate.get_me()
    except Exception as e:
        log.warning("Бот-помощник не запустился, вопросы будут текстовыми: %s", e)
        with contextlib.suppress(Exception):
            await candidate.disconnect()
        return
    bot_client = candidate
    log.info("Бот-помощник для кнопок запущен: @%s", me.username)


# ---------- управление музыкальной библиотекой (через beets) ----------
# Бот вызывает консольный «beet» с тем же конфигом, что и импорт, поэтому база beets и файлы не расходятся.
# Нужно: beets в образе бота (ТОЙ ЖЕ версии, что и в контейнере импорта) и общие папки библиотеки и конфига,
# смонтированные по тем же путям (в базе лежат абсолютные пути).
class LibraryError(Exception):
    """Понятная пользователю причина сбоя команды библиотеки."""


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
LIB_SEP = "\x1f"
# В beets 2.x жанры — список в поле «genres» (поле «genre» осталось только как синоним при записи).
LIB_KEYS = ("id", "artist", "title", "album", "genres", "year", "format", "bitrate", "path")
LIB_FORMAT = LIB_SEP.join(f"${k}" for k in LIB_KEYS)
LIB_SETTABLE = ("artist", "title", "album", "albumartist", "genre", "year")  # что разрешено менять через /set
LIB_FIELD_ALIAS = {"genre": "genres"}
LIB_MAX_LIST = 25
LIB_MAX_IDS = 20

LIB_HELP = (
    "🎵 Управление библиотекой:\n"
    "/find <запрос> — поиск (например: /find scatman)\n"
    "/last [N] — последние добавленные\n"
    "/info <id> — подробности о треке\n"
    "/rename <id> Исполнитель - Название — переименовать (файл переедет сам)\n"
    "/set <id> поле=значение … — изменить теги: artist, title, album, albumartist, genre, year\n"
    "/del <id> [id …] — удалить треки вместе с файлами (спрошу подтверждение)\n"
    "/stats — статистика библиотеки\n"
    "/rescan — пересканировать Navidrome\n"
    "/history [N] — последние загрузки\n"
    "/undo [номер] — отменить загрузку (удалить файл с сервера)\n"
    "\n🔗 Пришлите ссылку (YouTube, YouTube Music, SoundCloud, Bandcamp …) — скачаю аудио (.m4a) или видео; "
    "плейлисты и альбомы — целиком. «--force» в сообщении — загрузить, даже если такое уже есть.\n"
    "💬 Субтитры (.srt .ass .vtt …) кладу рядом с видео под тем же именем."
)


async def run_beet(*args: str) -> str:
    """Запускает «beet» с нашим конфигом, возвращает вывод. Ошибки → LibraryError.
    Пользовательский текст передаётся только отдельными аргументами (без shell) и после «--» там, где это запросы."""
    cmd = [BEETS_BIN, "-c", BEETS_CONFIG, "-p", BEETS_PLUGINS, *args]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env={**os.environ, "NO_COLOR": "1"},
        )
    except FileNotFoundError:
        raise LibraryError("в контейнере бота не установлен beets (команда «%s» не найдена)" % BEETS_BIN) from None
    try:
        raw, _ = await asyncio.wait_for(proc.communicate(), timeout=BEETS_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        await proc.wait()
        raise LibraryError(f"beets не ответил за {BEETS_TIMEOUT_SEC} сек") from None
    text = _ANSI_RE.sub("", raw.decode("utf-8", errors="replace"))
    lines = [ln for ln in text.splitlines() if not ln.startswith("Created database backup")]
    if proc.returncode != 0:
        tail = "\n".join(lines).strip()[-300:]
        log.warning("beet %s -> код %s: %s", " ".join(args[:2]), proc.returncode, tail)
        raise LibraryError(tail or f"beets завершился с кодом {proc.returncode}")
    return "\n".join(lines)


def parse_items(output: str) -> list:
    items = []
    for line in output.splitlines():
        parts = line.split(LIB_SEP)
        if len(parts) == len(LIB_KEYS) and parts[0].isdigit():
            items.append(dict(zip(LIB_KEYS, parts)))
    return items


async def find_items(terms: list) -> list:
    # «--»: всё дальше — запрос, даже если слово начинается с «-».
    return parse_items(await run_beet("ls", "-f", LIB_FORMAT, "--", *terms))


async def get_item(item_id: int) -> Optional[dict]:
    found = await find_items([f"id:{item_id}"])
    return found[0] if found else None


def split_args(text: str) -> list:
    try:
        return shlex.split(text)
    except ValueError:  # незакрытая кавычка
        return text.split()


def parse_ids(text: str) -> list:
    ids = []
    for tok in re.split(r"[\s,]+", text.strip()):
        if not tok:
            continue
        if not tok.isdigit():
            raise LibraryError(f"«{tok}» — это не id трека (id — число из /find или /last)")
        if int(tok) not in ids:
            ids.append(int(tok))
    if len(ids) > LIB_MAX_IDS:
        raise LibraryError(f"за раз можно не больше {LIB_MAX_IDS} треков")
    return ids


def item_line(it: dict) -> str:
    album = f" · {it['album']}" if it["album"] else ""
    return f"#{it['id']} · {it['artist'] or '—'} — {it['title'] or '—'}{album}"


def format_items(items: list, header: str) -> str:
    if not items:
        return "🤷 Ничего не найдено."
    shown = items[:LIB_MAX_LIST]
    text = header + "\n" + "\n".join(item_line(it) for it in shown)
    if len(items) > len(shown):
        text += f"\n…и ещё {len(items) - len(shown)} — уточните запрос."
    return text + "\n\n/info <id> · /rename <id> … · /del <id>"


def item_details(it: dict) -> str:
    try:
        bitrate = f"{int(it['bitrate']) // 1000} кбит/с"
    except ValueError:
        bitrate = it["bitrate"] or "—"
    year = it["year"] if it["year"].strip("0") else "—"  # у трека без года beets отдаёт «0000»
    rows = [
        f"#{it['id']} · {it['artist'] or '—'} — {it['title'] or '—'}",
        f"Альбом: {it['album'] or '—'}",
        f"Жанр: {it['genres'] or '—'} · Год: {year}",
        f"Формат: {it['format'] or '—'} · {bitrate}",
        f"Файл: {it['path']}",
    ]
    return "\n".join(rows)


async def navidrome_scan() -> Optional[bool]:
    """None — Navidrome не настроен; True/False — удалось ли запустить сканирование."""
    if not (NAVIDROME_USER and NAVIDROME_PASS):
        return None
    salt = uuid.uuid4().hex[:8]
    params = {
        "u": NAVIDROME_USER,
        "t": hashlib.md5((NAVIDROME_PASS + salt).encode()).hexdigest(),  # токен Subsonic API, пароль в URL не уходит
        "s": salt, "v": "1.16.1", "c": "userbot", "f": "json", "fullScan": "true",
    }
    try:
        r = await http.get(f"{NAVIDROME_URL.rstrip('/')}/rest/startScan.view", params=params, timeout=15)
        ok = r.status_code == 200 and r.json().get("subsonic-response", {}).get("status") == "ok"
    except Exception as e:
        log.warning("Navidrome не ответил на запрос сканирования: %s", e)
        return False
    if not ok:
        log.warning("Navidrome отклонил запрос сканирования (%s)", r.status_code)
    return ok


def scan_note(scan: Optional[bool]) -> str:
    if scan is True:
        return "\n🔄 Сканирование Navidrome запущено."
    if scan is False:
        return "\n⚠️ Navidrome не ответил — запустите сканирование вручную."
    return ""


async def cmd_help(event, rest: str) -> None:
    await event.reply(LIB_HELP)


async def cmd_find(event, rest: str) -> None:
    terms = split_args(rest)
    if not terms:
        await event.reply("Что искать? Например: /find scatman")
        return
    items = await find_items(terms)
    await event.reply(format_items(items, f"🔎 Найдено: {len(items)}"))


async def cmd_last(event, rest: str) -> None:
    n = int(rest) if rest.strip().isdigit() else 10
    n = max(1, min(n, LIB_MAX_LIST))
    items = (await find_items(["added-"]))[:n]  # «added-» — сортировка по дате добавления, новые сверху
    await event.reply(format_items(items, f"🕒 Последние {len(items)}:"))


async def cmd_info(event, rest: str) -> None:
    ids = parse_ids(rest)
    if len(ids) != 1:
        await event.reply("Укажите один id: /info 123")
        return
    it = await get_item(ids[0])
    await event.reply(item_details(it) if it else f"🤷 Трека #{ids[0]} нет в библиотеке.")


async def apply_changes(event, item_id: int, changes: dict, what: str) -> None:
    if await get_item(item_id) is None:
        raise LibraryError(f"трека #{item_id} нет в библиотеке")
    # «поле=значение» — один аргумент; -y: без вопроса «Really modify?». Файл переименуется по шаблону путей beets.
    await run_beet("modify", "-y", f"id:{item_id}", *[f"{k}={v}" for k, v in changes.items()])
    it = await get_item(item_id)
    scan = await navidrome_scan()
    text = f"✅ {what}\n{item_details(it)}" if it else f"✅ {what}"
    await event.reply(text + scan_note(scan))


async def cmd_rename(event, rest: str) -> None:
    m = re.match(r"^(\d+)\s+(.+?)\s+-\s+(.+)$", normalize_text(rest).strip())
    if not m:
        await event.reply("Формат: /rename <id> Исполнитель - Название\nНапример: /rename 12 Scatman John - Scatman")
        return
    changes = {"artist": m.group(2).strip(), "title": m.group(3).strip()}
    await apply_changes(event, int(m.group(1)), changes, "Переименовано.")


async def cmd_set(event, rest: str) -> None:
    tokens = split_args(normalize_text(rest))
    usage = 'Формат: /set <id> поле=значение …\nНапример: /set 12 album="Best Of" year=1995'
    if len(tokens) < 2 or not tokens[0].isdigit():
        await event.reply(usage)
        return
    changes = {}
    for tok in tokens[1:]:
        key, sep, value = tok.partition("=")
        key = key.strip().lower()
        if not sep or key not in LIB_SETTABLE:
            raise LibraryError(f"не понял «{tok}». Можно менять: {', '.join(LIB_SETTABLE)}")
        if key == "year" and not value.isdigit():
            raise LibraryError("год должен быть числом")
        changes[LIB_FIELD_ALIAS.get(key, key)] = value.strip()  # жанры несколько: «Rock; Metal»
    await apply_changes(event, int(tokens[0]), changes, "Теги изменены.")


def _delete_note(choice: Optional[str]) -> str:
    if choice == "yes":
        return "🗑 Удаляю…"
    if choice is None:
        return "⌛ Ответа не дождался — ничего не удалено."
    return "✖️ Отменено, ничего не удалено."


async def cmd_del(event, rest: str) -> None:
    ids = parse_ids(rest)
    if not ids:
        await event.reply("Формат: /del <id> [id …]")
        return
    items = [it for it in [await get_item(i) for i in ids] if it]
    if not items:
        await event.reply("🤷 Таких треков нет в библиотеке.")
        return
    listing = "\n".join(item_line(it) for it in items)
    answer = await ask_choice(
        event,
        f"🗑 Удалить навсегда ({len(items)})?\n{listing}\nФайлы тоже удалятся с диска.",
        [[("🗑 Удалить", "yes"), ("✖️ Отмена", "cancel")]],
        YES_ALIASES,
        "да — удалить, нет — отмена",
        _delete_note,
    )
    if answer != "yes":
        return
    done = 0
    problems = []
    for it in items:
        try:
            await run_beet("remove", "-d", "-f", f"id:{it['id']}")  # -d: и файл с диска; -f: без вопроса
            done += 1
        except LibraryError as e:
            problems.append(f"#{it['id']}: {e}")
    scan = await navidrome_scan() if done else None
    text = f"✅ Удалено: {done} из {len(items)}." + scan_note(scan)
    if problems:
        text += "\n⚠️ Не удалось:\n" + "\n".join(problems)
    await event.reply(text)


async def cmd_stats(event, rest: str) -> None:
    await event.reply("📊 " + (await run_beet("stats")).strip())


async def cmd_rescan(event, rest: str) -> None:
    scan = await navidrome_scan()
    if scan is None:
        await event.reply("Navidrome не настроен (NAVIDROME_USER / NAVIDROME_PASS).")
    else:
        await event.reply(scan_note(scan).strip())


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
HISTORY_DB = Path(os.environ.get("HISTORY_DB", str(TMP_DIR / "state" / "history.sqlite3")))
DUPLICATE_CHECK = env_flag("DUPLICATE_CHECK", True)
LINK_INGEST = env_flag("LINK_INGEST", True)
YTDLP_BIN = os.environ.get("YTDLP_BIN", "yt-dlp")
YTDLP_COOKIES = os.environ.get("YTDLP_COOKIES", "").strip()
YTDLP_EXTRA_ARGS = shlex.split(os.environ.get("YTDLP_EXTRA_ARGS", ""))
YTDLP_TIMEOUT_SEC = int(os.environ.get("YTDLP_TIMEOUT_SEC", "1800"))
LINK_MAX_ENTRIES = max(1, int(os.environ.get("LINK_MAX_ENTRIES", "100")))
LINK_VIDEO_MAX_HEIGHT = int(os.environ.get("LINK_VIDEO_MAX_HEIGHT", "1080"))
LINK_EMBED_THUMB = env_flag("LINK_EMBED_THUMB", True)
SUBTITLE_RECODE = env_flag("SUBTITLE_RECODE", True)
SUBTITLE_FALLBACK_ENCODING = os.environ.get("SUBTITLE_FALLBACK_ENCODING", "cp1251").strip() or "cp1251"
SUBTITLE_MAX_BYTES = 20 * 1024 * 1024


def is_active(key: tuple) -> bool:
    """Есть ли в работе задание этого сообщения (у заданий-по-ссылке ключ длиннее: (чат, сообщение, номер))."""
    return any(k[:2] == key for k in ACTIVE_JOBS)


def beets_ready() -> bool:
    return bool(LIBRARY_ADMIN and shutil.which(BEETS_BIN) and Path(BEETS_CONFIG).exists())


# ---------- история загрузок (SQLite) ----------
_HISTORY_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS history ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, chat_id INTEGER NOT NULL, "
    "kind TEXT NOT NULL, remote_name TEXT NOT NULL, filename TEXT NOT NULL, size INTEGER DEFAULT 0, "
    "replaced INTEGER DEFAULT 0, source TEXT DEFAULT '', undone INTEGER DEFAULT 0)"
)


def _history_conn() -> sqlite3.Connection:
    HISTORY_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(HISTORY_DB, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute(_HISTORY_SCHEMA)
    return conn


def history_add(chat_id: int, kind: str, remote_name: str, filename: str, size: int, replaced: bool, source: str) -> None:
    with contextlib.closing(_history_conn()) as conn:
        with conn:
            conn.execute(
                "INSERT INTO history (ts, chat_id, kind, remote_name, filename, size, replaced, source) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (time.time(), chat_id, kind, remote_name, filename, size or 0, int(replaced), source),
            )


def history_list(chat_id: int, limit: int) -> list:
    with contextlib.closing(_history_conn()) as conn:
        rows = conn.execute(
            "SELECT * FROM history WHERE chat_id=? ORDER BY id DESC LIMIT ?", (chat_id, limit)
        ).fetchall()
    return [dict(r) for r in rows]


def history_get(chat_id: int, hid: Optional[int] = None) -> Optional[dict]:
    with contextlib.closing(_history_conn()) as conn:
        if hid is not None:
            row = conn.execute("SELECT * FROM history WHERE id=? AND chat_id=?", (hid, chat_id)).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM history WHERE chat_id=? AND undone=0 ORDER BY id DESC LIMIT 1", (chat_id,)
            ).fetchone()
    return dict(row) if row else None


def history_mark_undone(hid: int) -> None:
    with contextlib.closing(_history_conn()) as conn:
        with conn:
            conn.execute("UPDATE history SET undone=1 WHERE id=?", (hid,))


async def record_history(job: Job, remote_name: str, existed: bool) -> None:
    """Запись об успешной загрузке. Сбой истории не должен портить саму загрузку."""
    source = "subtitle" if job.subtitle else ("link" if job.url else "telegram")
    try:
        await run_blocking(history_add, job.chat_id, job.kind, remote_name, job.filename, job.size, existed, source)
    except Exception as e:
        log.warning("Не удалось записать загрузку в историю: %s", e)


def _history_icon(row: dict) -> str:
    if row["source"] == "subtitle":
        return "💬"
    return "🎵" if row["kind"] == "music" else "🎬"


# ---------- содержимое папок на сервере ----------
async def list_remote_entries(url, user, pw, segments: list) -> Optional[tuple]:
    """(папки, файлы) в папке на сервере. ([], []) — папки нет; None — узнать не удалось."""
    path = "/".join(quote(s, safe="") for s in segments)
    base = f"{url.rstrip('/')}/" + (f"{path}/" if path else "")
    try:
        resp = await http.get(base + "?ls", auth=(user, pw), timeout=30)
    except httpx.HTTPError as e:
        log.warning("Не удалось получить список «%s»: %s", "/".join(segments) or "/", e)
        return None
    if resp.status_code == 404:
        return [], []
    if resp.status_code != 200:
        return None
    try:
        data = resp.json()
    except ValueError:
        return None

    def names(key: str) -> list:
        out = []
        for entry in data.get(key) or []:
            href = entry.get("href") if isinstance(entry, dict) else entry
            if isinstance(href, str):
                name = unquote(href).strip("/").rsplit("/", 1)[-1]
                if name:
                    out.append(name)
        return out

    return names("dirs"), names("files")


async def find_series_dirs(url, user, pw, show: str, season: int) -> Optional[list]:
    """Существующие папки [сериал, сезон] без создания новых. None — такого сериала/сезона ещё нет."""
    show = truncate_dirname(show)
    root = await list_remote_dirs(url, user, pw, []) or []
    show_dir = show if show in root else None
    if show_dir is None:
        key = series_key(show)
        if key:
            show_dir = next((n for n in sorted(root, key=str.casefold) if series_key(n) == key), None)
    if show_dir is None:
        return None
    seasons = await list_remote_dirs(url, user, pw, [show_dir]) or []
    season_dir = next((n for n in sorted(seasons, key=str.casefold) if season_number(n) == season), None)
    return [show_dir, season_dir] if season_dir else None


async def prune_empty_dirs(url, user, pw, segments: list) -> None:
    """Удаляет опустевшие папки сериала (сезон, потом сериал). Папку трогаем, только убедившись, что она пуста:
    DELETE на папку в copyparty рекурсивный."""
    while segments:
        entries = await list_remote_entries(url, user, pw, segments)
        if entries is None or entries[0] or entries[1]:
            return
        if not await delete_remote_quietly(url, user, pw, "/".join(segments)):
            return
        segments = segments[:-1]


# ---------- проверка дубликатов ----------
async def find_library_matches(stem: str) -> list:
    """Треки библиотеки beets с тем же исполнителем и названием (точное сравнение, без учёта регистра и знаков)."""
    m = _ARTIST_TITLE_RE.match(stem)
    if not m:
        return []
    artist, title = m.group(1).strip(), m.group(2).strip()
    items = await find_items([f"artist:{artist}", f"title:{title}"])
    ak, tk = series_key(artist), series_key(title)
    return [it for it in items if series_key(it["artist"]) == ak and series_key(it["title"]) == tk]


async def find_duplicate(kind: str, filename: str, series: Optional[dict]) -> Optional[str]:
    """Текст про уже имеющийся файл или None."""
    url, user, pw = TARGETS[kind]
    if kind == "music":
        if beets_ready():
            try:
                items = await find_library_matches(Path(filename).stem)
            except LibraryError as e:
                log.warning("Проверка библиотеки beets не удалась: %s", e)
                items = []
            if items:
                more = f"\n…и ещё {len(items) - 3}" if len(items) > 3 else ""
                return "Такой трек уже есть в библиотеке:\n" + "\n".join(item_line(it) for it in items[:3]) + more
        if await get_remote_size(url, user, pw, filename) is not None:
            return f"«{short_name(filename)}» уже лежит на сервере (ждёт импорта)."
        return None
    if series and series.get("episode") is not None:
        dirs = await find_series_dirs(url, user, pw, series["show"], int(series["season"]))
        if not dirs:
            return None
        entries = await list_remote_entries(url, user, pw, dirs)
        if not entries:
            return None
        existing = pick_episode_video(entries[1], int(series["season"]), int(series["episode"]))
        return f"Эта серия уже есть: {'/'.join(dirs)}/«{existing}»." if existing else None
    size = await get_remote_size(url, user, pw, filename)
    if size is not None:
        extra = f" ({format_size(size)})" if size > 0 else ""
        return f"«{short_name(filename)}» уже есть в фильмах{extra}."
    return None


def _dup_note(choice: Optional[str]) -> str:
    if choice == "yes":
        return "✅ Загружаю всё равно."
    if choice is None:
        return "⌛ Ответа не дождался — файл пропущен."
    return "✖️ Отменено."


# ---------- субтитры ----------
_SUB_TAGS = {
    "ru", "rus", "russian", "en", "eng", "english", "uk", "ukr", "de", "ger", "fr", "es", "it", "pt",
    "ja", "jpn", "zh", "forced", "sdh", "default",
}


def split_sub_name(stem: str) -> tuple:
    """«Movie.2021.ru.forced» → («Movie.2021», ['ru', 'forced']). Теги языка отделяются точкой или «_»."""
    tags = []
    base = stem
    while True:
        m = re.search(r"[._]([A-Za-z]{2,8})$", base)
        if not (m and m.group(1).lower() in _SUB_TAGS):
            break
        tags.insert(0, m.group(1).lower())
        base = base[: m.start()]
    return base.strip(" ._-"), tags


def pick_episode_video(files: list, season: int, episode: int) -> Optional[str]:
    for f in sorted(files, key=str.casefold):
        if Path(f).suffix.lower() not in VIDEO_EXTS:
            continue
        m = _SERIES_MARK_RE.search(Path(f).stem)
        if m and int(m.group(1) or m.group(3)) == season and int(m.group(2) or m.group(4)) == episode:
            return f
    return None


def _title_key(name: str) -> str:
    """series_key + без года (в имени субтитров он бывает «Dune 2021», а у видео — «Dune (2021)»)."""
    return series_key(re.sub(r"(?<!\d)(?:19|20)\d{2}(?!\d)", " ", name))


def pick_movie_video(files: list, base: str) -> Optional[str]:
    sk = _title_key(base)
    if not sk:
        return None
    vids = sorted((f for f in files if Path(f).suffix.lower() in VIDEO_EXTS), key=str.casefold)
    keyed = [(f, _title_key(Path(f).stem)) for f in vids]
    exact = [f for f, k in keyed if k == sk]
    if exact:
        return exact[0]
    loose = [f for f, k in keyed if len(k) >= 6 and len(sk) >= 6 and (k.startswith(sk) or sk.startswith(k))]
    return loose[0] if len(loose) == 1 else None


def normalize_subtitle_file(path: str) -> None:
    """Текстовые субтитры → UTF-8 без BOM (Jellyfin плохо угадывает cp1251/UTF-16 и показывает кракозябры).
    Синхронная; вызывать через run_blocking. Бинарные форматы и уже нормальный UTF-8 не трогаем."""
    if not SUBTITLE_RECODE or Path(path).suffix.lower() not in {".srt", ".ass", ".ssa", ".vtt", ".sub"}:
        return
    try:
        p = Path(path)
        if p.stat().st_size > SUBTITLE_MAX_BYTES:
            return
        raw = p.read_bytes()
    except OSError:
        return
    try:
        if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
            text, enc = raw.decode("utf-16"), "utf-16"
        elif raw.startswith(b"\xef\xbb\xbf"):
            text, enc = raw.decode("utf-8-sig"), "utf-8-sig"
        else:
            try:
                raw.decode("utf-8")
                return  # уже UTF-8 (или чистый ASCII)
            except UnicodeDecodeError:
                text, enc = raw.decode(SUBTITLE_FALLBACK_ENCODING), SUBTITLE_FALLBACK_ENCODING
    except (UnicodeDecodeError, LookupError) as e:
        log.warning("Не удалось определить кодировку субтитров %s: %s", path, e)
        return
    try:
        p.write_bytes(text.encode("utf-8"))
        log.info("Субтитры перекодированы %s → UTF-8: %s", enc, path)
    except OSError as e:
        log.warning("Не удалось перезаписать субтитры: %s", e)


async def plan_subtitle(event, orig_filename: str) -> tuple:
    """(ok, series): series — {"show","season","episode"} для субтитров сериала, None — для фильма.
    ok=False — файл надо пропустить."""
    base, _tags = split_sub_name(Path(orig_filename).stem)
    info = parse_episode_from_name(base)
    if info is None:
        return True, None
    show = info["show"]
    if not show or is_generic_name(show):
        text = await ask_text(
            event,
            f"💬 «{orig_filename}» — субтитры к серии (сезон {info['season']}, серия {info['episode']}), "
            f"но название сериала я не нашёл.",
            "название сериала — например: Dune Prophecy",
        )
        show = clean_show_name(text or "")
        if not show or is_generic_name(show):
            if text:
                await event.reply("🤷 Не разобрал название сериала — субтитры пропущены.")
            return False, None
    return True, {"show": show, "season": info["season"], "episode": info["episode"]}


async def resolve_subtitle_target(url, user, pw, job: Job) -> tuple:
    """Находит видео, к которому относятся субтитры, и называет файл так же (+ тег языка).
    Возвращает (путь_на_сервере, текст_для_статуса). Меняет job.filename."""
    base, tags = split_sub_name(Path(job.filename).stem)
    ext = Path(job.filename).suffix
    dirs: list = []
    video = None
    if job.series:
        season = int(job.series["season"])
        dirs = await resolve_series_dirs(url, user, pw, job.series["show"], season)
        entries = await list_remote_entries(url, user, pw, dirs)
        if entries and job.series.get("episode") is not None:
            video = pick_episode_video(entries[1], season, int(job.series["episode"]))
    else:
        entries = await list_remote_entries(url, user, pw, [])
        if entries:
            video = pick_movie_video(entries[1], base)
    if video:
        leaf = Path(video).stem + "".join("." + t for t in tags) + ext
        note = f"рядом с «{short_name(video, 50)}»"
    else:
        leaf = job.filename
        note = "видео не найдено — имя оставлено как есть"
    leaf = truncate_filename(leaf)
    job.filename = leaf
    where = "/".join(dirs)
    where = f"{where} ({note})" if where else note
    return "/".join(dirs + [leaf]), where


# ---------- ссылки (yt-dlp) ----------
_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_SKIP_URL_RE = re.compile(r"^https?://(?:www\.)?(?:t\.me|telegram\.me)/", re.IGNORECASE)
_MUSIC_HOSTS = ("music.youtube.com", "soundcloud.com", "bandcamp.com")
_TITLE_NOISE_RE = re.compile(
    r"\s*[\(\[]\s*(?:official\s+(?:music\s+)?(?:video|audio|lyric\s+video)|lyrics?(?:\s+video)?|audio|video|hd|hq|4k)\s*[\)\]]",
    re.IGNORECASE,
)
_PERMANENT_YTDLP_RE = re.compile(
    r"private video|video unavailable|not available|copyright|sign in|age-restricted|members-only|"
    r"geo.?restricted|unsupported url|login required|premiere",
    re.IGNORECASE,
)


def extract_urls(text: str) -> list:
    urls = []
    for m in _URL_RE.finditer(text or ""):
        u = m.group(0).rstrip(".,;:!?)»”'\"")
        if u and u not in urls:
            urls.append(u)
    return urls


def is_link_message(event) -> bool:
    """Обычное текстовое сообщение со ссылкой (не команда и не ответ на вопрос бота)."""
    if not LINK_INGEST or event.is_reply:
        return False
    text = event.raw_text or ""
    if not text or text.startswith("/"):
        return False
    return any(not _SKIP_URL_RE.search(u) for u in extract_urls(text))


def link_kind(url: str) -> Optional[str]:
    host = (urlparse(url).hostname or "").lower()
    if any(host == h or host.endswith("." + h) for h in _MUSIC_HOSTS):
        return "music"
    return None


def _is_single_video_url(url: str) -> bool:
    """Ссылка на одно видео (в т.ч. с хвостом &list=…) — плейлист вокруг неё не разворачиваем."""
    p = urlparse(url)
    host = (p.hostname or "").lower()
    if host == "youtu.be":
        return True
    return host.endswith("youtube.com") and p.path == "/watch" and "v" in parse_qs(p.query)


def ytdlp_base_args() -> list:
    args = [
        YTDLP_BIN, "--ignore-config", "--no-warnings",
        "--retries", "5", "--fragment-retries", "5", "--socket-timeout", "30",
    ]
    if YTDLP_COOKIES:
        args += ["--cookies", YTDLP_COOKIES]
    return args + YTDLP_EXTRA_ARGS


def ytdlp_error(err: str, rc: int) -> TransferError:
    lines = re.findall(r"ERROR:\s*(.+)", err)
    tail = err.strip().splitlines()[-1] if err.strip() else f"код {rc}"
    reason = (lines[-1] if lines else tail)[:200]
    hint = ""
    if re.search(r"sign in|not a bot|cookies", err, re.IGNORECASE):
        hint = " (YouTube просит авторизацию — задайте YTDLP_COOKIES)"
    return TransferError(f"yt-dlp: {reason}{hint}", retriable=not _PERMANENT_YTDLP_RE.search(err))


async def _run_capture(cmd: list, timeout: int) -> tuple:
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
    except FileNotFoundError:
        raise TransferError(f"в образе бота нет «{cmd[0]}» — добавьте yt-dlp в requirements.txt") from None
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        await proc.wait()
        raise TransferError(f"«{cmd[0]}» не ответил за {timeout} сек") from None
    return proc.returncode, out.decode("utf-8", errors="replace"), err.decode("utf-8", errors="replace")


def _entry_url(e: dict) -> Optional[str]:
    for key in ("webpage_url", "url"):
        u = e.get(key)
        if isinstance(u, str) and u.startswith("http"):
            return u
    extractor = str(e.get("ie_key") or e.get("extractor_key") or "").lower()
    if e.get("id") and extractor.startswith("youtube"):
        return f"https://www.youtube.com/watch?v={e['id']}"
    return None


async def probe_link(url: str) -> tuple:
    """(название_плейлиста, [элементы]). У элемента есть «_url» — по нему его и скачиваем."""
    args = ytdlp_base_args() + ["-J", "--flat-playlist", "--skip-download"]
    if _is_single_video_url(url):
        args.append("--no-playlist")
    args.append(url)
    rc, out, err = await _run_capture(args, 120)
    if rc != 0:
        raise ytdlp_error(err, rc)
    start = out.find("{")
    try:
        info = json.loads(out[start:]) if start >= 0 else None
    except ValueError:
        info = None
    if not isinstance(info, dict):
        raise TransferError("не удалось разобрать ответ yt-dlp")
    entries = info.get("entries")
    if entries is None:
        return "", [dict(info, _url=url)]
    items = []
    for e in entries:
        if isinstance(e, dict):
            u = _entry_url(e)
            if u:
                items.append(dict(e, _url=u))
    return info.get("title") or "", items


def music_meta_from_info(info: dict, fallback: str = "") -> tuple:
    """(исполнитель, название, альбом) из метаданных yt-dlp. У YouTube Music есть artist/track/album;
    у обычных видео — «Исполнитель - Название» в заголовке или имя канала."""
    artist = info.get("artist") or ""
    if not artist and isinstance(info.get("artists"), list):
        artist = ", ".join(str(a) for a in info["artists"] if a)
    track = info.get("track")
    title = track or _TITLE_NOISE_RE.sub("", info.get("title") or "") or fallback
    if not artist:
        m = _ARTIST_TITLE_RE.match(title)
        if m:
            artist, title = m.group(1), m.group(2)
    if not artist:
        artist = re.sub(r"\s*-\s*Topic$", "", info.get("uploader") or info.get("channel") or "")
    artist = sanitize_name(artist) or "Unknown Artist"
    title = sanitize_name(title) or "Untitled"
    album = normalize_text(info.get("album") or "").strip()
    return artist, title, album


def fix_m4a_tags(path: str, artist: str, title: str, album: str) -> None:
    """Исполнитель и название в тегах = тем, что в имени файла (иначе beets импортирует трек без тегов).
    Синхронная; вызывать через run_blocking."""
    try:
        mp4 = MP4(path)
        if mp4.tags is None:
            mp4.add_tags()
        mp4["\xa9ART"] = [artist]
        mp4["\xa9nam"] = [title]
        if album and not mp4.get("\xa9alb"):
            mp4["\xa9alb"] = [album]
        mp4.save()
    except Exception as e:
        log.warning("Не удалось записать теги m4a %s: %s", path, e)


def _to_int(value: str) -> Optional[int]:
    try:
        return int(float(value))
    except ValueError:
        return None


async def _run_ytdlp_download(args: list, state: TransferState) -> None:
    try:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
    except FileNotFoundError:
        raise TransferError(f"в образе бота нет «{args[0]}» — добавьте yt-dlp в requirements.txt") from None
    err_task = asyncio.ensure_future(proc.stderr.read())

    async def pump() -> int:
        while True:
            raw = await proc.stdout.readline()
            if not raw:
                break
            parts = raw.decode("utf-8", errors="replace").split()
            if len(parts) >= 4 and parts[0] == "PROG":
                done = _to_int(parts[1])
                total = _to_int(parts[2]) or _to_int(parts[3])
                if done is not None:
                    state.current = done
                state.total = max(total or 0, state.current, 1)
        return await proc.wait()

    def kill() -> None:
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()

    try:
        rc = await asyncio.wait_for(pump(), timeout=YTDLP_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        kill()
        err_task.cancel()
        raise TransferError(f"yt-dlp не уложился в {YTDLP_TIMEOUT_SEC} сек") from None
    except BaseException:
        kill()
        err_task.cancel()
        raise
    err = (await err_task).decode("utf-8", errors="replace")
    if rc != 0:
        raise ytdlp_error(err, rc)


async def download_from_link(job: Job, local_path: Path, state: TransferState) -> Path:
    """Скачивает job.url через yt-dlp в TMP_DIR, уточняет имя/теги по метаданным, проверяет дубликат.
    Возвращает путь к готовому файлу (job.filename к этому моменту уже окончательный)."""
    stem = local_path.stem
    args = ytdlp_base_args() + [
        "--no-playlist", "--no-mtime", "--newline", "--write-info-json",
        "--progress-template",
        "download:PROG %(progress.downloaded_bytes)s %(progress.total_bytes)s %(progress.total_bytes_estimate)s",
        "-o", str(TMP_DIR / f"{stem}.%(ext)s"),
    ]
    if job.kind == "music":
        args += ["-f", "bestaudio[ext=m4a]/bestaudio/best", "-x", "--audio-format", "m4a",
                 "--audio-quality", "0", "--embed-metadata"]
        if LINK_EMBED_THUMB:
            args += ["--embed-thumbnail", "--convert-thumbnails", "jpg"]
    else:
        args += ["-S", f"res:{LINK_VIDEO_MAX_HEIGHT},vcodec:h264,acodec:m4a", "--merge-output-format", "mp4"]
    args.append(job.url)

    for attempt in (1, 2):
        state.begin("download", attempt, current=0, total=1)
        try:
            await _run_ytdlp_download(args, state)
            break
        except TransferError as e:
            log.warning("yt-dlp, попытка %s/2: %s", attempt, e.user_message)
            if attempt == 2 or not e.retriable:
                raise
            await sleep_waiting(state, RETRY_BACKOFF_SEC)

    wanted = (".m4a",) if job.kind == "music" else (".mp4", ".mkv", ".webm", ".mov")
    out = next(
        (TMP_DIR / f"{stem}{e}" for e in wanted
         if (TMP_DIR / f"{stem}{e}").exists() and (TMP_DIR / f"{stem}{e}").stat().st_size > 0),
        None,
    )
    if out is None:
        raise TransferError("yt-dlp не создал файл")
    size = out.stat().st_size
    state.begin("download", state.attempt, current=size, total=size)

    info: dict = {}
    with contextlib.suppress(OSError, ValueError):
        info = json.loads((TMP_DIR / f"{stem}.info.json").read_text(encoding="utf-8"))

    if job.kind == "music":
        artist, title, album = music_meta_from_info(info, fallback=Path(job.filename).stem)
        job.filename = truncate_filename(f"{artist} - {title}.m4a")
        await run_blocking(fix_m4a_tags, str(out), artist, title, album)
    elif Path(job.filename).suffix.lower() != out.suffix.lower():
        job.filename = truncate_filename(Path(job.filename).stem + out.suffix.lower())

    if DUPLICATE_CHECK and not job.force:
        try:
            dup = await find_duplicate(job.kind, job.filename, job.series)
        except Exception as e:
            log.warning("Проверка дубликатов не удалась: %s", e)
            dup = None
        if dup:
            raise TransferError("дубликат: " + dup.splitlines()[0].rstrip(":.") + " (добавьте --force к сообщению, чтобы загрузить всё равно)")
    return out


def _link_kind_note(choice: Optional[str]) -> str:
    if choice == "music":
        return "✅ Скачиваю аудио."
    if choice == "movie":
        return "✅ Скачиваю видео."
    if choice is None:
        return "⌛ Ответа не дождался — ссылка пропущена."
    return "✖️ Отменено."


async def ingest_link(event, url: str, force: bool) -> None:
    status = await event.reply("🔎 Смотрю ссылку…")
    try:
        title, items = await probe_link(url)
    except Exception as e:
        log.warning("Ссылка %s не прочиталась: %s", url, e)
        await safe_edit(status, f"❌ Ссылка не обработана: {describe_error(e)}.")
        return
    if not items:
        await safe_edit(status, "🤷 По этой ссылке ничего не нашёл.")
        return
    total_found = len(items)
    items = items[:LINK_MAX_ENTRIES]

    async def drop_status() -> None:
        with contextlib.suppress(Exception):
            await status.delete()

    kind = link_kind(url)
    cut = f" (беру первые {len(items)})" if total_found > len(items) else ""
    if kind is None:
        first = short_name(items[0].get("title") or url, 70)
        extra = f" и ещё {len(items) - 1}{cut}" if len(items) > 1 else ""
        choice = await ask_choice(
            event, f"🔗 Что скачать: «{first}»{extra}?",
            [[("🎵 Аудио (m4a)", "music"), ("🎬 Видео", "movie")], [("✖️ Отмена", "cancel")]],
            KIND_ALIASES, "1 — аудио (в музыку), 2 — видео (в фильмы), 0 — отмена", _link_kind_note,
        )
        if choice not in ("music", "movie"):
            await drop_status()
            return
        kind = choice
    elif len(items) > 1:
        choice = await ask_choice(
            event, f"📋 «{short_name(title or url, 60)}» — {len(items)} шт.{cut}. Скачать все как музыку?",
            [[("⬇️ Скачать все", "yes"), ("✖️ Отмена", "cancel")]],
            YES_ALIASES, "да — скачать, нет — отмена", _dup_note,
        )
        if choice != "yes":
            await drop_status()
            return

    jobs = []
    for i, it in enumerate(items, 1):
        series = None
        if kind == "music":
            artist, ttl, _album = music_meta_from_info(it)
            filename = truncate_filename(f"{artist} - {ttl}.m4a")
        else:
            name = sanitize_name(normalize_text(it.get("title") or it.get("id") or "video")) or "video"
            filename = truncate_filename(name + ".mp4")
            if len(items) == 1:
                plan = await plan_video(event, filename, ".mp4", None, filename)
                if plan is None:
                    await drop_status()
                    return
                series, filename = plan
        jobs.append(Job(
            chat_id=event.chat_id, sender_id=event.sender_id, message_id=event.message.id,
            kind=kind, filename=filename, size=0, series=series, url=it["_url"], part=i, force=force,
        ))
    for job in jobs:
        journal_write(job)
    await drop_status()
    results = await asyncio.gather(*(run_job(j, None, event.reply) for j in jobs), return_exceptions=True)
    for r in results:
        if isinstance(r, BaseException):
            log.error("Задание по ссылке завершилось с ошибкой: %r", r)


async def handle_links(event) -> None:
    text = event.raw_text or ""
    force = "--force" in text
    urls = [u for u in extract_urls(text) if not _SKIP_URL_RE.search(u)][:5]
    for url in urls:
        try:
            await ingest_link(event, url, force)
        except Exception as e:
            log.exception("link ingest failed")
            await event.reply(f"❌ Не получилось: {describe_error(e)}.")


# ---------- /history и /undo ----------
def _format_ts(ts: float) -> str:
    return time.strftime("%d.%m %H:%M", time.localtime(ts))


async def cmd_history(event, rest: str) -> None:
    n = int(rest) if rest.strip().isdigit() else 10
    n = max(1, min(n, LIB_MAX_LIST))
    rows = await run_blocking(history_list, event.chat_id, n)
    if not rows:
        await event.reply("🕒 История пуста.")
        return
    lines = [
        f"#{r['id']} · {_format_ts(r['ts'])} · {_history_icon(r)} {short_name(r['remote_name'], 70)}"
        + (" ↩️" if r["undone"] else "")
        for r in rows
    ]
    await event.reply("🕒 Последние загрузки:\n" + "\n".join(lines) + "\n\n/undo [номер] — отменить загрузку (↩️ — уже отменена)")


def _undo_note(choice: Optional[str]) -> str:
    if choice == "yes":
        return "↩️ Отменяю…"
    if choice is None:
        return "⌛ Ответа не дождался — ничего не удалено."
    return "✖️ Оставляю как есть."


async def cmd_undo(event, rest: str) -> None:
    arg = rest.strip()
    if arg and not arg.isdigit():
        await event.reply("Формат: /undo — последняя загрузка, или /undo <номер из /history>")
        return
    row = await run_blocking(history_get, event.chat_id, int(arg) if arg else None)
    if row is None:
        await event.reply("🤷 Нечего отменять." if not arg else f"🤷 Загрузки #{arg} нет в истории.")
        return
    if row["undone"]:
        await event.reply(f"↩️ Загрузка #{row['id']} уже отменена.")
        return

    url, user, pw = TARGETS[row["kind"]]
    name = row["remote_name"]
    library_items: list = []
    if row["kind"] == "music":
        if await get_remote_size(url, user, pw, name) is not None:
            question = f"↩️ Удалить с сервера «{short_name(name, 80)}»?"
        else:
            # Файла на copyparty уже нет — скорее всего, его успел забрать импорт в библиотеку beets.
            library_items = await find_library_matches(Path(name).stem) if beets_ready() else []
            if not library_items:
                await event.reply(
                    f"🤷 «{short_name(name, 80)}» уже нет на сервере, а в библиотеке я его не нашёл "
                    f"(найти вручную: /find)."
                )
                return
            listing = "\n".join(item_line(it) for it in library_items[:5])
            question = (
                "↩️ Файла уже нет на сервере — похоже, трек импортирован в библиотеку.\n"
                f"Удалить из библиотеки (и с диска)?\n{listing}"
            )
    else:
        question = f"↩️ Удалить «{short_name(name, 80)}» с сервера фильмов?"
    if row["replaced"]:
        question += "\n⚠️ Эта загрузка заменила старый файл — прежнюю версию вернуть нельзя."

    answer = await ask_choice(
        event, question,
        [[("↩️ Отменить загрузку", "yes"), ("✖️ Нет", "cancel")]],
        YES_ALIASES, "да — отменить, нет — оставить", _undo_note,
    )
    if answer != "yes":
        return

    if library_items:
        done, problems = 0, []
        for it in library_items:
            try:
                await run_beet("remove", "-d", "-f", f"id:{it['id']}")
                done += 1
            except LibraryError as e:
                problems.append(f"#{it['id']}: {e}")
        if done:
            await run_blocking(history_mark_undone, row["id"])
        scan = await navidrome_scan() if done else None
        text = f"✅ Удалено из библиотеки: {done} из {len(library_items)}." + scan_note(scan)
        if problems:
            text += "\n⚠️ Не удалось:\n" + "\n".join(problems)
        await event.reply(text)
        return

    if not await delete_remote_quietly(url, user, pw, name):
        await event.reply("❌ Не удалось удалить файл с сервера — вероятно, у пользователя copyparty нет права delete.")
        return
    await run_blocking(history_mark_undone, row["id"])
    note = ""
    if row["kind"] == "movie":
        await prune_empty_dirs(url, user, pw, name.split("/")[:-1])
        scan = await refresh_jellyfin()
        if scan is True:
            note = "\n🔄 Сканирование Jellyfin запущено."
        elif scan is False:
            note = "\n⚠️ Jellyfin не ответил — запустите сканирование вручную."
    await event.reply(f"✅ Загрузка #{row['id']} отменена: «{short_name(name, 80)}» удалён с сервера." + note)


LIBRARY_COMMANDS = {
    "/help": cmd_help, "/start": cmd_help,
    "/find": cmd_find, "/last": cmd_last, "/info": cmd_info,
    "/rename": cmd_rename, "/set": cmd_set, "/del": cmd_del,
    "/stats": cmd_stats, "/rescan": cmd_rescan,
    "/undo": cmd_undo, "/history": cmd_history,
}


async def handle_library_command(event) -> None:
    cmd, _, rest = event.raw_text.strip().partition(" ")
    handler = LIBRARY_COMMANDS.get(cmd.split("@")[0].lower())
    if handler is None:
        return
    if not LIBRARY_ADMIN and handler not in (cmd_undo, cmd_history):
        await event.reply("🔒 Управление библиотекой выключено (LIBRARY_ADMIN=0).")
        return
    try:
        await handler(event, rest.strip())
    except LibraryError as e:
        await event.reply(f"❌ {e}")
    except Exception as e:
        log.exception("library command failed")
        await event.reply(f"❌ Не получилось: {describe_error(e)}.")


# ---------- входящие сообщения ----------
@client.on(events.NewMessage(incoming=True))
async def handle_dm(event):
    if not event.is_private:
        return

    if ALLOWED_SENDER_IDS and event.sender_id not in ALLOWED_SENDER_IDS:
        log.info("Игнорирую сообщение от чужого sender_id=%s", event.sender_id)
        return

    if not (event.media and event.document):
        if is_link_message(event):
            link_key = (event.chat_id, event.message.id)
            if is_active(link_key):  # catch_up мог прислать сообщение повторно
                log.info("Ссылка из сообщения %s уже обрабатывается — пропускаю повтор", event.message.id)
                return
            ACTIVE_JOBS.add(link_key)
            try:
                await process_message(event)
            finally:
                ACTIVE_JOBS.discard(link_key)
            return
        await process_message(event)
        return

    # Защита от повторной обработки: сообщение уже в работе или будет продолжено после перезапуска
    # (catch_up мог прислать его заново).
    key = (event.chat_id, event.message.id)
    if key in ACTIVE_JOBS:
        log.info("Сообщение %s уже обрабатывается — пропускаю повтор", event.message.id)
        return
    ACTIVE_JOBS.add(key)
    try:
        await process_message(event)
    finally:
        ACTIVE_JOBS.discard(key)


async def process_message(event):
    doc = event.document
    if not event.media or not doc:
        if resolve_text_choice(event):  # ответ на вопрос бота («1/2/0», «да/нет», название)
            return
        if event.raw_text and event.raw_text.startswith("/"):
            await handle_library_command(event)
        elif is_link_message(event):
            await handle_links(event)
        return

    attrs = doc.attributes or []
    is_voice = any(isinstance(a, DocumentAttributeAudio) and getattr(a, "voice", False) for a in attrs)
    is_round = any(isinstance(a, DocumentAttributeVideo) and getattr(a, "round_message", False) for a in attrs)
    if is_voice or is_round:
        await event.reply("🎙 Голосовые и видеосообщения-«кружки» не загружаю — жду музыку или видеофайлы.")
        return
    if any(isinstance(a, JUNK_ATTR_TYPES) for a in attrs):
        await event.reply("🎞 GIF-анимации и стикеры не загружаю — жду музыку или видеофайлы.")
        return

    orig_filename = None
    for attr in attrs:
        if isinstance(attr, DocumentAttributeFilename) and attr.file_name:
            orig_filename = attr.file_name
    if not orig_filename:
        orig_filename = guess_filename(event.message, doc)
        log.info("У файла нет имени, придумал: %s", orig_filename)

    # Не доверяем имени из Telegram буквально: чиним кодировку, убираем путь/traversal и запрещённые символы.
    orig_filename = clean_filename(orig_filename)

    mime_type = getattr(doc, "mime_type", None)
    has_video_attr = any(isinstance(a, DocumentAttributeVideo) for a in attrs)
    has_audio_attr = any(isinstance(a, DocumentAttributeAudio) for a in attrs)
    kind = classify(
        orig_filename, mime_type, size=doc.size or 0,
        has_video_attr=has_video_attr, has_audio_attr=has_audio_attr,
    )
    if kind == "subtitle":
        ok, sub_series = await plan_subtitle(event, orig_filename)
        if not ok:
            return
        job = Job(
            chat_id=event.chat_id, sender_id=event.sender_id, message_id=event.message.id,
            kind="movie", filename=truncate_filename(orig_filename), size=doc.size or 0,
            series=sub_series, subtitle=True,
        )
        journal_write(job)
        await run_job(job, event.message, event.reply)
        return

    asked = False
    if kind is None:
        # Вопрос идёт ДО очереди загрузок, так что ожидание ответа не занимает слот.
        asked = True
        kind = await ask_kind(event, orig_filename)
        if kind is None:
            return

    # Короткое «Исполнитель - Название.mp4» с видеодорожкой — вероятно, клип: спрашиваем, не музыка ли это.
    extract_audio_flag = False
    video_attr = next((a for a in attrs if isinstance(a, DocumentAttributeVideo)), None)
    if (
        kind == "movie" and video_attr is not None
        and Path(orig_filename).suffix.lower() in VIDEO_EXTS
        and looks_like_music_video(Path(orig_filename).stem, getattr(video_attr, "duration", 0) or 0, doc.size or 0)
    ):
        minutes = int((getattr(video_attr, "duration", 0) or 0) // 60)
        secs = int((getattr(video_attr, "duration", 0) or 0) % 60)
        length = f" ({minutes}:{secs:02d})" if getattr(video_attr, "duration", 0) else ""
        choice = await ask_choice(
            event,
            f"🎬 «{orig_filename}»{length} — похоже на музыкальный клип.\nЧто сделать?",
            [[("🎵 Музыка (достать звук)", "music"), ("🎬 Фильм", "movie")], [("✖️ Отмена", "cancel")]],
            KIND_ALIASES,
            "1 — музыка (достану звук), 2 — фильм, 0 — отмена",
            lambda c: (
                "✅ Достану звук и положу в 🎵 музыку." if c == "music"
                else "✅ Понял, кладу в 🎬 фильмы." if c == "movie"
                else "⌛ Ответа не дождался — файл пропущен." if c is None
                else "✖️ Отменено."
            ),
        )
        if choice not in ("music", "movie"):
            return
        if choice == "music":
            kind = "music"
            extract_audio_flag = True
            orig_filename = Path(orig_filename).stem + ".m4a"

    # Расширения нет или это обрывок названия («Mr. Brightside») — дописываем по mime, ничего не отрезая.
    orig_filename, ext = ensure_extension(orig_filename, kind, mime_type)
    if asked:
        orig_filename, ext = align_extension(orig_filename, ext, kind, mime_type)

    # Трек, пришедший как «.mp4»/«.m4v»/«.mov»: в музыкальной библиотеке и для индексатора тегов нужен «.m4a».
    if kind == "music" and ext.lower() in MP4_FAMILY_EXTS:
        orig_filename = orig_filename[: -len(ext)] + ".m4a"
        ext = ".m4a"
        log.info("Аудио в контейнере mp4 — переименовал в %s", orig_filename)

    filename = orig_filename
    if kind == "movie":
        parsed = parse_video_caption(event.raw_text)
        candidate = build_filename_from_video_caption(parsed, ext) if parsed else None
    else:
        parsed = parse_music_caption(event.raw_text)
        candidate = build_filename_from_music_caption(parsed, ext) if parsed else None
    if parsed:
        if should_use_caption(kind, orig_filename, parsed):
            filename = candidate
            log.info("Имя по подписи (%s): %s -> %s", CAPTION_NAMING, parsed, filename)
        else:
            log.info(
                "Подпись распознана (%s), но имя файла из Telegram «%s» уже информативно — оставляю его",
                candidate, orig_filename,
            )
    filename = truncate_filename(filename)

    # Видео: фильм или серия сериала (и если не понять — спрашиваем). Клип, из которого достаём звук, — не сериал.
    series = None
    if kind == "movie" and not extract_audio_flag:
        plan = await plan_video(event, orig_filename, ext, parsed, filename)
        if plan is None:
            return
        series, filename = plan

    if DUPLICATE_CHECK:
        try:
            dup = await find_duplicate(kind, filename, series)
        except Exception as e:
            log.warning("Проверка дубликатов не удалась: %s", e)
            dup = None
        if dup:
            answer = await ask_choice(
                event, f"♻️ {dup}\nЗагрузить всё равно?",
                [[("⬆️ Загрузить", "yes"), ("✖️ Отмена", "cancel")]],
                YES_ALIASES, "да — загрузить, нет — отмена", _dup_note,
            )
            if answer != "yes":
                return

    job = Job(
        chat_id=event.chat_id,
        sender_id=event.sender_id,
        message_id=event.message.id,
        kind=kind,
        filename=filename,
        size=doc.size or 0,
        extract=extract_audio_flag,
        series=series,
    )
    journal_write(job)  # с этого момента задание переживёт перезапуск бота
    await run_job(job, event.message, event.reply)


async def resume_one(job: Job) -> None:
    """Продолжает задание, прерванное остановкой бота."""
    try:
        message = None
        if not job.url:  # задания по ссылке не привязаны к документу в Telegram
            message = await client.get_messages(job.chat_id, ids=job.message_id)
            if not message or not message.document:
                log.warning("Сообщение %s пропало — задание «%s» отбрасываю", job.message_id, job.filename)
                drop_job_files(job)
                return
        log.info("Продолжаю после перезапуска: «%s»", job.filename)
        await run_job(job, message, functools.partial(client.send_message, job.chat_id))
    except Exception:
        log.exception("Не удалось продолжить задание «%s»", job.filename)
    finally:
        ACTIVE_JOBS.discard(job.key)


async def main():
    global http, UPLOAD_SEMAPHORE
    UPLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_TRANSFERS)
    pending_jobs = prepare_tmp_dir()
    for job in pending_jobs:
        ACTIVE_JOBS.add(job.key)  # чтобы catch_up не запустил то же сообщение второй раз
    http = httpx.AsyncClient(
        timeout=HTTP_DEFAULT_TIMEOUT,
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=4),
    )
    if LIBRARY_ADMIN:
        if not shutil.which(BEETS_BIN):
            log.warning("Управление библиотекой: «%s» не найден — команды /find, /del и т.д. работать не будут", BEETS_BIN)
        elif not Path(BEETS_CONFIG).exists():
            log.warning("Управление библиотекой: нет конфига %s — смонтируйте папку с конфигом beets", BEETS_CONFIG)
    if LINK_INGEST and not shutil.which(YTDLP_BIN):
        log.warning("Ссылки: «%s» не найден — установите yt-dlp (pip install yt-dlp), иначе ссылки работать не будут", YTDLP_BIN)
    try:
        await start_buttons_bot()
        await client.start()
        me = await client.get_me()
        log.info(
            "Userbot started as %s (id=%s), watching DMs from %s, version %s",
            me.username, me.id, ALLOWED_SENDER_IDS or "ВСЕХ (!)", BOT_VERSION,
        )
        for job in pending_jobs:
            job.starts += 1
            journal_write(job)
            spawn(resume_one(job))
        await client.run_until_disconnected()
    finally:
        # Закрываем клиента явно: иначе сессия (sqlite) и состояние для catch_up могут сохраниться не до конца.
        with contextlib.suppress(Exception):
            await client.disconnect()
        if bot_client is not None:
            with contextlib.suppress(Exception):
                await bot_client.disconnect()
        await http.aclose()


if __name__ == "__main__":
    asyncio.run(main())
