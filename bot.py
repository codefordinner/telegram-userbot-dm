"""
Userbot: принимает в личку музыку/видео и заливает на copyparty (+ обновляет Jellyfin).

Переменные окружения (новые помечены *):
  TG_API_ID, TG_API_HASH, SESSION_PATH, ALLOWED_SENDER_IDS
  MUSIC_COPYPARTY_URL/USER/PASS, MOVIES_COPYPARTY_URL/USER/PASS
  JELLYFIN_URL, JELLYFIN_API_KEY
  * TMP_DIR            — куда качать временные файлы (по умолчанию /tmp/userbot-uploader)
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
  * CHOICE_TIMEOUT_SEC — сколько секунд ждать выбора, прежде чем пропустить файл (по умолчанию 300)
  * LIBRARY_ADMIN      — команды управления музыкальной библиотекой в личке: /find /last /info /rename /set
                         /del /stats /rescan (1/0, по умолчанию 1). Нужен beets В ТОЙ ЖЕ ВЕРСИИ, что и в контейнере
                         импорта, и общие папки библиотеки и конфига по тем же путям, что и там
  * BEETS_BIN          — команда beets (по умолчанию beet)
  * BEETS_CONFIG       — конфиг beets (по умолчанию /beets-cfg/config.yaml)
  * BEETS_PLUGINS      — плагины beets для команд бота (по умолчанию scrub; пусто = без плагинов)
  * BEETS_TIMEOUT_SEC  — сколько секунд ждать одну команду beets (по умолчанию 120)
  * NAVIDROME_URL/USER/PASS — после правок библиотеки запускать пересканирование Navidrome
                         (по умолчанию http://navidrome:4533; без логина и пароля пересканирование отключено)
"""
import asyncio
import contextlib
import errno
import functools
import hashlib
import inspect
import logging
import mimetypes
import os
import re
import shlex
import shutil
import time
import unicodedata
import uuid
import weakref
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import quote

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

# Размер запроса к Telegram: кратен 4 КБ, 1 МБ делится на него нацело (требование API).
DOWNLOAD_REQUEST_SIZE = 512 * 1024
# Докачку начинаем с границы, кратной 1 МБ — иначе Telegram отклонит offset.
RESUME_ALIGN = 1024 * 1024

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

# Откуда брать имя файла: подпись к сообщению или имя из Telegram (см. should_use_caption).
CAPTION_NAMING = os.environ.get("CAPTION_NAMING", "auto").strip().lower()
if CAPTION_NAMING not in ("auto", "always", "never"):
    CAPTION_NAMING = "auto"

# Догонять сообщения, пришедшие, пока бот был выключен.
CATCH_UP = env_flag("CATCH_UP", True)

# Вопрос «музыка или фильм?», когда тип файла определить не удалось.
BUTTONS_BOT_TOKEN = os.environ.get("BUTTONS_BOT_TOKEN", "").strip()
CHOICE_TIMEOUT_SEC = int(os.environ.get("CHOICE_TIMEOUT_SEC", "300"))

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

# Временные файлы. Папка выделенная: при старте из неё удаляется всё, что осталось от прошлого запуска.
TMP_DIR = Path(os.environ.get("TMP_DIR", "/tmp/userbot-uploader"))

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
# Ожидающие выбора: ключ ("cb", token) или ("reply", chat_id, id_вопроса) → (Future с ответом, текстовые синонимы).
PENDING_CHOICES: dict = {}


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


# ---------- диск ----------
def prepare_tmp_dir() -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    removed = 0
    for p in TMP_DIR.iterdir():
        if p.is_file():
            remove_quietly(p)
            removed += 1
    if removed:
        log.info("Удалено %d недокачанных временных файлов из %s", removed, TMP_DIR)


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


# ---------- прогресс ----------
async def progress_updater(status, state: TransferState, stop_event: asyncio.Event):
    last_shown = None
    samples = deque()  # (время, байт) — для скорости по скользящему окну
    sample_key = None

    while True:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=PROGRESS_EDIT_INTERVAL_SEC)
        except asyncio.TimeoutError:
            pass
        if stop_event.is_set():
            break

        now = time.monotonic()
        total = max(state.total, 1)
        current = state.current
        phase = state.phase

        # Новая фаза/попытка → окно считаем заново (иначе после ретрая скорость «врёт»)
        key = (phase, state.attempt, state.phase_started_at)
        if key != sample_key:
            sample_key = key
            samples.clear()
            samples.append((state.phase_started_at, state.phase_start_current))
        samples.append((now, current))
        while len(samples) > 2 and samples[1][0] <= now - SPEED_WINDOW_SEC:
            samples.popleft()

        t0, c0 = samples[0]
        dt = now - t0
        speed = max((current - c0) / dt, 0.0) if dt > 0.5 else 0.0
        eta = (max(total - current, 0) / speed) if speed > 0 else float("inf")

        pct = min(100, int(current / total * 100))
        icon = "📥" if phase == "download" else "📤"
        label = "Скачиваю" if phase == "download" else "Загружаю на сервер"
        retries = UPLOAD_RETRIES if phase == "upload" else DOWNLOAD_RETRIES
        attempt_note = f" (попытка {state.attempt}/{retries})" if state.attempt > 1 else ""
        pause_note = "\n⏸ Жду перед повторной попыткой…" if state.waiting else ""

        shown = (
            f"{icon} {label} «{state.filename}»{attempt_note}\n"
            f"[{make_progress_bar(pct)}] {pct}%\n"
            f"{format_size(current)} / {format_size(total)} · {format_speed(speed)} · ~{format_eta(eta)}"
            f"{pause_note}"
        )
        if shown != last_shown:
            try:
                await status.edit(shown)
                last_shown = shown
            except Exception as e:
                log.debug("Не удалось обновить прогресс: %s", e)


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


# ---------- скачивание из Telegram (с докачкой) ----------
async def refresh_message(message):
    """Перечитываем сообщение: file reference в старом объекте мог протухнуть."""
    try:
        fresh = await asyncio.wait_for(client.get_messages(message.chat_id, ids=message.id), timeout=30)
        return fresh or message
    except Exception as e:
        log.debug("Не удалось обновить сообщение: %s", e)
        return message


async def download_resumable(message, local_path: Path, state: TransferState, attempt: int) -> None:
    """Качает документ, продолжая с места обрыва, если часть файла уже есть на диске."""
    doc = message.document
    total = doc.size or 0

    offset = 0
    if local_path.exists():
        existing = local_path.stat().st_size
        if existing > total:  # мусор от чего-то другого — начинаем заново
            remove_quietly(local_path)
        else:
            offset = (existing // RESUME_ALIGN) * RESUME_ALIGN  # Telegram требует выровненный offset
    if offset:
        log.info("Докачиваю «%s» с %s из %s", state.filename, format_size(offset), format_size(total))

    state.begin("download", attempt, current=offset, total=total or 1)
    state.disk_reserved = max(total - offset, 0)

    with open(local_path, "r+b" if offset else "wb") as f:
        if offset:
            f.seek(offset)
            f.truncate()
        async for chunk in client.iter_download(
            doc, offset=offset, request_size=DOWNLOAD_REQUEST_SIZE, file_size=total or None
        ):
            await run_blocking(f.write, chunk)
            state.current += len(chunk)
            state.disk_reserved = max(total - state.current, 0)

    state.disk_reserved = 0
    got = local_path.stat().st_size
    if total and got != total:
        remove_quietly(local_path)  # битый результат — следующая попытка начнёт с нуля
        raise TransferError(
            f"скачанный файл неполный ({format_size(got)} из {format_size(total)})", retriable=True
        )


async def download_with_retry(message, local_path: Path, state: TransferState) -> None:
    """Скачивание из Telegram с повторными попытками.
    * Каждая попытка идёт под своим watchdog-ом: зависание — это повторяемая ошибка. Недокачанный файл
      остаётся на диске, и следующая попытка продолжает с последней выровненной границы.
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
    """Имя файла обязательно кодируем: иначе «#», «?», «%» в названии ломают URL."""
    return f"{base.rstrip('/')}/{quote(filename, safe='')}"


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


def name_lock(key: str) -> asyncio.Lock:
    lock = _NAME_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _NAME_LOCKS[key] = lock
    return lock


def new_temp_name() -> str:
    # Короткое ASCII-имя без точки в начале: не упирается в лимит длины и не прячется как dotfile.
    # Расширение «.part» не распознают ни Jellyfin, ни индексатор тегов.
    return f"upload-{uuid.uuid4().hex[:12]}.part"


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

    Схема (UPLOAD_VIA_TEMP=1, по умолчанию): PUT во временный «upload-<id>.part» → проверка размера →
    MOVE на итоговое имя. Так на сервере никогда не появляется обрезанный файл под настоящим именем,
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
                put_name = new_temp_name() if use_temp else filename
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


# ---------- обработка одного файла ----------
async def transfer_file(message, kind: str, filename: str, local_path: Path, state: TransferState) -> bool:
    """Скачать → (для mp3) починить теги → залить. Возвращает True, если файл с таким именем уже был."""
    ensure_disk_space(message.document.size or 0, state)
    await download_with_retry(message, local_path, state)

    if kind == "music" and Path(filename).suffix.lower() == ".mp3":
        await run_blocking(fix_mp3_tags, str(local_path))

    url, user, pw = TARGETS[kind]
    return await upload_with_retry(url, user, pw, filename, local_path, state)


# ---------- вопросы пользователю (кнопки у бота-помощника или текстовый ответ) ----------
KIND_LABELS = {"music": "🎵 музыку", "movie": "🎬 фильмы"}
KIND_ALIASES = {
    "1": "music", "м": "music", "музыка": "music", "music": "music",
    "2": "movie", "ф": "movie", "фильм": "movie", "видео": "movie", "movie": "movie",
    "0": "cancel", "отмена": "cancel", "нет": "cancel", "cancel": "cancel",
}
YES_ALIASES = {
    "да": "yes", "д": "yes", "yes": "yes", "y": "yes", "1": "yes",
    "нет": "cancel", "н": "cancel", "no": "cancel", "n": "cancel", "0": "cancel",
    "отмена": "cancel", "cancel": "cancel",
}


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
    choice = aliases.get(event.raw_text.strip().lower())
    if choice is None or fut.done():
        return False
    fut.set_result(choice)
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


def _kind_note(choice: Optional[str]) -> str:
    if choice in KIND_LABELS:
        return f"✅ Понял, кладу в {KIND_LABELS[choice]}."
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
    "/rescan — пересканировать Navidrome"
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


LIBRARY_COMMANDS = {
    "/help": cmd_help, "/start": cmd_help,
    "/find": cmd_find, "/last": cmd_last, "/info": cmd_info,
    "/rename": cmd_rename, "/set": cmd_set, "/del": cmd_del,
    "/stats": cmd_stats, "/rescan": cmd_rescan,
}


async def handle_library_command(event) -> None:
    cmd, _, rest = event.raw_text.strip().partition(" ")
    handler = LIBRARY_COMMANDS.get(cmd.split("@")[0].lower())
    if handler is None:
        return
    if not LIBRARY_ADMIN:
        await event.reply("🔒 Управление библиотекой выключено (LIBRARY_ADMIN=0).")
        return
    try:
        await handler(event, rest.strip())
    except LibraryError as e:
        await event.reply(f"❌ {e}")
    except Exception as e:
        log.exception("library command failed")
        await event.reply(f"❌ Не получилось: {describe_error(e)}.")


@client.on(events.NewMessage(incoming=True))
async def handle_dm(event):
    if not event.is_private:
        return

    if ALLOWED_SENDER_IDS and event.sender_id not in ALLOWED_SENDER_IDS:
        log.info("Игнорирую сообщение от чужого sender_id=%s", event.sender_id)
        return

    doc = event.document
    if not event.media or not doc:
        if resolve_text_choice(event):  # ответ на вопрос бота («1/2/0», «да/нет»)
            return
        if event.raw_text and event.raw_text.startswith("/"):
            await handle_library_command(event)
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
    asked = False
    if kind is None:
        # Вопрос идёт ДО очереди загрузок, так что ожидание ответа не занимает слот.
        asked = True
        kind = await ask_kind(event, orig_filename)
        if kind is None:
            return

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

    size = doc.size or 0
    status = await event.reply(f"⏳ Принял «{filename}» ({size / 1024 / 1024:.0f} МБ), жду очереди…")

    # Имя временного файла не зависит от названия (длинные/странные имена не ломают ФС),
    # а пара sender_id + message.id уникальна между разными отправителями.
    local_path = TMP_DIR / f"{event.sender_id}_{event.message.id}.part"

    async with UPLOAD_SEMAPHORE:
        state = TransferState(filename=filename, total=size or 1)
        ACTIVE_TRANSFERS.add(state)
        stop_event = asyncio.Event()
        updater_task = asyncio.create_task(progress_updater(status, state, stop_event))

        async def stop_updater():
            # Останавливаем апдейтер ДО финального сообщения, иначе он может затереть его старым прогрессом.
            stop_event.set()
            await updater_task

        final_text = None
        try:
            # Watchdog на зависания работает внутри transfer_file — на каждой попытке скачивания/отправки.
            existed = await transfer_file(event.message, kind, filename, local_path, state)
            await stop_updater()

            note = "\n⚠️ Файл с таким именем уже был на сервере — заменён." if existed else ""
            if kind == "music":
                final_text = (
                    f"✅ Готово: «{filename}»\n"
                    f"📁 Загружено в музыку — теги проставятся автоматически в течение ~10 минут.{note}"
                )
            else:
                scan = await refresh_jellyfin()
                if scan is True:
                    suffix = "\n🔄 Сканирование Jellyfin запущено."
                elif scan is False:
                    suffix = "\n⚠️ Jellyfin не ответил — запустите сканирование библиотеки вручную."
                else:
                    suffix = ""
                final_text = f"✅ Готово: «{filename}»\n📁 Загружено в фильмы.{suffix}{note}"

        except Exception as e:
            log.exception("upload failed")
            final_text = f"❌ Не удалось загрузить «{filename}»: {describe_error(e)}."
        finally:
            ACTIVE_TRANSFERS.discard(state)
            await stop_updater()
            remove_quietly(local_path)

    if final_text:
        await safe_edit(status, final_text)


async def main():
    global http, UPLOAD_SEMAPHORE
    UPLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_TRANSFERS)
    prepare_tmp_dir()
    http = httpx.AsyncClient(
        timeout=HTTP_DEFAULT_TIMEOUT,
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=4),
    )
    if LIBRARY_ADMIN:
        if not shutil.which(BEETS_BIN):
            log.warning("Управление библиотекой: «%s» не найден — команды /find, /del и т.д. работать не будут", BEETS_BIN)
        elif not Path(BEETS_CONFIG).exists():
            log.warning("Управление библиотекой: нет конфига %s — смонтируйте папку с конфигом beets", BEETS_CONFIG)
    try:
        await start_buttons_bot()
        await client.start()
        me = await client.get_me()
        log.info(
            "Userbot started as %s (id=%s), watching DMs from %s",
            me.username, me.id, ALLOWED_SENDER_IDS or "ВСЕХ (!)",
        )
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
