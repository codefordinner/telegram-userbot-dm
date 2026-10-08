import asyncio
import logging
import os
import re
import shutil
import time
from pathlib import Path

import httpx
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from mutagen.id3 import ID3
from mutagen import MutagenError

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("userbot-dm-uploader")

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

AUDIO_EXTS = {".mp3", ".flac", ".m4a", ".ogg", ".opus", ".wav", ".aac"}
VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".ts", ".m4v"}

UPLOAD_RETRIES = 3
DOWNLOAD_RETRIES = 3
MAX_FLOOD_WAITS = 5
RETRY_BACKOFF_SEC = 5
PROGRESS_EDIT_INTERVAL_SEC = 4
UPLOAD_CHUNK_SIZE = 1024 * 1024

# Не больше N файлов одновременно скачиваем/грузим — бережём диск в /tmp и канал.
MAX_CONCURRENT_TRANSFERS = 2
UPLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_TRANSFERS)

# Общий таймаут на обработку одного файла (скачивание + заливка), чтобы бот не подвисал
# навечно и не держал слот в семафоре, если Telegram/copyparty перестали отвечать без ошибки.
MIN_TIMEOUT_SEC = 120
PER_MB_TIMEOUT_SEC = 2.4  # ~ запас на медленную сеть для download+upload суммарно

# Жёсткий пол: после записи файла на диске должно остаться не меньше этого объёма свободно,
# независимо от размера конкретного файла. Актуально, пока сервер стоит на маленьком диске —
# не даём боту забить систему под ноль и уронить ОС/другие сервисы.
MIN_FREE_DISK_AFTER_BYTES = int(1.5 * 1024 ** 3)  # 1.5 ГБ

client = TelegramClient(SESSION_PATH, API_ID, API_HASH)


def classify(filename: str, mime_type: str = None):
    ext = Path(filename).suffix.lower()
    if ext in AUDIO_EXTS:
        return "music"
    if ext in VIDEO_EXTS:
        return "movie"
    # Расширение не распознали — подстрахуемся по mime_type, если Telegram его прислал.
    if mime_type:
        if mime_type.startswith("audio/"):
            return "music"
        if mime_type.startswith("video/"):
            return "movie"
    return None


def sanitize_name(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|]', "", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip(" .")


def safe_basename(name: str) -> str:
    """Убираем путь и потенциальный path traversal (../, /, \\) из имени файла,
    пришедшего от Telegram — оно используется как часть локального пути и URL."""
    name = os.path.basename(name.replace("\\", "/"))
    name = name.lstrip(".")  # чтобы не получить скрытый файл или пустое имя через ведущие точки
    return name or "file"


def has_enough_space(dir_path: str, needed_bytes: int) -> bool:
    """Проверяем, что на диске есть место с запасом 10% под скачиваемый файл,
    и что после записи на диске всё равно останется жёсткий минимум
    (MIN_FREE_DISK_AFTER_BYTES) — чтобы не забить систему под ноль.
    Если не смогли проверить (например, папка ещё не существует) — не блокируем."""
    if not needed_bytes:
        return True
    try:
        usage = shutil.disk_usage(dir_path)
    except OSError as e:
        log.warning("Не удалось проверить свободное место в %s: %s", dir_path, e)
        return True

    has_margin = usage.free > needed_bytes * 1.1
    projected_free_after_write = usage.free - needed_bytes
    keeps_hard_floor = projected_free_after_write > MIN_FREE_DISK_AFTER_BYTES
    return has_margin and keeps_hard_floor


def compute_timeout(size_bytes: int) -> float:
    size_mb = (size_bytes or 0) / 1024 / 1024
    return max(MIN_TIMEOUT_SEC, size_mb * PER_MB_TIMEOUT_SEC)


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


# ---------- парсинг подписи для видео (фильм/сериал) ----------
# Формат непредсказуем по содержанию, но структура обычно такая:
#   🆔 `id`                              (опционально, игнорируем)
#   Название (доп.инфо [качество])       (обязательная первая содержательная строка)
#   N сезон M серия                      (опционально, только для сериалов)
#   «Название серии»                     (опционально)
#   [ссылки/реклама источника]           (игнорируем — строки со скобками-ссылками/URL)
def parse_video_caption(text: str):
    if not text:
        return None

    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    lines = [l for l in lines if not l.startswith("🆔")]
    if not lines:
        return None

    title_line = lines[0]
    m = re.match(r"^(.*?)\s*\((.*)\)\s*$", title_line)
    if not m:
        # нет скобок с доп.инфой — берём строку как есть, это тоже нормальное название
        title = sanitize_name(title_line)
        if not title:
            return None
        quality = None
        extra_lines = lines[1:]
    else:
        title = sanitize_name(m.group(1))
        if not title:
            return None
        info = m.group(2)
        q = re.search(r"\[(\d+p)\]", info)
        quality = q.group(1) if q else None
        extra_lines = lines[1:]

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
        "quality": quality,
        "season": season,
        "episode": episode,
        "episode_title": episode_title,
    }


def build_filename_from_video_caption(parsed: dict, ext: str) -> str:
    base = parsed["title"]
    if parsed["season"] and parsed["episode"]:
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

    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    if not lines:
        return None

    first = lines[0]
    # убираем служебные пометки в скобках вроде "(в файле)", не трогая содержательные
    first = re.sub(r"\(\s*в\s*файле\s*\)", "", first, flags=re.IGNORECASE).strip()

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


def clean_id3_junk(path: str) -> None:
    """Убирает мусорные ID3-фреймы (пустые USLT/UFID от pesni.fm),
    которые ломают сохранение тегов в tagr (node-taglib-sharp)."""
    try:
        tag = ID3(path)
    except Exception as e:
        # Битые/нестандартные mp3 могут кидать не только MutagenError
        # (KeyError, struct.error и т.п.) — это не критичный путь, просто пропускаем.
        log.warning("Не удалось прочитать ID3-теги для очистки: %s: %s", path, e)
        return

    changed = False
    for key in list(tag.keys()):
        if key.startswith("USLT"):
            frame = tag[key]
            if not frame.text or frame.lang == "XXX":
                del tag[key]
                changed = True
        elif key.startswith("UFID"):
            frame = tag[key]
            if not frame.data:
                del tag[key]
                changed = True

    if changed:
        try:
            tag.save(path)
            log.info("Очищены мусорные ID3-фреймы: %s", path)
        except Exception as e:
            log.warning("Не удалось сохранить очищенные ID3-теги: %s: %s", path, e)


async def progress_updater(status, state, stop_event):
    last_shown = None
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=PROGRESS_EDIT_INTERVAL_SEC)
        except asyncio.TimeoutError:
            pass
        if stop_event.is_set():
            break

        total = state.get("total") or 1
        current = state.get("current") or 0
        pct = min(100, int(current / total * 100)) if total else 0
        phase = state.get("phase")
        icon = "📥" if phase == "download" else "📤"
        label = "Скачиваю" if phase == "download" else "Загружаю на сервер"
        retries = UPLOAD_RETRIES if phase == "upload" else DOWNLOAD_RETRIES
        attempt = state.get("attempt", 1)
        attempt_note = f" (попытка {attempt}/{retries})" if attempt > 1 else ""

        started_at = state.get("phase_started_at", time.monotonic())
        elapsed = max(time.monotonic() - started_at, 0.001)
        speed = current / elapsed
        eta = (max(total - current, 0) / speed) if speed > 0 else float("inf")

        bar = make_progress_bar(pct)
        shown = (
            f"{icon} {label} «{state['filename']}»{attempt_note}\n"
            f"[{bar}] {pct}%\n"
            f"{format_size(current)} / {format_size(total)} · {format_speed(speed)} · ~{format_eta(eta)}"
        )
        if shown != last_shown:
            try:
                await status.edit(shown)
                last_shown = shown
            except Exception:
                pass


async def download_with_retry(message, local_path, state):
    """Скачивание из Telegram с повторными попытками при обрыве соединения
    и корректной обработкой FloodWaitError (ждём ровно столько, сколько просит Telegram,
    не тратя на это обычный лимит попыток)."""
    last_exc = None
    attempt = 0
    flood_waits = 0
    while attempt < DOWNLOAD_RETRIES:
        attempt += 1
        state["attempt"] = attempt
        state["current"] = 0
        state["phase_started_at"] = time.monotonic()
        try:
            def dl_progress(current, total):
                state["current"] = current
                state["total"] = total or state["total"]

            await client.download_media(message, file=local_path, progress_callback=dl_progress)
            return
        except asyncio.CancelledError:
            raise
        except FloodWaitError as e:
            flood_waits += 1
            if os.path.exists(local_path):
                os.remove(local_path)
            if flood_waits > MAX_FLOOD_WAITS:
                raise
            log.warning("Telegram просит подождать %s сек (FloodWait), ждём и пробуем снова", e.seconds)
            await asyncio.sleep(e.seconds + 1)
            attempt -= 1  # честный FloodWait не считаем за неудачную попытку
            last_exc = e
        except Exception as e:
            last_exc = e
            log.warning("Попытка скачивания %s/%s не удалась: %s", attempt, DOWNLOAD_RETRIES, e)
            if os.path.exists(local_path):
                os.remove(local_path)
            if attempt < DOWNLOAD_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF_SEC * attempt)
    raise last_exc


async def upload_stream(path, state):
    sent = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(UPLOAD_CHUNK_SIZE)
            if not chunk:
                break
            sent += len(chunk)
            state["current"] = sent
            yield chunk
            await asyncio.sleep(0)


async def check_duplicate(url, user, pw, filename) -> bool:
    """Проверяем, существует ли уже файл с таким именем на сервере (просто для лога —
    не блокируем загрузку, но хотим знать, что произошла перезапись)."""
    try:
        async with httpx.AsyncClient(auth=(user, pw), timeout=10) as http:
            r = await http.head(f"{url}/{filename}")
        return r.status_code == 200
    except Exception as e:
        log.warning("Не удалось проверить наличие дубликата «%s»: %s", filename, e)
        return False


async def upload_with_retry(url, user, pw, filename, local_path, state):
    last_exc = None
    file_size = os.path.getsize(local_path)
    for attempt in range(1, UPLOAD_RETRIES + 1):
        state["attempt"] = attempt
        state["current"] = 0
        state["phase_started_at"] = time.monotonic()
        try:
            async with httpx.AsyncClient(auth=(user, pw), timeout=httpx.Timeout(None, connect=30)) as http:
                resp = await http.put(
                    f"{url}/{filename}",
                    content=upload_stream(local_path, state),
                    # Явно указываем размер, чтобы не уходить в Transfer-Encoding: chunked —
                    # некоторые связки copyparty/прокси плохо с ним работают.
                    headers={"Content-Length": str(file_size)},
                )
            return resp
        except (httpx.TransportError, httpx.ReadError) as e:
            last_exc = e
            log.warning("Попытка загрузки %s/%s не удалась: %s", attempt, UPLOAD_RETRIES, e)
            if attempt < UPLOAD_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF_SEC * attempt)
    raise last_exc


@client.on(events.NewMessage(incoming=True))
async def handle_dm(event):
    if not event.is_private:
        return

    if ALLOWED_SENDER_IDS and event.sender_id not in ALLOWED_SENDER_IDS:
        log.info("Игнорирую сообщение от чужого sender_id=%s", event.sender_id)
        return

    if not event.media or not event.document:
        return

    orig_filename = None
    for attr in event.document.attributes:
        if hasattr(attr, "file_name") and attr.file_name:
            orig_filename = attr.file_name

    if not orig_filename:
        return

    # Не доверяем имени файла из Telegram буквально — убираем путь/traversal,
    # т.к. оно идёт и в локальный путь на диске, и (при отсутствии подписи) в имя на сервере.
    orig_filename = safe_basename(orig_filename)

    kind = classify(orig_filename, getattr(event.document, "mime_type", None))
    if kind is None:
        await event.reply("🤷 Не понимаю, что это за файл — жду музыку (🎵 mp3/flac/…) или видео (🎬 mp4/mkv/…).")
        return

    filename = orig_filename
    ext = Path(orig_filename).suffix or (".mp4" if kind == "movie" else ".mp3")

    if kind == "movie":
        parsed = parse_video_caption(event.raw_text)
        if parsed:
            filename = build_filename_from_video_caption(parsed, ext)
            log.info("Распознал подпись видео: %s -> %s", parsed, filename)
    else:
        parsed = parse_music_caption(event.raw_text)
        if parsed:
            filename = build_filename_from_music_caption(parsed, ext)
            log.info("Распознал подпись трека: %s -> %s", parsed, filename)

    size_mb = event.document.size / 1024 / 1024
    status = await event.reply(f"⏳ Принял «{filename}» ({size_mb:.0f} МБ), жду очереди…")

    # Проверка места на диске временно отключена по просьбе — не блокируем загрузку.
    # Функция has_enough_space() осталась в коде, легко включить обратно при необходимости.

    # Уникальный путь на случай нескольких файлов с одинаковым именем одновременно.
    local_path = f"/tmp/{event.message.id}_{orig_filename}"

    async with UPLOAD_SEMAPHORE:
        state = {
            "phase": "download",
            "current": 0,
            "total": event.document.size or 1,
            "filename": filename,
            "attempt": 1,
            "phase_started_at": time.monotonic(),
        }
        stop_event = asyncio.Event()
        updater_task = asyncio.create_task(progress_updater(status, state, stop_event))

        async def process():
            await download_with_retry(event.message, local_path, state)

            if kind == "music":
                clean_id3_junk(local_path)

            state["phase"] = "upload"
            state["current"] = 0
            state["phase_started_at"] = time.monotonic()
            state["total"] = os.path.getsize(local_path) or 1

            if kind == "music":
                url, user, pw = MUSIC_URL, MUSIC_USER, MUSIC_PASS
            else:
                url, user, pw = MOVIES_URL, MOVIES_USER, MOVIES_PASS

            if await check_duplicate(url, user, pw, filename):
                log.warning("Файл «%s» уже существует на сервере — будет перезаписан", filename)

            return await upload_with_retry(url, user, pw, filename, local_path, state)

        try:
            resp = await process()

            os.remove(local_path)

            if resp.status_code not in (200, 201, 204):
                log.error("upload failed: %s %s", resp.status_code, resp.text[:300])
                await status.edit(f"❌ Ошибка загрузки «{filename}» на сервер (код {resp.status_code}).")
                return

            if kind == "music":
                await status.edit(
                    f"✅ Готово: «{filename}»\n"
                    f"📁 Загружено в музыку — теги проставятся автоматически в течение ~10 минут."
                )
            else:
                ok = None
                if JELLYFIN_API_KEY:
                    async with httpx.AsyncClient(timeout=30) as http:
                        r = await http.post(f"{JELLYFIN_URL}/Library/Refresh", params={"api_key": JELLYFIN_API_KEY})
                    ok = r.status_code in (200, 204)
                suffix = "\n🔄 Сканирование Jellyfin запущено." if ok else ""
                await status.edit(f"✅ Готово: «{filename}»\n📁 Загружено в фильмы.{suffix}")

        except Exception:
            log.exception("upload failed")
            if os.path.exists(local_path):
                os.remove(local_path)
            await status.edit(f"❌ Не получилось загрузить «{filename}» после нескольких попыток. Попробуйте ещё раз позже.")
        finally:
            stop_event.set()
            await updater_task


async def main():
    await client.start()
    me = await client.get_me()
    log.info("Userbot started as %s (id=%s), watching DMs from %s", me.username, me.id, ALLOWED_SENDER_IDS or "ВСЕХ (!)")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
