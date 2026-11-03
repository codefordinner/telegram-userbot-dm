"""Caption parsing and filename rules for music, movies, and TV series."""

import re
import unicodedata
from pathlib import Path
from typing import Optional

from domain.filename_utils import normalize_text, sanitize_name


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


def should_use_caption(kind: str, orig_filename: str, parsed: dict, caption_naming: str = "auto") -> bool:
    """Подпись должна перебивать имя из Telegram не всегда: если файл уже называется нормально
    («Исполнитель - Название.mp3», «Film.2021.1080p.mkv»), подпись с рекламой или шумом его только испортит.
    Режим задаётся CAPTION_NAMING: auto (по умолчанию) / always / never."""
    if caption_naming == "never":
        return False
    if caption_naming == "always":
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


def detect_series(orig_filename: str, parsed: Optional[dict], caption_naming: str = "auto"):
    """Серия сериала или нет. Возвращает dict {show, season, episode, episode_title, quality, name_ok} или None.
    name_ok=True — имя файла уже хорошее и годится как есть; иначе имя надо собрать (build_series_filename).
    show может быть пустым — тогда вызывающий спросит пользователя."""
    stem = Path(orig_filename).stem
    from_name = parse_episode_from_name(stem)
    cap = None
    if (
        parsed and caption_naming != "never"
        and parsed.get("season") is not None and parsed.get("episode") is not None
    ):
        cap = parsed

    if cap and (from_name is None or should_use_caption("movie", orig_filename, cap, caption_naming)):
        return {
            "show": clean_show_name(cap["title"]), "season": cap["season"], "episode": cap["episode"],
            "episode_title": cap.get("episode_title"), "quality": cap.get("quality"), "name_ok": False,
        }
    if from_name:
        show = from_name["show"]
        name_ok = True
        if (not show or is_generic_name(show)) and parsed and parsed.get("title") and caption_naming != "never":
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
