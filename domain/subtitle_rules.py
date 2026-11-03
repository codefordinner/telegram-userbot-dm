"""Subtitle naming, matching, and text-encoding helpers."""

import logging
import re
from pathlib import Path
from typing import Optional

from domain.media_types import VIDEO_EXTS
from domain.naming_rules import parse_episode_from_name, series_key


log = logging.getLogger("userbot-dm-uploader")

_SUB_TAGS = {
    "ru", "rus", "russian", "en", "eng", "english", "uk", "ukr", "de", "ger", "fr", "es", "it", "pt",
    "ja", "jpn", "zh", "forced", "sdh", "default",
}


def split_sub_name(stem: str) -> tuple:
    """Split language/track suffixes from a subtitle stem."""
    tags = []
    base = stem
    while True:
        match = re.search(r"[._]([A-Za-z]{2,8})$", base)
        if not (match and match.group(1).lower() in _SUB_TAGS):
            break
        tags.insert(0, match.group(1).lower())
        base = base[: match.start()]
    return base.strip(" ._-"), tags


def pick_episode_video(files: list, season: int, episode: int) -> Optional[str]:
    for filename in sorted(files, key=str.casefold):
        if Path(filename).suffix.lower() not in VIDEO_EXTS:
            continue
        info = parse_episode_from_name(Path(filename).stem)
        if info and info["season"] == season and info["episode"] == episode:
            return filename
    return None


def _title_key(name: str) -> str:
    """Build a comparison key without a year, including years without parentheses."""
    return series_key(re.sub(r"(?<!\d)(?:19|20)\d{2}(?!\d)", " ", name))


def pick_movie_video(files: list, base: str) -> Optional[str]:
    subtitle_key = _title_key(base)
    if not subtitle_key:
        return None
    videos = sorted(
        (filename for filename in files if Path(filename).suffix.lower() in VIDEO_EXTS),
        key=str.casefold,
    )
    keyed = [(filename, _title_key(Path(filename).stem)) for filename in videos]
    exact = [filename for filename, key in keyed if key == subtitle_key]
    if exact:
        return exact[0]
    loose = [
        filename for filename, key in keyed
        if len(key) >= 6 and len(subtitle_key) >= 6
        and (key.startswith(subtitle_key) or subtitle_key.startswith(key))
    ]
    return loose[0] if len(loose) == 1 else None


def normalize_subtitle_file(
    path: str,
    recode: bool = True,
    max_bytes: int = 20 * 1024 * 1024,
    fallback_encoding: str = "cp1251",
) -> None:
    """Convert supported text subtitles to UTF-8 without BOM when needed."""
    if not recode or Path(path).suffix.lower() not in {".srt", ".ass", ".ssa", ".vtt", ".sub"}:
        return
    try:
        subtitle_path = Path(path)
        if subtitle_path.stat().st_size > max_bytes:
            return
        raw = subtitle_path.read_bytes()
    except OSError:
        return
    try:
        if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
            text, encoding = raw.decode("utf-16"), "utf-16"
        elif raw.startswith(b"\xef\xbb\xbf"):
            text, encoding = raw.decode("utf-8-sig"), "utf-8-sig"
        else:
            try:
                raw.decode("utf-8")
                return
            except UnicodeDecodeError:
                text, encoding = raw.decode(fallback_encoding), fallback_encoding
    except (UnicodeDecodeError, LookupError) as exc:
        log.warning("Не удалось определить кодировку субтитров %s: %s", path, exc)
        return
    try:
        subtitle_path.write_bytes(text.encode("utf-8"))
        log.info("Субтитры перекодированы %s → UTF-8: %s", encoding, path)
    except OSError as exc:
        log.warning("Не удалось перезаписать субтитры: %s", exc)
