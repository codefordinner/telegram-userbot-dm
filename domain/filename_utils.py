"""Pure filename normalization, sanitizing, and Unicode repair helpers."""

import os
import re
import unicodedata
from pathlib import Path
from typing import Optional


MAX_FILENAME_BYTES = 200
MAX_DIRNAME_BYTES = 120
_CYRILLIC_RE = re.compile(r"[А-Яа-яЁёІіЇїЄєҐґЎў]")
_CYRILLIC_VOWEL_RE = re.compile(r"[аеёиоуыэюяіїє]", re.IGNORECASE)


def normalize_text(text: str) -> str:
    """Remove control/invisible characters and normalize Unicode to NFC."""
    text = re.sub(r"[\x00-\x1f\x7f]", " ", text)
    text = re.sub(r"[\u200b-\u200f\u202a-\u202e\u2060\ufeff]", "", text)
    return unicodedata.normalize("NFC", text)


def sanitize_name(name: str) -> str:
    name = normalize_text(name)
    name = re.sub(r'[\\/:*?"<>|]', "", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip(" .")


def safe_basename(name: str) -> str:
    """Strip path components and leading dots from an externally supplied name."""
    name = os.path.basename(name.replace("\\", "/"))
    name = name.lstrip(".")
    return name or "file"


def truncate_filename(name: str, max_bytes: int = MAX_FILENAME_BYTES) -> str:
    """Shorten a UTF-8 filename to max_bytes while preserving its extension."""
    if len(name.encode("utf-8")) <= max_bytes:
        return name
    ext = Path(name).suffix
    stem = name[: -len(ext)] if ext else name
    budget = max(max_bytes - len(ext.encode("utf-8")), 1)
    stem = stem.encode("utf-8")[:budget].decode("utf-8", errors="ignore").rstrip(" .")
    return (stem or "file") + ext


def truncate_dirname(name: str, max_bytes: int = MAX_DIRNAME_BYTES) -> str:
    """Shorten a UTF-8 directory name to max_bytes."""
    if len(name.encode("utf-8")) <= max_bytes:
        return name
    cut = name.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore").rstrip(" .")
    return cut or "folder"


def _decode_cp1251_word(word: str) -> Optional[tuple]:
    """Try decoding a Latin-1 representation of a cp1251 word."""
    letters = [char for char in word if char.isalpha()]
    if len(letters) < 2 or not all(0x80 <= ord(char) <= 0xFF for char in letters):
        return None
    try:
        fixed = word.encode("latin-1").decode("cp1251")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return None
    fixed_letters = [char for char in fixed if char.isalpha()]
    if not fixed_letters or not all(_CYRILLIC_RE.fullmatch(char) for char in fixed_letters):
        return None
    core = "".join(fixed_letters)
    if len(core) >= 3 and not _CYRILLIC_VOWEL_RE.search(core):
        return None
    return fixed, len(core)


def _fix_cp1251_text(text: str) -> str:
    fixes = {}
    for match in re.finditer(r"\S+", text):
        word = match.group(0)
        if word not in fixes:
            result = _decode_cp1251_word(word)
            if result:
                fixes[word] = result
    if not any(length >= 3 for _, length in fixes.values()):
        return text
    return re.sub(r"\S+", lambda match: fixes[match.group(0)][0] if match.group(0) in fixes else match.group(0), text)


def fix_mojibake(text: str) -> str:
    """Repair common UTF-8/cp1251 text decoded as Latin-1 or cp1252."""
    if not text or text.isascii():
        return text
    for wrong in ("latin-1", "cp1252"):
        try:
            fixed = text.encode(wrong).decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
        if fixed != text:
            return fixed
    return _fix_cp1251_text(text)


def clean_filename(name: str) -> str:
    """Repair encoding, strip paths, and remove forbidden filename characters."""
    name = sanitize_name(safe_basename(normalize_text(fix_mojibake(name))))
    return name or "file"
