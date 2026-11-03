"""Media type classification and filename-extension rules."""

import mimetypes
import re
from pathlib import Path
from typing import Optional


AUDIO_EXTS = {".mp3", ".flac", ".m4a", ".ogg", ".opus", ".wav", ".aac", ".mka"}
VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"}
SUBTITLE_EXTS = {".srt", ".ass", ".ssa", ".vtt", ".sub"}
MP4_FAMILY_EXTS = {".mp4", ".m4v", ".mov"}
AMBIGUOUS_VIDEO_EXTS = {".ts"}

MIME_EXT = {
    "audio/mpeg": ".mp3", "audio/mp3": ".mp3", "audio/mp4": ".m4a", "audio/x-m4a": ".m4a",
    "audio/aac": ".aac", "audio/ogg": ".ogg", "audio/opus": ".opus", "audio/flac": ".flac",
    "audio/x-flac": ".flac", "audio/wav": ".wav", "audio/x-wav": ".wav",
    "video/mp4": ".mp4", "video/x-matroska": ".mkv", "video/webm": ".webm",
    "video/quicktime": ".mov", "video/x-msvideo": ".avi", "video/mp2t": ".ts",
}

_VALID_EXT_RE = re.compile(r"\.[A-Za-z0-9]{1,5}")


def classify(
    filename: str,
    mime_type: Optional[str] = None,
    size: int = 0,
    has_video_attr: bool = False,
    has_audio_attr: bool = False,
    ambiguous_min_bytes: int = 5 * 1024 * 1024,
) -> Optional[str]:
    """Classify media, preferring Telegram attributes and known extensions."""
    ext = Path(filename).suffix.lower()
    mime = (mime_type or "").lower()
    if ext in SUBTITLE_EXTS:
        return "subtitle"
    if ext in AUDIO_EXTS:
        return "music"
    if ext in VIDEO_EXTS:
        if has_video_attr:
            return "movie"
        if has_audio_attr or mime.startswith("audio/"):
            return "music"
        return "movie"
    if ext in AMBIGUOUS_VIDEO_EXTS:
        return "movie" if (has_video_attr or size >= ambiguous_min_bytes) else None
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


def ensure_extension(filename: str, kind: str, mime_type: Optional[str]) -> tuple:
    """Ensure a filename has a plausible extension, preserving meaningful stems."""
    ext = Path(filename).suffix
    if _VALID_EXT_RE.fullmatch(ext):
        return filename, ext
    ext = ext_from_mime(mime_type) or (".mp4" if kind == "movie" else ".mp3")
    return filename + ext, ext


def align_extension(filename: str, ext: str, kind: str, mime_type: Optional[str]) -> tuple:
    """For an uninformative filename, prefer its Telegram MIME extension when compatible."""
    allowed = AUDIO_EXTS if kind == "music" else (VIDEO_EXTS | AMBIGUOUS_VIDEO_EXTS)
    if ext.lower() in allowed:
        return filename, ext
    new_ext = ext_from_mime(mime_type)
    if new_ext in allowed:
        return filename[:-len(ext)] + new_ext, new_ext
    return filename, ext
