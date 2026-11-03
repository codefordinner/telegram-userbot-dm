"""URL extraction and link media-type inference rules."""

import re
from typing import Optional
from urllib.parse import urlparse


_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_SKIP_URL_RE = re.compile(r"^https?://(?:www\.)?(?:t\.me|telegram\.me)/", re.IGNORECASE)
_MUSIC_HOSTS = ("music.youtube.com", "soundcloud.com", "bandcamp.com")


def extract_urls(text: str) -> list:
    urls = []
    for match in _URL_RE.finditer(text or ""):
        url = match.group(0).rstrip(".,;:!?)»”'\"")
        if url and url not in urls:
            urls.append(url)
    return urls


def is_ignored_url(url: str) -> bool:
    return bool(_SKIP_URL_RE.search(url))


def looks_like_song(info: dict) -> bool:
    """Infer a song from yt-dlp metadata, including YouTube Music topic channels."""
    categories = info.get("categories") or []
    channel = f"{info.get('channel') or ''} {info.get('uploader') or ''}"
    return bool(
        info.get("track") or info.get("artist") or info.get("artists") or info.get("album")
        or "Music" in categories or re.search(r"-\s*Topic\b", channel)
    )


def link_kind(url: str, items: Optional[list] = None, text: str = "", default_kind: str = "") -> Optional[str]:
    """Apply explicit flags, host defaults, configured default, then metadata inference."""
    lowered = text.lower()
    if "--video" in lowered:
        return "movie"
    if "--audio" in lowered or "--music" in lowered:
        return "music"
    host = (urlparse(url).hostname or "").lower()
    if any(host == candidate or host.endswith("." + candidate) for candidate in _MUSIC_HOSTS):
        return "music"
    if default_kind in ("music", "movie"):
        return default_kind
    if items and looks_like_song(items[0]):
        return "music"
    return None
