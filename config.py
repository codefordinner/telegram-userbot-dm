"""Runtime configuration for the Telegram uploader.

Environment variable names and defaults are kept compatible with existing deployments.
"""

from dataclasses import dataclass, field
import os
import shlex
from pathlib import Path


def _required(name: str) -> str:
    try:
        return os.environ[name]
    except KeyError as exc:
        raise ValueError(f"Required environment variable {name} is missing") from exc


def _flag(name: str, default: bool = True) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in ("0", "false", "no", "off", "")


def _download_request_size() -> int:
    """Preserve Telegram's 4 KB alignment and 1 MB boundary constraints."""
    try:
        size = int(float(os.environ.get("DOWNLOAD_REQUEST_KB", "1024")) * 1024)
    except ValueError:
        return 512 * 1024
    if size <= 0 or size % 4096 or (1024 * 1024) % size:
        return 512 * 1024
    return size


def _caption_naming() -> str:
    value = os.environ.get("CAPTION_NAMING", "auto").strip().lower()
    return value if value in ("auto", "always", "never") else "auto"


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str = field(repr=False)
    session_path: str = "/session/uploader"
    music_url: str = "http://copyparty:3923"
    music_user: str = field(default="", repr=False)
    music_pass: str = field(default="", repr=False)
    movies_url: str = "http://copyparty-movies:3923"
    movies_user: str = field(default="", repr=False)
    movies_pass: str = field(default="", repr=False)
    allowed_sender_ids: set[int] = field(default_factory=set)
    jellyfin_url: str = "http://jellyfin:8096"
    jellyfin_api_key: str = field(default="", repr=False)
    ambiguous_min_bytes: int = 5 * 1024 * 1024
    max_flood_wait_sec: int = 600
    download_request_size: int = 1024 * 1024
    download_workers: int = 4
    stall_timeout_sec: int = 180
    upload_via_temp: bool = True
    caption_naming: str = "auto"
    catch_up: bool = True
    buttons_bot_token: str = field(default="", repr=False)
    choice_timeout_sec: int = 300
    music_video_max_min: float = 12.0
    ffmpeg_bin: str = "ffmpeg"
    ffprobe_bin: str = "ffprobe"
    ffmpeg_timeout_sec: int = 600
    library_admin: bool = True
    beets_bin: str = "beet"
    beets_config: str = "/beets-cfg/config.yaml"
    beets_plugins: str = "scrub"
    beets_timeout_sec: int = 120
    navidrome_url: str = "http://navidrome:4533"
    navidrome_user: str = ""
    navidrome_pass: str = field(default="", repr=False)
    tmp_dir: Path = Path("/tmp/userbot-uploader")
    resume_on_start: bool = True
    resume_max_age_sec: int = 24 * 3600
    batch_grace_sec: float = 4.0
    check_disk: bool = True
    min_free_disk_after_bytes: int = int(1.5 * 1024 ** 3)
    history_db: Path = Path("/tmp/userbot-uploader/state/history.sqlite3")
    duplicate_check: bool = True
    link_ingest: bool = True
    ytdlp_bin: str = "yt-dlp"
    ytdlp_cookies: str = field(default="", repr=False)
    ytdlp_extra_args: list[str] = field(default_factory=list, repr=False)
    ytdlp_timeout_sec: int = 1800
    link_max_entries: int = 100
    link_video_max_height: int = 1080
    link_embed_thumb: bool = True
    subtitle_recode: bool = True
    subtitle_fallback_encoding: str = "cp1251"
    link_default_kind: str = ""

    @property
    def targets(self) -> dict[str, tuple[str, str, str]]:
        return {
            "music": (self.music_url, self.music_user, self.music_pass),
            "movie": (self.movies_url, self.movies_user, self.movies_pass),
        }

    @classmethod
    def from_environment(cls) -> "Settings":
        raw_api_id = _required("TG_API_ID")
        try:
            api_id = int(raw_api_id)
        except ValueError as exc:
            raise ValueError("TG_API_ID must be an integer") from exc
        if api_id <= 0:
            raise ValueError("TG_API_ID must be a positive integer")

        return cls(
            api_id=api_id,
            api_hash=_required("TG_API_HASH"),
            session_path=os.environ.get("SESSION_PATH", "/session/uploader"),
            music_url=os.environ.get("MUSIC_COPYPARTY_URL", "http://copyparty:3923"),
            music_user=_required("MUSIC_COPYPARTY_USER"),
            music_pass=_required("MUSIC_COPYPARTY_PASS"),
            movies_url=os.environ.get("MOVIES_COPYPARTY_URL", "http://copyparty-movies:3923"),
            movies_user=_required("MOVIES_COPYPARTY_USER"),
            movies_pass=_required("MOVIES_COPYPARTY_PASS"),
            allowed_sender_ids={int(x) for x in os.environ.get("ALLOWED_SENDER_IDS", "").split(",") if x.strip()},
            jellyfin_url=os.environ.get("JELLYFIN_URL", "http://jellyfin:8096"),
            jellyfin_api_key=os.environ.get("JELLYFIN_API_KEY", ""),
            ambiguous_min_bytes=int(float(os.environ.get("AMBIGUOUS_MIN_MB", "5")) * 1024 * 1024),
            max_flood_wait_sec=int(os.environ.get("MAX_FLOOD_WAIT_SEC", "600")),
            download_request_size=_download_request_size(),
            download_workers=max(1, int(os.environ.get("DOWNLOAD_WORKERS", "4"))),
            stall_timeout_sec=int(os.environ.get("STALL_TIMEOUT_SEC", "180")),
            upload_via_temp=_flag("UPLOAD_VIA_TEMP", True),
            caption_naming=_caption_naming(),
            catch_up=_flag("CATCH_UP", True),
            buttons_bot_token=os.environ.get("BUTTONS_BOT_TOKEN", "").strip(),
            choice_timeout_sec=int(os.environ.get("CHOICE_TIMEOUT_SEC", "300")),
            music_video_max_min=float(os.environ.get("MUSIC_VIDEO_MAX_MIN", "12")),
            ffmpeg_bin=os.environ.get("FFMPEG_BIN", "ffmpeg"),
            ffprobe_bin=os.environ.get("FFPROBE_BIN", "ffprobe"),
            ffmpeg_timeout_sec=int(os.environ.get("FFMPEG_TIMEOUT_SEC", "600")),
            library_admin=_flag("LIBRARY_ADMIN", True),
            beets_bin=os.environ.get("BEETS_BIN", "beet"),
            beets_config=os.environ.get("BEETS_CONFIG", "/beets-cfg/config.yaml"),
            beets_plugins=os.environ.get("BEETS_PLUGINS", "scrub"),
            beets_timeout_sec=int(os.environ.get("BEETS_TIMEOUT_SEC", "120")),
            navidrome_url=os.environ.get("NAVIDROME_URL", "http://navidrome:4533"),
            navidrome_user=os.environ.get("NAVIDROME_USER", ""),
            navidrome_pass=os.environ.get("NAVIDROME_PASS", ""),
            tmp_dir=Path(os.environ.get("TMP_DIR", "/tmp/userbot-uploader")),
            resume_on_start=_flag("RESUME_ON_START", True),
            resume_max_age_sec=int(float(os.environ.get("RESUME_MAX_AGE_HOURS", "24")) * 3600),
            batch_grace_sec=float(os.environ.get("BATCH_GRACE_SEC", "4")),
            check_disk=_flag("CHECK_DISK", True),
            min_free_disk_after_bytes=int(float(os.environ.get("MIN_FREE_DISK_GB", "1.5")) * 1024 ** 3),
            history_db=Path(os.environ.get("HISTORY_DB", str(Path(os.environ.get("TMP_DIR", "/tmp/userbot-uploader")) / "state" / "history.sqlite3"))),
            duplicate_check=_flag("DUPLICATE_CHECK", True),
            link_ingest=_flag("LINK_INGEST", True),
            ytdlp_bin=os.environ.get("YTDLP_BIN", "yt-dlp"),
            ytdlp_cookies=os.environ.get("YTDLP_COOKIES", "").strip(),
            ytdlp_extra_args=shlex.split(os.environ.get("YTDLP_EXTRA_ARGS", "")),
            ytdlp_timeout_sec=int(os.environ.get("YTDLP_TIMEOUT_SEC", "1800")),
            link_max_entries=max(1, int(os.environ.get("LINK_MAX_ENTRIES", "100"))),
            link_video_max_height=int(os.environ.get("LINK_VIDEO_MAX_HEIGHT", "1080")),
            link_embed_thumb=_flag("LINK_EMBED_THUMB", True),
            subtitle_recode=_flag("SUBTITLE_RECODE", True),
            subtitle_fallback_encoding=os.environ.get("SUBTITLE_FALLBACK_ENCODING", "cp1251").strip() or "cp1251",
            link_default_kind=os.environ.get("LINK_DEFAULT_KIND", "").strip().lower(),
        )
