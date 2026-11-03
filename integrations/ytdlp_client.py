"""yt-dlp subprocess client used for link probing and downloads."""

import asyncio
import contextlib
import json
import logging
import re
from urllib.parse import parse_qs, urlparse

from domain.filename_utils import normalize_text, sanitize_name
from domain.transfer_models import TransferError, TransferState


log = logging.getLogger("userbot-dm-uploader")
_TITLE_NOISE_RE = re.compile(
    r"\s*[\(\[]\s*(?:official\s+(?:music\s+)?(?:video|audio|lyric\s+video)|lyrics?(?:\s+video)?|audio|video|hd|hq|4k)\s*[\)\]]",
    re.IGNORECASE,
)
_PERMANENT_ERROR_RE = re.compile(
    r"private video|video unavailable|not available|copyright|sign in|age-restricted|members-only|"
    r"geo.?restricted|unsupported url|login required|premiere",
    re.IGNORECASE,
)
_ARTIST_TITLE_RE = re.compile(r"^(.+?)\s+-\s+(.+)$")


class YtDlpClient:
    def __init__(self, binary: str, cookies: str, extra_args: list, timeout_sec: int):
        self.binary = binary
        self.cookies = cookies
        self.extra_args = list(extra_args)
        self.timeout_sec = timeout_sec

    def base_args(self) -> list:
        args = [
            self.binary, "--ignore-config", "--no-warnings",
            "--retries", "5", "--fragment-retries", "5", "--socket-timeout", "30",
        ]
        if self.cookies:
            args += ["--cookies", self.cookies]
        return args + self.extra_args

    @staticmethod
    def error(err: str, rc: int) -> TransferError:
        lines = re.findall(r"ERROR:\s*(.+)", err)
        tail = err.strip().splitlines()[-1] if err.strip() else f"код {rc}"
        reason = (lines[-1] if lines else tail)[:200]
        hint = ""
        if re.search(r"sign in|not a bot|cookies", err, re.IGNORECASE):
            hint = " (YouTube просит авторизацию — задайте YTDLP_COOKIES)"
        return TransferError(f"yt-dlp: {reason}{hint}", retriable=not _PERMANENT_ERROR_RE.search(err))

    async def run_capture(self, command: list, timeout: int) -> tuple:
        try:
            proc = await asyncio.create_subprocess_exec(
                *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
        except FileNotFoundError:
            raise TransferError(f"в образе бота нет «{command[0]}» — добавьте yt-dlp в requirements.txt") from None
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()
            raise TransferError(f"«{command[0]}» не ответил за {timeout} сек") from None
        return proc.returncode, out.decode("utf-8", errors="replace"), err.decode("utf-8", errors="replace")

    @staticmethod
    def is_single_video_url(url: str) -> bool:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if host == "youtu.be":
            return True
        return host.endswith("youtube.com") and parsed.path == "/watch" and "v" in parse_qs(parsed.query)

    @staticmethod
    def entry_url(entry: dict):
        for key in ("webpage_url", "url"):
            url = entry.get(key)
            if isinstance(url, str) and url.startswith("http"):
                return url
        extractor = str(entry.get("ie_key") or entry.get("extractor_key") or "").lower()
        if entry.get("id") and extractor.startswith("youtube"):
            return f"https://www.youtube.com/watch?v={entry['id']}"
        return None

    async def probe_link(self, url: str) -> tuple:
        """Return (playlist title, entries) with a concrete URL on each entry."""
        args = self.base_args() + ["-J", "--flat-playlist", "--skip-download"]
        if self.is_single_video_url(url):
            args.append("--no-playlist")
        args.append(url)
        rc, out, err = await self.run_capture(args, 120)
        if rc != 0:
            raise self.error(err, rc)
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
        for entry in entries:
            if isinstance(entry, dict):
                entry_url = self.entry_url(entry)
                if entry_url:
                    items.append(dict(entry, _url=entry_url))
        return info.get("title") or "", items

    @staticmethod
    def music_meta_from_info(info: dict, fallback: str = "") -> tuple:
        artist = info.get("artist") or ""
        if not artist and isinstance(info.get("artists"), list):
            artist = ", ".join(str(value) for value in info["artists"] if value)
        track = info.get("track")
        title = track or _TITLE_NOISE_RE.sub("", info.get("title") or "") or fallback
        if not artist:
            match = _ARTIST_TITLE_RE.match(title)
            if match:
                artist, title = match.group(1), match.group(2)
        if not artist:
            artist = re.sub(r"\s*-\s*Topic$", "", info.get("uploader") or info.get("channel") or "")
        artist = sanitize_name(artist) or "Unknown Artist"
        title = sanitize_name(title) or "Untitled"
        album = normalize_text(info.get("album") or "").strip()
        return artist, title, album

    @staticmethod
    def _to_int(value: str):
        try:
            return int(float(value))
        except ValueError:
            return None

    async def run_download(self, args: list, state: TransferState) -> None:
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
                    done = self._to_int(parts[1])
                    total = self._to_int(parts[2]) or self._to_int(parts[3])
                    if done is not None:
                        state.current = done
                    state.total = max(total or 0, state.current, 1)
            return await proc.wait()

        def kill() -> None:
            if proc.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()

        try:
            rc = await asyncio.wait_for(pump(), timeout=self.timeout_sec)
        except asyncio.TimeoutError:
            kill()
            err_task.cancel()
            raise TransferError(f"yt-dlp не уложился в {self.timeout_sec} сек") from None
        except BaseException:
            kill()
            err_task.cancel()
            raise
        err = (await err_task).decode("utf-8", errors="replace")
        if rc != 0:
            raise self.error(err, rc)
