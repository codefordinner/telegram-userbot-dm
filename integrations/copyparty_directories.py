"""Copyparty WebDAV directory listing and series-folder resolution."""

import logging
from typing import Optional
from urllib.parse import quote, unquote

import httpx

from domain.filename_utils import truncate_dirname
from domain.naming_rules import season_number, series_key


log = logging.getLogger("userbot-dm-uploader")


class CopypartyDirectories:
    def __init__(self, http_client, series_lock, remote_url, ok_mkcol_statuses, request_timeout):
        self.http = http_client
        self.series_lock = series_lock
        self.remote_url = remote_url
        self.ok_mkcol_statuses = ok_mkcol_statuses
        self.request_timeout = request_timeout

    async def list_remote_dirs(self, url, user, pw, segments: list) -> Optional[list]:
        """Return folder names; [] means missing and None means the listing failed."""
        path = "/".join(quote(segment, safe="") for segment in segments)
        base = f"{url.rstrip('/')}/" + (f"{path}/" if path else "")
        try:
            resp = await self.http.get(base + "?ls", auth=(user, pw), timeout=30)
        except httpx.HTTPError as exc:
            log.warning("Не удалось получить список папок «%s»: %s", "/".join(segments) or "/", exc)
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

    async def list_remote_entries(self, url, user, pw, segments: list) -> Optional[tuple]:
        """Return (folder names, file names); None means the listing failed."""
        path = "/".join(quote(segment, safe="") for segment in segments)
        base = f"{url.rstrip('/')}/" + (f"{path}/" if path else "")
        try:
            resp = await self.http.get(base + "?ls", auth=(user, pw), timeout=30)
        except httpx.HTTPError as exc:
            log.warning("Не удалось получить список «%s»: %s", "/".join(segments) or "/", exc)
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
            result = []
            for entry in data.get(key) or []:
                href = entry.get("href") if isinstance(entry, dict) else entry
                if isinstance(href, str):
                    name = unquote(href).strip("/").rsplit("/", 1)[-1]
                    if name:
                        result.append(name)
            return result

        return names("dirs"), names("files")

    async def ensure_remote_dirs(self, url, user, pw, segments: list) -> None:
        """Best-effort creation of nested folders using WebDAV MKCOL."""
        path = ""
        for segment in segments:
            path = f"{path}/{segment}" if path else segment
            try:
                resp = await self.http.request(
                    "MKCOL", self.remote_url(url, path) + "/", auth=(user, pw), timeout=self.request_timeout
                )
            except httpx.HTTPError as exc:
                log.warning("Не удалось создать папку «%s»: %s: %s", path, type(exc).__name__, exc)
                return
            if resp.status_code in self.ok_mkcol_statuses:
                continue
            log.warning(
                "Не удалось создать папку «%s» (%s %s) — проверьте, что у пользователя copyparty есть право write",
                path, resp.status_code, resp.text[:200],
            )
            return

    async def resolve_series_dirs(self, url, user, pw, show: str, season: int) -> list:
        show = truncate_dirname(show)
        season_dir = f"Season {season:02d}"
        async with self.series_lock:
            show_dir = show
            found = False
            root = await self.list_remote_dirs(url, user, pw, [])
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
                seasons = await self.list_remote_dirs(url, user, pw, [show_dir]) or []
                if season_dir not in seasons:
                    for name in sorted(seasons, key=str.casefold):
                        if season_number(name) == season:
                            season_dir = name
                            break
            await self.ensure_remote_dirs(url, user, pw, [show_dir, season_dir])
        return [show_dir, season_dir]

    async def find_series_dirs(self, url, user, pw, show: str, season: int) -> Optional[list]:
        """Return existing [show, season] directories without creating any."""
        show = truncate_dirname(show)
        root = await self.list_remote_dirs(url, user, pw, []) or []
        show_dir = show if show in root else None
        if show_dir is None:
            key = series_key(show)
            if key:
                show_dir = next((name for name in sorted(root, key=str.casefold) if series_key(name) == key), None)
        if show_dir is None:
            return None
        seasons = await self.list_remote_dirs(url, user, pw, [show_dir]) or []
        season_dir = next((name for name in sorted(seasons, key=str.casefold) if season_number(name) == season), None)
        return [show_dir, season_dir] if season_dir else None
