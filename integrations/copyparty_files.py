"""Copyparty WebDAV file operations with atomic upload and retry handling."""

import asyncio
import logging
import time
import uuid
import weakref
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import httpx

from domain.transfer_models import RenameUnsupported, StallError, TransferError, TransferState


log = logging.getLogger("userbot-dm-uploader")


class CopypartyFiles:
    def __init__(
        self,
        http_client: httpx.AsyncClient,
        upload_via_temp: bool,
        upload_retries: int,
        retry_backoff_sec: int,
        perms_ttl_sec: int,
        chunk_size: int,
        default_timeout: httpx.Timeout,
        upload_timeout: httpx.Timeout,
        ok_move_statuses: set,
        ok_upload_statuses: set,
        retry_upload_statuses: set,
        move_denied_statuses: set,
        http_reasons: dict,
        stall_watchdog,
        run_blocking,
        sleep_waiting,
        format_size,
    ):
        self.http = http_client
        self.upload_via_temp = upload_via_temp
        self.upload_retries = upload_retries
        self.retry_backoff_sec = retry_backoff_sec
        self.perms_ttl_sec = perms_ttl_sec
        self.chunk_size = chunk_size
        self.default_timeout = default_timeout
        self.upload_timeout = upload_timeout
        self.ok_move_statuses = ok_move_statuses
        self.ok_upload_statuses = ok_upload_statuses
        self.retry_upload_statuses = retry_upload_statuses
        self.move_denied_statuses = move_denied_statuses
        self.http_reasons = http_reasons
        self.stall_watchdog = stall_watchdog
        self.run_blocking = run_blocking
        self.sleep_waiting = sleep_waiting
        self.format_size = format_size
        self._perms_cache: dict = {}
        self._name_locks: weakref.WeakValueDictionary = weakref.WeakValueDictionary()

    @staticmethod
    def remote_url(base: str, filename: str) -> str:
        """Encode each path component while preserving slash separators."""
        return f"{base.rstrip('/')}/" + "/".join(quote(part, safe="") for part in filename.split("/"))

    async def upload_stream(self, path: Path, state: TransferState):
        sent = 0
        with open(path, "rb") as file:
            while True:
                chunk = await self.run_blocking(file.read, self.chunk_size)
                if not chunk:
                    break
                sent += len(chunk)
                state.current = sent
                yield chunk

    async def get_remote_size(self, url, user, password, filename) -> Optional[int]:
        """HEAD size; None means absent/unreadable, -1 means present without a size."""
        try:
            response = await self.http.head(
                self.remote_url(url, filename), auth=(user, password), timeout=10
            )
        except httpx.HTTPError as exc:
            log.warning("Не удалось проверить файл «%s» на сервере: %s", filename, exc)
            return None
        if response.status_code != 200:
            return None
        length = response.headers.get("content-length")
        return int(length) if length and length.isdigit() else -1

    @staticmethod
    def parse_retry_after(response: httpx.Response) -> Optional[float]:
        value = response.headers.get("retry-after")
        try:
            return min(float(value), 60.0) if value else None
        except ValueError:
            return None

    def name_lock(self, key: str) -> asyncio.Lock:
        lock = self._name_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._name_locks[key] = lock
        return lock

    @staticmethod
    def new_temp_name(final_name: str = "") -> str:
        folder = final_name.rpartition("/")[0]
        leaf = f"upload-{uuid.uuid4().hex[:12]}.part"
        return f"{folder}/{leaf}" if folder else leaf

    async def get_remote_perms(self, url, user, password) -> Optional[frozenset]:
        now = time.monotonic()
        cached = self._perms_cache.get(url)
        if cached and now - cached[0] < self.perms_ttl_sec:
            return cached[1]
        permissions = None
        try:
            response = await self.http.get(
                f"{url.rstrip('/')}/?ls", auth=(user, password), timeout=30
            )
            if response.status_code == 200:
                raw = response.json().get("perms")
                if isinstance(raw, list):
                    permissions = frozenset(str(value) for value in raw)
        except (httpx.HTTPError, ValueError) as exc:
            log.debug("Не удалось узнать права на %s: %s", url, exc)
        self._perms_cache[url] = (now, permissions)
        return permissions

    async def delete_remote_quietly(self, url, user, password, name) -> bool:
        """Best-effort removal of an incomplete or superseded remote file."""
        try:
            response = await self.http.delete(self.remote_url(url, name), auth=(user, password), timeout=15)
        except httpx.HTTPError as exc:
            log.warning("Не удалось удалить «%s» с сервера: %s", name, exc)
            return False
        if response.status_code in (200, 202, 204, 404):
            return True
        log.warning(
            "Не удалось удалить «%s» с сервера (%s) — без права delete у пользователя copyparty такие файлы "
            "придётся убирать вручную", name, response.status_code,
        )
        return False

    async def _put_file(self, url, user, password, name, local_path: Path, file_size: int, state: TransferState):
        stream = self.upload_stream(local_path, state)
        try:
            return await self.stall_watchdog(
                self.http.put(
                    self.remote_url(url, name),
                    auth=(user, password),
                    content=stream,
                    headers={"Content-Length": str(file_size)},
                    timeout=self.upload_timeout,
                ),
                state,
            )
        finally:
            await stream.aclose()

    async def move_remote(self, url, user, password, src_name, dst_name, expected_size: int) -> None:
        """Retry a WebDAV MOVE without re-uploading the source file."""
        last_error: Optional[BaseException] = None
        for attempt in range(1, 4):
            try:
                response = await self.http.request(
                    "MOVE",
                    self.remote_url(url, src_name),
                    auth=(user, password),
                    headers={
                        "Destination": self.remote_url(url, dst_name),
                        "Overwrite": "T",
                    },
                    timeout=self.default_timeout,
                )
            except httpx.TransportError as exc:
                last_error = exc
                log.warning("Переименование: попытка %s/3 не удалась: %s: %s", attempt, type(exc).__name__, exc)
            else:
                code = response.status_code
                if code in self.ok_move_statuses:
                    return
                if code == 404 and attempt > 1:
                    if await self.get_remote_size(url, user, password, dst_name) == expected_size:
                        return
                (log.warning if code in self.move_denied_statuses else log.error)(
                    "MOVE failed: %s %s", code, response.text[:300]
                )
                if code in self.move_denied_statuses:
                    if "delete" in response.text.lower():
                        raise TransferError(
                            "файл с таким именем уже есть на сервере, а заменить его нельзя: "
                            "у пользователя copyparty нет права delete"
                        )
                    raise RenameUnsupported("copyparty не разрешает переименование (нужно право move)")
                reason = self.http_reasons.get(code)
                user_message = f"сервер вернул {code}" + (f" ({reason})" if reason else "")
                last_error = TransferError(
                    f"не удалось переименовать временный файл: {user_message}",
                    retriable=code in self.retry_upload_statuses,
                )
                if not last_error.retriable:
                    raise last_error
            if attempt < 3:
                await asyncio.sleep(2 * attempt)
        raise last_error

    async def _upload_once(self, url, user, password, put_name, final_name, local_path, file_size, state) -> None:
        response = await self._put_file(url, user, password, put_name, local_path, file_size, state)
        if response.status_code not in self.ok_upload_statuses:
            log.error("upload failed: %s %s", response.status_code, response.text[:300])
            reason = self.http_reasons.get(response.status_code)
            user_message = f"сервер вернул {response.status_code}" + (f" ({reason})" if reason else "")
            raise TransferError(
                user_message,
                retriable=response.status_code in self.retry_upload_statuses,
                retry_after=self.parse_retry_after(response),
            )
        remote_size = await self.get_remote_size(url, user, password, put_name)
        if remote_size is not None and remote_size >= 0 and remote_size != file_size:
            log.warning("Размер после загрузки не совпал: сервер=%s, локально=%s", remote_size, file_size)
            raise TransferError(
                f"размер на сервере ({self.format_size(remote_size)}) не совпал с отправленным "
                f"({self.format_size(file_size)})",
                retriable=True,
            )
        if put_name != final_name:
            await self.move_remote(url, user, password, put_name, final_name, file_size)

    async def upload_with_retry(self, url, user, password, filename, local_path: Path, state: TransferState) -> bool:
        """Upload atomically when possible, preserving direct-upload fallback and retry behavior."""
        file_size = local_path.stat().st_size
        state.begin("upload", 1, current=0, total=file_size)
        last_error: Optional[BaseException] = None
        committed = False
        partial_name: Optional[str] = None
        force_direct = False

        async with self.name_lock(self.remote_url(url, filename)):
            existed = await self.get_remote_size(url, user, password, filename) is not None
            if existed:
                log.warning("Файл «%s» уже есть на сервере — будет заменён", filename)

            perms = await self.get_remote_perms(url, user, password) if self.upload_via_temp else None
            can_move = perms is None or "move" in perms
            can_delete = perms is None or "delete" in perms
            if self.upload_via_temp and existed and not (can_move and can_delete):
                raise TransferError(
                    "файл с таким именем уже есть на сервере, а заменить его нельзя: "
                    "у пользователя copyparty нет прав move и delete"
                )
            use_temp_ok = self.upload_via_temp and can_move
            if self.upload_via_temp and not can_move:
                log.warning("У пользователя copyparty нет права move — гружу напрямую, без временного файла")

            try:
                attempt = 0
                while attempt < self.upload_retries:
                    attempt += 1
                    state.begin("upload", attempt, current=0, total=file_size)
                    delay = self.retry_backoff_sec * attempt
                    use_temp = use_temp_ok and not force_direct
                    put_name = self.new_temp_name(filename) if use_temp else filename
                    partial_name = put_name if (use_temp or not existed) else None
                    try:
                        await self._upload_once(url, user, password, put_name, filename, local_path, file_size, state)
                        committed = True
                        return existed
                    except RenameUnsupported as exc:
                        self._perms_cache.pop(url, None)
                        if existed:
                            raise TransferError(
                                "файл с таким именем уже есть на сервере, а заменить его нельзя: "
                                "у пользователя copyparty нет прав move и delete"
                            ) from exc
                        log.warning("%s — перехожу на прямую загрузку", exc)
                        force_direct = True
                        await self.delete_remote_quietly(url, user, password, put_name)
                        partial_name = None
                        attempt -= 1
                        continue
                    except TransferError as exc:
                        if not exc.retriable:
                            raise
                        last_error = exc
                        delay = exc.retry_after or delay
                        log.warning("Попытка загрузки %s/%s не удалась: %s", attempt, self.upload_retries, exc)
                    except httpx.TransportError as exc:
                        last_error = exc
                        log.warning(
                            "Попытка загрузки %s/%s не удалась: %s: %s",
                            attempt, self.upload_retries, type(exc).__name__, exc,
                        )

                    if partial_name:
                        cleaned = await self.delete_remote_quietly(url, user, password, partial_name)
                        if cleaned:
                            partial_name = None
                        elif not use_temp:
                            partial_name = None
                            raise last_error
                    if attempt < self.upload_retries:
                        await self.sleep_waiting(state, delay)
                raise last_error
            finally:
                if not committed and partial_name:
                    await self.delete_remote_quietly(url, user, password, partial_name)
