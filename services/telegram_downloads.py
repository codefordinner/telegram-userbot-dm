"""Resumable parallel Telegram document downloader."""

import asyncio
import contextlib
import json
import logging
import os
from collections import deque
from functools import partial
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

from domain.transfer_models import TransferError, TransferState


log = logging.getLogger("userbot-dm-uploader")


class TelegramDownloadService:
    """Download through isolated Telethon sessions and persist contiguous progress."""

    def __init__(
        self,
        primary_client: TelegramClient,
        api_id: int,
        api_hash: str,
        request_size: int,
        workers: int,
        segment_size: int,
        retries: int,
        max_flood_waits: int,
        max_flood_wait_sec: int,
        retry_backoff_sec: int,
        stall_watchdog,
        meta_path,
        remove_quietly,
        format_size,
    ):
        self.primary_client = primary_client
        self.api_id = api_id
        self.api_hash = api_hash
        self.request_size = request_size
        self.workers = workers
        self.segment_size = segment_size
        self.retries = retries
        self.max_flood_waits = max_flood_waits
        self.max_flood_wait_sec = max_flood_wait_sec
        self.retry_backoff_sec = retry_backoff_sec
        self.stall_watchdog = stall_watchdog
        self.meta_path = meta_path
        self.remove_quietly = remove_quietly
        self.format_size = format_size

    async def run_blocking(self, func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(func, *args))

    async def sleep_waiting(self, state: TransferState, seconds: float) -> None:
        state.waiting = True
        try:
            await asyncio.sleep(seconds)
        finally:
            state.waiting = False

    async def refresh_message(self, message):
        """Refresh a message whose media file reference may have expired."""
        try:
            fresh = await asyncio.wait_for(
                self.primary_client.get_messages(message.chat_id, ids=message.id), timeout=30
            )
            return fresh or message
        except Exception as exc:
            log.debug("Не удалось обновить сообщение: %s", exc)
            return message

    def read_resume_point(self, local_path: Path, doc) -> int:
        try:
            data = json.loads(self.meta_path(local_path).read_text(encoding="utf-8"))
            have = local_path.stat().st_size
            done = int(data.get("done", 0))
        except (OSError, ValueError, TypeError):
            return 0
        total = doc.size or 0
        if (
            data.get("doc_id") != doc.id or data.get("size") != total
            or data.get("segment") != self.segment_size or done < 0 or done > total or done > have
        ):
            return 0
        return done

    def write_resume_point(self, local_path: Path, doc, done: int) -> None:
        payload = {"doc_id": doc.id, "size": doc.size or 0, "segment": self.segment_size, "done": done}
        target = self.meta_path(local_path)
        temporary = target.with_name(target.name + ".tmp")
        try:
            temporary.write_text(json.dumps(payload), encoding="utf-8")
            os.replace(temporary, target)
        except OSError as exc:
            log.debug("Не удалось записать прогресс докачки: %s", exc)

    async def _pwrite_all(self, fd: int, data: bytes, pos: int) -> None:
        view = memoryview(data)
        while len(view):
            written = await self.run_blocking(os.pwrite, fd, view, pos)
            view = view[written:]
            pos += written

    async def make_download_client(self) -> TelegramClient:
        """Create an isolated in-memory client for one media download attempt."""
        session_data = StringSession.save(self.primary_client.session)
        if not session_data:
            raise TransferError("Не удалось открыть авторизованную сессию Telegram", retriable=False)
        downloader = TelegramClient(
            StringSession(session_data),
            self.api_id,
            self.api_hash,
            connection_retries=3,
            retry_delay=2,
            auto_reconnect=True,
            timeout=10,
        )
        try:
            await asyncio.wait_for(downloader.connect(), timeout=30)
            me = await asyncio.wait_for(downloader.get_me(), timeout=30)
            if me is None:
                raise TransferError("Не удалось проверить авторизацию загрузчика Telegram", retriable=True)
            return downloader
        except BaseException:
            with contextlib.suppress(BaseException):
                await asyncio.wait_for(downloader.disconnect(), timeout=10)
            raise

    async def download_unknown_size(
        self, message, local_path: Path, state: TransferState, attempt: int, downloader: TelegramClient
    ) -> None:
        self.remove_quietly(self.meta_path(local_path))
        state.begin("download", attempt, current=0, total=1)
        with open(local_path, "wb") as file:
            async for chunk in downloader.iter_download(message.document, request_size=self.request_size):
                await self.run_blocking(file.write, chunk)
                state.current += len(chunk)
                state.total = max(state.total, state.current)

    async def download_resumable(
        self, message, local_path: Path, state: TransferState, attempt: int, downloader: TelegramClient
    ) -> None:
        doc = message.document
        total = doc.size or 0
        if total <= 0:
            await self.download_unknown_size(message, local_path, state, attempt, downloader)
            return

        segment = self.segment_size
        nseg = (total + segment - 1) // segment
        done = self.read_resume_point(local_path, doc)
        first = nseg if done >= total else done // segment
        if done:
            log.info("Докачиваю «%s» с %s из %s", state.filename, self.format_size(done), self.format_size(total))
        else:
            self.remove_quietly(local_path)

        state.begin("download", attempt, current=done, total=total)
        state.disk_reserved = max(total - done, 0)
        pending = deque(range(first, nseg))
        finished = set()
        prefix = first
        persist_lock = asyncio.Lock()

        async def persist() -> None:
            async with persist_lock:
                await self.run_blocking(
                    self.write_resume_point, local_path, doc, min(prefix * segment, total)
                )

        fd = os.open(local_path, os.O_WRONLY | os.O_CREAT, 0o644)
        try:
            async def fetch_segment(idx: int) -> None:
                start = idx * segment
                end = min(start + segment, total)
                chunks = (end - start + self.request_size - 1) // self.request_size
                pos = start
                async for chunk in downloader.iter_download(
                    doc, offset=start, limit=chunks, request_size=self.request_size, file_size=total
                ):
                    chunk = chunk[: end - pos]
                    if not chunk:
                        break
                    await self._pwrite_all(fd, chunk, pos)
                    pos += len(chunk)
                    state.current += len(chunk)
                    state.disk_reserved = max(total - state.current, 0)
                    if pos >= end:
                        break
                if pos != end:
                    raise TransferError(
                        f"Telegram отдал неполный фрагмент файла "
                        f"({self.format_size(pos - start)} из {self.format_size(end - start)})",
                        retriable=True,
                    )

            async def worker() -> None:
                nonlocal prefix
                while pending:
                    idx = pending.popleft()
                    await fetch_segment(idx)
                    finished.add(idx)
                    advanced = False
                    while prefix in finished:
                        finished.discard(prefix)
                        prefix += 1
                        advanced = True
                    if advanced:
                        await persist()

            tasks = [asyncio.create_task(worker()) for _ in range(min(self.workers, len(pending)))]
            try:
                await asyncio.gather(*tasks)
            except BaseException:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                with contextlib.suppress(Exception):
                    await persist()
                raise
        finally:
            os.close(fd)

        state.disk_reserved = 0
        got = local_path.stat().st_size
        if got != total:
            self.remove_quietly(local_path)
            self.remove_quietly(self.meta_path(local_path))
            raise TransferError(
                f"скачанный файл неполный ({self.format_size(got)} из {self.format_size(total)})",
                retriable=True,
            )
        await self.run_blocking(self.write_resume_point, local_path, doc, total)

    async def download_with_retry(self, message, local_path: Path, state: TransferState) -> None:
        last_exc = None
        attempt = 0
        flood_waits = 0
        flood_budget = self.max_flood_wait_sec
        need_refresh = False
        while attempt < self.retries:
            attempt += 1
            state.attempt = attempt
            downloader = None
            retry_wait = 0
            try:
                if need_refresh:
                    message = await self.refresh_message(message)
                downloader = await self.make_download_client()
                await self.stall_watchdog(
                    self.download_resumable(message, local_path, state, attempt, downloader), state
                )
                return
            except asyncio.CancelledError:
                raise
            except FloodWaitError as exc:
                flood_waits += 1
                wait = exc.seconds + 1
                if flood_waits > self.max_flood_waits or wait > flood_budget:
                    raise TransferError(
                        f"Telegram просит подождать {exc.seconds} сек (FloodWait) — слишком долго или слишком "
                        f"часто, попробуйте позже"
                    ) from exc
                flood_budget -= wait
                log.warning(
                    "Telegram просит подождать %s сек (FloodWait), ждём и пробуем снова "
                    "(бюджет ожидания: ещё %s сек)",
                    exc.seconds, flood_budget,
                )
                attempt -= 1
                last_exc = exc
                need_refresh = True
                retry_wait = wait
            except Exception as exc:
                last_exc = exc
                need_refresh = True
                log.warning("Попытка скачивания %s/%s не удалась: %s", attempt, self.retries, exc)
                if attempt < self.retries:
                    retry_wait = self.retry_backoff_sec * attempt
            finally:
                if downloader is not None:
                    try:
                        await asyncio.wait_for(downloader.disconnect(), timeout=10)
                    except Exception as exc:
                        log.warning("Не удалось закрыть временный Telegram-клиент загрузки: %s", exc)
            if retry_wait:
                await self.sleep_waiting(state, retry_wait)
        raise last_exc
