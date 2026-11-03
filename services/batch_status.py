"""Per-chat transfer batches and their Telegram progress messages."""

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass, field

from domain.job_model import Job
from services.status_format import (
    fit_text,
    format_eta,
    format_size,
    format_speed,
    make_progress_bar,
    short_name,
)
from domain.transfer_models import SpeedMeter, TransferState


log = logging.getLogger("userbot-dm-uploader")


@dataclass(eq=False)
class BatchItem:
    job: Job
    state: TransferState
    status: str = "queued"
    replaced: bool = False
    where: str = ""
    error: str = ""
    final_text: str = ""
    meter: SpeedMeter = field(default_factory=SpeedMeter)

    def mark_ok(self, existed: bool, where: str) -> None:
        self.status = "ok"
        self.replaced = existed
        self.where = where
        note = "\n⚠️ Файл с таким именем уже был на сервере — заменён." if existed else ""
        name = self.job.filename
        if self.job.kind == "music":
            self.final_text = (
                f"✅ Готово: «{name}»\n"
                f"📁 Загружено в музыку — теги проставятся автоматически в течение ~10 минут.{note}"
            )
        else:
            place = f": {where}" if where else ""
            self.final_text = f"✅ Готово: «{name}»\n📁 Загружено в фильмы{place}.{note}"

    def mark_error(self, reason: str) -> None:
        self.status = "error"
        self.error = reason
        self.final_text = f"❌ Не удалось загрузить «{self.job.filename}»: {reason}."


class BatchManager:
    def __init__(
        self, *, spawn, refresh_jellyfin, safe_edit, batch_grace_sec,
        progress_edit_interval_sec, max_batch_lines, upload_retries, download_retries,
    ):
        self.spawn = spawn
        self.refresh_jellyfin = refresh_jellyfin
        self.safe_edit = safe_edit
        self.batch_grace_sec = batch_grace_sec
        self.progress_edit_interval_sec = progress_edit_interval_sec
        self.max_batch_lines = max_batch_lines
        self.upload_retries = upload_retries
        self.download_retries = download_retries
        self.batches: dict = {}

    def join(self, chat_id: int, send, item: BatchItem):
        batch = self.batches.get(chat_id)
        if batch is None or batch.closed:
            batch = Batch(self, chat_id, send)
            self.batches[chat_id] = batch
        batch.add(item)
        return batch


class Batch:
    def __init__(self, manager: BatchManager, chat_id: int, send):
        self.manager = manager
        self.chat_id = chat_id
        self.send = send
        self.items: list = []
        self.status = None
        self.pending = 0
        self.closed = False
        self.lock = asyncio.Lock()
        self.stop = asyncio.Event()
        self.updater_task = None
        self.close_task = None

    def add(self, item: BatchItem) -> None:
        self.items.append(item)
        self.pending += 1
        if self.close_task is not None and not self.close_task.done():
            self.close_task.cancel()
        self.close_task = None

    def item_finished(self) -> None:
        self.pending -= 1
        if self.pending <= 0 and not self.closed and self.close_task is None:
            self.close_task = self.manager.spawn(self._close_later())

    async def ensure_status(self) -> None:
        async with self.lock:
            if self.status is None:
                self.status = await self.send(fit_text(self.render()))
                self.updater_task = self.manager.spawn(self._run_updater())

    async def _run_updater(self) -> None:
        last = None
        while True:
            try:
                await asyncio.wait_for(
                    self.stop.wait(), timeout=self.manager.progress_edit_interval_sec
                )
            except asyncio.TimeoutError:
                pass
            if self.stop.is_set():
                break
            text = fit_text(self.render())
            if text != last:
                try:
                    await self.status.edit(text)
                    last = text
                except Exception as exc:
                    log.debug("Не удалось обновить прогресс: %s", exc)

    async def _close_later(self) -> None:
        try:
            await asyncio.sleep(self.manager.batch_grace_sec)
        except asyncio.CancelledError:
            return
        if self.pending > 0 or self.closed:
            return
        self.closed = True
        if self.manager.batches.get(self.chat_id) is self:
            del self.manager.batches[self.chat_id]
        try:
            await self.finish()
        except Exception:
            log.exception("Не удалось завершить пачку")

    async def finish(self) -> None:
        self.stop.set()
        if self.updater_task is not None:
            with contextlib.suppress(Exception):
                await self.updater_task
        if self.status is None:
            return
        suffix = ""
        if any(item.status == "ok" and item.job.kind == "movie" for item in self.items):
            scan = await self.manager.refresh_jellyfin()
            if scan is True:
                suffix = "\n🔄 Сканирование Jellyfin запущено."
            elif scan is False:
                suffix = "\n⚠️ Jellyfin не ответил — запустите сканирование библиотеки вручную."
        await self.manager.safe_edit(self.status, fit_text(self.render(final_suffix=suffix)))

    def render(self, final_suffix: str = "") -> str:
        now = time.monotonic()
        items = list(self.items)
        if len(items) == 1:
            item = items[0]
            return self._render_single(item, now) + (final_suffix if item.status == "ok" else "")

        total = len(items)
        ok = sum(1 for item in items if item.status == "ok")
        bad = sum(1 for item in items if item.status == "error")
        all_done = ok + bad == total
        head = f"📦 Готово: {ok} из {total}" if all_done else f"📦 Загружаю файлы: {total} · готово {ok}"
        if bad:
            head += f" · ошибок {bad}"

        if total > self.manager.max_batch_lines:
            lines = [f"✅ Загружено: {ok}"] + [
                self._line(item, now) for item in items if item.status != "ok"
            ][:self.manager.max_batch_lines]
        else:
            lines = [self._line(item, now) for item in items]
        text = head + "\n" + "\n".join(lines)

        if all_done:
            notes = []
            if any(item.status == "ok" and item.job.kind == "music" for item in items):
                notes.append("📁 Музыка: теги проставятся автоматически в течение ~10 минут.")
            if any(item.status == "ok" and item.job.kind == "movie" for item in items):
                notes.append("📁 Видео загружено в фильмы.")
            if notes:
                text += "\n\n" + "\n".join(notes)
            text += final_suffix
        return text

    def _render_single(self, item: BatchItem, now: float) -> str:
        job, state = item.job, item.state
        if item.status == "queued":
            size_note = f" ({job.size / 1024 / 1024:.0f} МБ)" if job.size else ""
            return f"⏳ Принял «{job.filename}»{size_note}, жду очереди…"
        if item.status in ("ok", "error"):
            return item.final_text
        if state.phase == "convert":
            return f"🎧 Достаю звук из видео «{job.filename}»…"
        if state.phase == "upload" and state.current >= state.total:
            return f"📤 «{job.filename}» отправлен — жду подтверждения сервера и проверяю размер…"
        speed, eta = item.meter.measure(state, now)
        total = max(state.total, 1)
        percent = min(100, int(state.current / total * 100))
        icon = "📥" if state.phase == "download" else "📤"
        label = "Скачиваю" if state.phase == "download" else "Загружаю на сервер"
        retries = self.manager.upload_retries if state.phase == "upload" else self.manager.download_retries
        attempt_note = f" (попытка {state.attempt}/{retries})" if state.attempt > 1 else ""
        pause_note = "\n⏸ Жду перед повторной попыткой…" if state.waiting else ""
        return (
            f"{icon} {label} «{job.filename}»{attempt_note}\n"
            f"[{make_progress_bar(percent)}] {percent}%\n"
            f"{format_size(state.current)} / {format_size(total)} · {format_speed(speed)} · ~{format_eta(eta)}"
            f"{pause_note}"
        )

    def _line(self, item: BatchItem, now: float) -> str:
        name = short_name(item.job.filename)
        state = item.state
        if item.status == "queued":
            return f"⏳ {name}"
        if item.status == "ok":
            line = f"✅ {name}"
            if item.where:
                line += f" → {short_name(item.where, 50)}"
            if item.replaced:
                line += " (заменён)"
            return line
        if item.status == "error":
            return f"❌ {name} — {item.error}"
        if state.phase == "convert":
            return f"🎧 {name} — достаю звук"
        if state.phase == "upload" and state.current >= state.total:
            return f"📤 {name} — проверяю на сервере"
        speed, _eta = item.meter.measure(state, now)
        percent = min(100, int(state.current / max(state.total, 1) * 100))
        icon = "📥" if state.phase == "download" else "📤"
        pause = " ⏸" if state.waiting else ""
        return f"{icon} {name} — {percent}% · {format_speed(speed)}{pause}"
