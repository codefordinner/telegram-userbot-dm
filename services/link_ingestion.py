"""Telegram link ingestion and resumable job creation for yt-dlp sources."""

import asyncio
import contextlib
import logging

from domain.filename_utils import normalize_text, sanitize_name, truncate_filename
from domain.job_model import Job
from domain.link_rules import extract_urls, is_ignored_url
from services.status_format import short_name


log = logging.getLogger("userbot-dm-uploader")


class LinkIngestionService:
    def __init__(
        self, *, probe_link, max_entries, link_kind, default_kind, ask_choice,
        kind_aliases, link_kind_note, yes_aliases, duplicate_note,
        music_meta_from_info, plan_video, journal_write, run_job, describe_error,
        safe_edit,
    ):
        self.probe_link = probe_link
        self.max_entries = max_entries
        self.link_kind = link_kind
        self.default_kind = default_kind
        self.ask_choice = ask_choice
        self.kind_aliases = kind_aliases
        self.link_kind_note = link_kind_note
        self.yes_aliases = yes_aliases
        self.duplicate_note = duplicate_note
        self.music_meta_from_info = music_meta_from_info
        self.plan_video = plan_video
        self.journal_write = journal_write
        self.run_job = run_job
        self.describe_error = describe_error
        self.safe_edit = safe_edit

    async def ingest(self, event, url: str, force: bool) -> None:
        status = await event.reply("🔎 Смотрю ссылку…")
        try:
            title, items = await self.probe_link(url)
        except Exception as exc:
            log.warning("Ссылка %s не прочиталась: %s", url, exc)
            await self.safe_edit(status, f"❌ Ссылка не обработана: {self.describe_error(exc)}.")
            return
        if not items:
            await self.safe_edit(status, "🤷 По этой ссылке ничего не нашёл.")
            return

        total_found = len(items)
        items = items[:self.max_entries]

        async def drop_status() -> None:
            with contextlib.suppress(Exception):
                await status.delete()

        kind = self.link_kind(url, items, event.raw_text or "", self.default_kind)
        cut = f" (беру первые {len(items)})" if total_found > len(items) else ""
        if kind is None:
            first = short_name(items[0].get("title") or url, 70)
            extra = f" и ещё {len(items) - 1}{cut}" if len(items) > 1 else ""
            choice = await self.ask_choice(
                event, f"🔗 Что скачать: «{first}»{extra}?",
                [[("🎵 Аудио (m4a)", "music"), ("🎬 Видео", "movie")], [("✖️ Отмена", "cancel")]],
                self.kind_aliases,
                "1 — аудио (в музыку), 2 — видео (в фильмы), 0 — отмена",
                self.link_kind_note,
            )
            if choice not in ("music", "movie"):
                await drop_status()
                return
            kind = choice
        elif len(items) > 1:
            choice = await self.ask_choice(
                event,
                f"📋 «{short_name(title or url, 60)}» — {len(items)} шт.{cut}. Скачать все как "
                f"{'музыку' if kind == 'music' else 'видео'}?",
                [[("⬇️ Скачать все", "yes"), ("✖️ Отмена", "cancel")]],
                self.yes_aliases, "да — скачать, нет — отмена", self.duplicate_note,
            )
            if choice != "yes":
                await drop_status()
                return

        jobs = []
        for index, item in enumerate(items, 1):
            series = None
            if kind == "music":
                artist, track_title, _album = self.music_meta_from_info(item)
                filename = truncate_filename(f"{artist} - {track_title}.m4a")
            else:
                name = sanitize_name(normalize_text(item.get("title") or item.get("id") or "video")) or "video"
                filename = truncate_filename(name + ".mp4")
                if len(items) == 1:
                    plan = await self.plan_video(event, filename, ".mp4", None, filename)
                    if plan is None:
                        await drop_status()
                        return
                    series, filename = plan
            jobs.append(Job(
                chat_id=event.chat_id, sender_id=event.sender_id, message_id=event.message.id,
                kind=kind, filename=filename, size=0, series=series,
                url=item["_url"], part=index, force=force,
            ))

        for job in jobs:
            self.journal_write(job)
        await drop_status()
        results = await asyncio.gather(
            *(self.run_job(job, None, event.reply) for job in jobs), return_exceptions=True
        )
        for result in results:
            if isinstance(result, BaseException):
                log.error("Задание по ссылке завершилось с ошибкой: %r", result)

    async def handle(self, event) -> None:
        text = event.raw_text or ""
        force = "--force" in text
        urls = [url for url in extract_urls(text) if not is_ignored_url(url)][:5]
        for url in urls:
            try:
                await self.ingest(event, url, force)
            except Exception as exc:
                log.exception("link ingest failed")
                await event.reply(f"❌ Не получилось: {self.describe_error(exc)}.")
