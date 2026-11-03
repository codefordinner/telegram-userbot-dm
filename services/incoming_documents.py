"""Incoming Telegram document classification, naming, and job creation."""

import logging
from pathlib import Path

from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeFilename, DocumentAttributeVideo
from domain.media_types import ext_from_mime


log = logging.getLogger("userbot-dm-uploader")


class IncomingDocumentProcessor:
    """Document pipeline with Telegram prompts and application services injected."""

    def __init__(
        self, *, junk_attribute_types, job_type, clean_filename,
        classify, ambiguous_min_bytes, plan_subtitle, truncate_filename, journal_write,
        run_job, ask_kind, video_extensions, looks_like_music_video, ask_choice,
        kind_aliases, ensure_extension, align_extension, mp4_family_extensions,
        parse_video_caption, build_video_filename, parse_music_caption,
        build_music_filename, should_use_caption, caption_naming, plan_video,
        duplicate_check, find_duplicate, yes_aliases, duplicate_note,
    ):
        self.junk_attribute_types = junk_attribute_types
        self.job_type = job_type
        self.clean_filename = clean_filename
        self.classify = classify
        self.ambiguous_min_bytes = ambiguous_min_bytes
        self.plan_subtitle = plan_subtitle
        self.truncate_filename = truncate_filename
        self.journal_write = journal_write
        self.run_job = run_job
        self.ask_kind = ask_kind
        self.video_extensions = video_extensions
        self.looks_like_music_video = looks_like_music_video
        self.ask_choice = ask_choice
        self.kind_aliases = kind_aliases
        self.ensure_extension = ensure_extension
        self.align_extension = align_extension
        self.mp4_family_extensions = mp4_family_extensions
        self.parse_video_caption = parse_video_caption
        self.build_video_filename = build_video_filename
        self.parse_music_caption = parse_music_caption
        self.build_music_filename = build_music_filename
        self.should_use_caption = should_use_caption
        self.caption_naming = caption_naming
        self.plan_video = plan_video
        self.duplicate_check = duplicate_check
        self.find_duplicate = find_duplicate
        self.yes_aliases = yes_aliases
        self.duplicate_note = duplicate_note

    async def process(self, event) -> None:
        doc = event.document
        if not event.media or not doc:
            return

        attrs = doc.attributes or []
        is_voice = any(isinstance(attr, DocumentAttributeAudio) and getattr(attr, "voice", False) for attr in attrs)
        is_round = any(
            isinstance(attr, DocumentAttributeVideo) and getattr(attr, "round_message", False)
            for attr in attrs
        )
        if is_voice or is_round:
            await event.reply("🎙 Голосовые и видеосообщения-«кружки» не загружаю — жду музыку или видеофайлы.")
            return
        if any(isinstance(attr, self.junk_attribute_types) for attr in attrs):
            await event.reply("🎞 GIF-анимации и стикеры не загружаю — жду музыку или видеофайлы.")
            return

        original_filename = None
        for attr in attrs:
            if isinstance(attr, DocumentAttributeFilename) and attr.file_name:
                original_filename = attr.file_name
        if not original_filename:
            original_filename = self._guess_filename(event.message, doc)
            log.info("У файла нет имени, придумал: %s", original_filename)
        original_filename = self.clean_filename(original_filename)

        mime_type = getattr(doc, "mime_type", None)
        has_video_attr = any(isinstance(attr, DocumentAttributeVideo) for attr in attrs)
        has_audio_attr = any(isinstance(attr, DocumentAttributeAudio) for attr in attrs)
        kind = self.classify(
            original_filename, mime_type, size=doc.size or 0,
            has_video_attr=has_video_attr, has_audio_attr=has_audio_attr,
            ambiguous_min_bytes=self.ambiguous_min_bytes,
        )
        if kind == "subtitle":
            ok, sub_series = await self.plan_subtitle(event, original_filename)
            if not ok:
                return
            job = self.job_type(
                chat_id=event.chat_id, sender_id=event.sender_id, message_id=event.message.id,
                kind="movie", filename=self.truncate_filename(original_filename), size=doc.size or 0,
                series=sub_series, subtitle=True,
            )
            self.journal_write(job)
            await self.run_job(job, event.message, event.reply)

            return

        asked = False
        if kind is None:
            asked = True
            kind = await self.ask_kind(event, original_filename)
            if kind is None:
                return

        extract_audio = False
        video_attr = next((attr for attr in attrs if isinstance(attr, DocumentAttributeVideo)), None)
        if (
            kind == "movie" and video_attr is not None
            and Path(original_filename).suffix.lower() in self.video_extensions
            and self.looks_like_music_video(
                Path(original_filename).stem,
                getattr(video_attr, "duration", 0) or 0,
                doc.size or 0,
            )
        ):
            duration = getattr(video_attr, "duration", 0) or 0
            minutes, seconds = int(duration // 60), int(duration % 60)
            length = f" ({minutes}:{seconds:02d})" if duration else ""
            choice = await self.ask_choice(
                event,
                f"🎬 «{original_filename}»{length} — похоже на музыкальный клип.\nЧто сделать?",
                [[("🎵 Музыка (достать звук)", "music"), ("🎬 Фильм", "movie")], [("✖️ Отмена", "cancel")]],
                self.kind_aliases,
                "1 — музыка (достану звук), 2 — фильм, 0 — отмена",
                lambda value: (
                    "✅ Достану звук и положу в 🎵 музыку." if value == "music"
                    else "✅ Понял, кладу в 🎬 фильмы." if value == "movie"
                    else "⌛ Ответа не дождался — файл пропущен." if value is None
                    else "✖️ Отменено."
                ),
            )
            if choice not in ("music", "movie"):
                return
            if choice == "music":
                kind = "music"
                extract_audio = True
                original_filename = Path(original_filename).stem + ".m4a"

        original_filename, extension = self.ensure_extension(original_filename, kind, mime_type)
        if asked:
            original_filename, extension = self.align_extension(original_filename, extension, kind, mime_type)

        if kind == "music" and extension.lower() in self.mp4_family_extensions:
            original_filename = original_filename[: -len(extension)] + ".m4a"
            extension = ".m4a"
            log.info("Аудио в контейнере mp4 — переименовал в %s", original_filename)

        filename = original_filename
        if kind == "movie":
            parsed = self.parse_video_caption(event.raw_text)
            candidate = self.build_video_filename(parsed, extension) if parsed else None
        else:
            parsed = self.parse_music_caption(event.raw_text)
            candidate = self.build_music_filename(parsed, extension) if parsed else None
        if parsed:
            if self.should_use_caption(kind, original_filename, parsed, self.caption_naming):
                filename = candidate
                log.info("Имя по подписи (%s): %s -> %s", self.caption_naming, parsed, filename)
            else:
                log.info(
                    "Подпись распознана (%s), но имя файла из Telegram «%s» уже информативно — оставляю его",
                    candidate, original_filename,
                )
        filename = self.truncate_filename(filename)

        series = None
        if kind == "movie" and not extract_audio:
            plan = await self.plan_video(event, original_filename, extension, parsed, filename)
            if plan is None:
                return
            series, filename = plan

        if self.duplicate_check:
            try:
                duplicate = await self.find_duplicate(kind, filename, series)
            except Exception as exc:
                log.warning("Проверка дубликатов не удалась: %s", exc)
                duplicate = None
            if duplicate:
                answer = await self.ask_choice(
                    event, f"♻️ {duplicate}\nЗагрузить всё равно?",
                    [[("⬆️ Загрузить", "yes"), ("✖️ Отмена", "cancel")]],
                    self.yes_aliases, "да — загрузить, нет — отмена", self.duplicate_note,
                )
                if answer != "yes":
                    return

        job = self.job_type(
            chat_id=event.chat_id,
            sender_id=event.sender_id,
            message_id=event.message.id,
            kind=kind,
            filename=filename,
            size=doc.size or 0,
            extract=extract_audio,
            series=series,
        )
        self.journal_write(job)
        await self.run_job(job, event.message, event.reply)

    @staticmethod
    def _guess_filename(message, doc) -> str:
        """Build a stable fallback name from Telegram audio attributes or message time."""
        attrs = doc.attributes or []
        audio = next((attr for attr in attrs if isinstance(attr, DocumentAttributeAudio)), None)
        if audio and (audio.performer or audio.title):
            base = " - ".join(part for part in (audio.performer, audio.title) if part)
        else:
            stamp = message.date.strftime("%Y%m%d_%H%M%S") if message.date else "file"
            base = f"telegram_{stamp}_{message.id}"
        return base + ext_from_mime(getattr(doc, "mime_type", None))
