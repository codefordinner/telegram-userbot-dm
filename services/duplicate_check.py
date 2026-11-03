"""Check whether a planned upload already exists in the media library."""

import logging
from pathlib import Path
from typing import Optional


class DuplicateCheckService:
    def __init__(
        self, *, targets, copyparty, beets_ready, find_items, library_error,
        artist_title_pattern, series_key, item_line, pick_episode_video,
        short_name, format_size, logger: logging.Logger,
    ):
        self.targets = targets
        self.copyparty = copyparty
        self.beets_ready = beets_ready
        self.find_items = find_items
        self.library_error = library_error
        self.artist_title_pattern = artist_title_pattern
        self.series_key = series_key
        self.item_line = item_line
        self.pick_episode_video = pick_episode_video
        self.short_name = short_name
        self.format_size = format_size
        self.logger = logger

    async def find_library_matches(self, stem: str) -> list:
        """Find exact artist/title matches, ignoring punctuation and case."""
        match = self.artist_title_pattern.match(stem)
        if not match:
            return []
        artist, title = match.group(1).strip(), match.group(2).strip()
        items = await self.find_items([f"artist:{artist}", f"title:{title}"])
        artist_key, title_key = self.series_key(artist), self.series_key(title)
        return [
            item for item in items
            if self.series_key(item["artist"]) == artist_key
            and self.series_key(item["title"]) == title_key
        ]

    async def find_duplicate(self, kind: str, filename: str, series: Optional[dict]) -> Optional[str]:
        """Return the existing-file description, or None when no duplicate is found."""
        url, user, password = self.targets[kind]
        if kind == "music":
            if self.beets_ready():
                try:
                    items = await self.find_library_matches(Path(filename).stem)
                except self.library_error as exc:
                    self.logger.warning("Проверка библиотеки beets не удалась: %s", exc)
                    items = []
                if items:
                    more = f"\n…и ещё {len(items) - 3}" if len(items) > 3 else ""
                    return "Такой трек уже есть в библиотеке:\n" + "\n".join(
                        self.item_line(item) for item in items[:3]
                    ) + more
            if await self.copyparty.get_remote_size(url, user, password, filename) is not None:
                return f"«{self.short_name(filename)}» уже лежит на сервере (ждёт импорта)."
            return None

        if series and series.get("episode") is not None:
            directories = await self.copyparty.find_series_dirs(
                url, user, password, series["show"], int(series["season"])
            )
            if not directories:
                return None
            entries = await self.copyparty.list_remote_entries(url, user, password, directories)
            if not entries:
                return None
            existing = self.pick_episode_video(
                entries[1], int(series["season"]), int(series["episode"])
            )
            return f"Эта серия уже есть: {'/'.join(directories)}/«{existing}»." if existing else None

        size = await self.copyparty.get_remote_size(url, user, password, filename)
        if size is not None:
            extra = f" ({self.format_size(size)})" if size > 0 else ""
            return f"«{self.short_name(filename)}» уже есть в фильмах{extra}."
        return None

    @staticmethod
    def duplicate_note(choice: Optional[str]) -> str:
        if choice == "yes":
            return "✅ Загружаю всё равно."
        if choice is None:
            return "⌛ Ответа не дождался — файл пропущен."
        return "✖️ Отменено."
