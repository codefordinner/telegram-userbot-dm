"""Beets library command behavior and display formatting."""

import re
import shlex
from typing import Optional


LIB_SETTABLE = ("artist", "title", "album", "albumartist", "genre", "year")
LIB_FIELD_ALIAS = {"genre": "genres"}
LIB_MAX_LIST = 25
LIB_MAX_IDS = 20
LIB_HELP = (
    "🎵 Управление библиотекой:\n"
    "/find <запрос> — поиск (например: /find scatman)\n"
    "/last [N] — последние добавленные\n"
    "/info <id> — подробности о треке\n"
    "/rename <id> Исполнитель - Название — переименовать (файл переедет сам)\n"
    "/set <id> поле=значение … — изменить теги: artist, title, album, albumartist, genre, year\n"
    "/del <id> [id …] — удалить треки вместе с файлами (спрошу подтверждение)\n"
    "/stats — статистика библиотеки\n"
    "/rescan — пересканировать Navidrome\n"
    "/history [N] — последние загрузки\n"
    "/undo [номер] — отменить загрузку (удалить файл с сервера)\n"
    "\n🔗 Пришлите ссылку (YouTube, YouTube Music, SoundCloud, Bandcamp …) — скачаю аудио (.m4a) или видео; "
    "плейлисты и альбомы — целиком. «--force» в сообщении — загрузить, даже если такое уже есть.\n"
    "💬 Субтитры (.srt .ass .vtt …) кладу рядом с видео под тем же именем."
)


def split_args(text: str) -> list:
    try:
        return shlex.split(text)
    except ValueError:
        return text.split()


def parse_ids(text: str, library_error, max_ids: int = LIB_MAX_IDS) -> list:
    ids = []
    for token in re.split(r"[\s,]+", text.strip()):
        if not token:
            continue
        if not token.isdigit():
            raise library_error(f"«{token}» — это не id трека (id — число из /find или /last)")
        if int(token) not in ids:
            ids.append(int(token))
    if len(ids) > max_ids:
        raise library_error(f"за раз можно не больше {max_ids} треков")
    return ids


def item_line(item: dict) -> str:
    album = f" · {item['album']}" if item["album"] else ""
    return f"#{item['id']} · {item['artist'] or '—'} — {item['title'] or '—'}{album}"


def format_items(items: list, header: str, max_list: int = LIB_MAX_LIST) -> str:
    if not items:
        return "🤷 Ничего не найдено."
    shown = items[:max_list]
    text = header + "\n" + "\n".join(item_line(item) for item in shown)
    if len(items) > len(shown):
        text += f"\n…и ещё {len(items) - len(shown)} — уточните запрос."
    return text + "\n\n/info <id> · /rename <id> … · /del <id>"


def item_details(item: dict) -> str:
    try:
        bitrate = f"{int(item['bitrate']) // 1000} кбит/с"
    except ValueError:
        bitrate = item["bitrate"] or "—"
    year = item["year"] if item["year"].strip("0") else "—"
    rows = [
        f"#{item['id']} · {item['artist'] or '—'} — {item['title'] or '—'}",
        f"Альбом: {item['album'] or '—'}",
        f"Жанр: {item['genres'] or '—'} · Год: {year}",
        f"Формат: {item['format'] or '—'} · {bitrate}",
        f"Файл: {item['path']}",
    ]
    return "\n".join(rows)


class LibraryCommands:
    def __init__(
        self, *, run_beet, find_items, get_item, library_error, normalize_text,
        ask_choice, yes_aliases, navidrome_scan, scan_note,
        max_list=LIB_MAX_LIST, max_ids=LIB_MAX_IDS,
    ):
        self.run_beet = run_beet
        self.find_items = find_items
        self.get_item = get_item
        self.library_error = library_error
        self.normalize_text = normalize_text
        self.ask_choice = ask_choice
        self.yes_aliases = yes_aliases
        self.navidrome_scan = navidrome_scan
        self.scan_note = scan_note
        self.max_list = max_list
        self.max_ids = max_ids

    async def help(self, event, rest: str) -> None:
        await event.reply(LIB_HELP)

    async def find(self, event, rest: str) -> None:
        terms = split_args(rest)
        if not terms:
            await event.reply("Что искать? Например: /find scatman")
            return
        items = await self.find_items(terms)
        await event.reply(format_items(items, f"🔎 Найдено: {len(items)}", self.max_list))

    async def last(self, event, rest: str) -> None:
        limit = int(rest) if rest.strip().isdigit() else 10
        limit = max(1, min(limit, self.max_list))
        items = (await self.find_items(["added-"]))[:limit]
        await event.reply(format_items(items, f"🕒 Последние {len(items)}:", self.max_list))

    async def info(self, event, rest: str) -> None:
        ids = parse_ids(rest, self.library_error, self.max_ids)
        if len(ids) != 1:
            await event.reply("Укажите один id: /info 123")
            return
        item = await self.get_item(ids[0])
        await event.reply(item_details(item) if item else f"🤷 Трека #{ids[0]} нет в библиотеке.")

    async def apply_changes(self, event, item_id: int, changes: dict, description: str) -> None:
        if await self.get_item(item_id) is None:
            raise self.library_error(f"трека #{item_id} нет в библиотеке")
        await self.run_beet("modify", "-y", f"id:{item_id}", *[f"{key}={value}" for key, value in changes.items()])
        item = await self.get_item(item_id)
        scan = await self.navidrome_scan()
        text = f"✅ {description}\n{item_details(item)}" if item else f"✅ {description}"
        await event.reply(text + self.scan_note(scan))

    async def rename(self, event, rest: str) -> None:
        match = re.match(r"^(\d+)\s+(.+?)\s+-\s+(.+)$", self.normalize_text(rest).strip())
        if not match:
            await event.reply("Формат: /rename <id> Исполнитель - Название\nНапример: /rename 12 Scatman John - Scatman")
            return
        changes = {"artist": match.group(2).strip(), "title": match.group(3).strip()}
        await self.apply_changes(event, int(match.group(1)), changes, "Переименовано.")

    async def set(self, event, rest: str) -> None:
        tokens = split_args(self.normalize_text(rest))
        usage = 'Формат: /set <id> поле=значение …\nНапример: /set 12 album="Best Of" year=1995'
        if len(tokens) < 2 or not tokens[0].isdigit():
            await event.reply(usage)
            return
        changes = {}
        for token in tokens[1:]:
            key, separator, value = token.partition("=")
            key = key.strip().lower()
            if not separator or key not in LIB_SETTABLE:
                raise self.library_error(f"не понял «{token}». Можно менять: {', '.join(LIB_SETTABLE)}")
            if key == "year" and not value.isdigit():
                raise self.library_error("год должен быть числом")
            changes[LIB_FIELD_ALIAS.get(key, key)] = value.strip()
        await self.apply_changes(event, int(tokens[0]), changes, "Теги изменены.")

    @staticmethod
    def _delete_note(choice: Optional[str]) -> str:
        if choice == "yes":
            return "🗑 Удаляю…"
        if choice is None:
            return "⌛ Ответа не дождался — ничего не удалено."
        return "✖️ Отменено, ничего не удалено."

    async def delete(self, event, rest: str) -> None:
        ids = parse_ids(rest, self.library_error, self.max_ids)
        if not ids:
            await event.reply("Формат: /del <id> [id …]")
            return
        items = [item for item in [await self.get_item(item_id) for item_id in ids] if item]
        if not items:
            await event.reply("🤷 Таких треков нет в библиотеке.")
            return
        listing = "\n".join(item_line(item) for item in items)
        answer = await self.ask_choice(
            event,
            f"🗑 Удалить навсегда ({len(items)})?\n{listing}\nФайлы тоже удалятся с диска.",
            [[("🗑 Удалить", "yes"), ("✖️ Отмена", "cancel")]],
            self.yes_aliases, "да — удалить, нет — отмена", self._delete_note,
        )
        if answer != "yes":
            return
        done, problems = 0, []
        for item in items:
            try:
                await self.run_beet("remove", "-d", "-f", f"id:{item['id']}")
                done += 1
            except self.library_error as exc:
                problems.append(f"#{item['id']}: {exc}")
        scan = await self.navidrome_scan() if done else None
        text = f"✅ Удалено: {done} из {len(items)}." + self.scan_note(scan)
        if problems:
            text += "\n⚠️ Не удалось:\n" + "\n".join(problems)
        await event.reply(text)

    async def stats(self, event, rest: str) -> None:
        await event.reply("📊 " + (await self.run_beet("stats")).strip())

    async def rescan(self, event, rest: str) -> None:
        scan = await self.navidrome_scan()
        if scan is None:
            await event.reply("Navidrome не настроен (NAVIDROME_USER / NAVIDROME_PASS).")
        else:
            await event.reply(self.scan_note(scan).strip())
