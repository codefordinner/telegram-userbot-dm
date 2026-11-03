"""Telegram command handlers for upload history and undo."""

import time
from pathlib import Path
from typing import Optional

from services.status_format import short_name


class HistoryCommands:
    """Command behavior with storage, Telegram prompts, and integrations injected."""

    def __init__(
        self, *, history_list, history_get, history_mark_undone, run_blocking,
        max_list, targets, get_remote_size, find_library_matches, beets_ready,
        item_line, ask_choice, yes_aliases, run_beet, library_error,
        navidrome_scan, scan_note, delete_remote_quietly, prune_empty_dirs,
        refresh_jellyfin,
    ):
        self.history_list = history_list
        self.history_get = history_get
        self.history_mark_undone = history_mark_undone
        self.run_blocking = run_blocking
        self.max_list = max_list
        self.targets = targets
        self.get_remote_size = get_remote_size
        self.find_library_matches = find_library_matches
        self.beets_ready = beets_ready
        self.item_line = item_line
        self.ask_choice = ask_choice
        self.yes_aliases = yes_aliases
        self.run_beet = run_beet
        self.library_error = library_error
        self.navidrome_scan = navidrome_scan
        self.scan_note = scan_note
        self.delete_remote_quietly = delete_remote_quietly
        self.prune_empty_dirs = prune_empty_dirs
        self.refresh_jellyfin = refresh_jellyfin

    @staticmethod
    def _history_icon(row: dict) -> str:
        if row["source"] == "subtitle":
            return "💬"
        return "🎵" if row["kind"] == "music" else "🎬"

    @staticmethod
    def _format_ts(timestamp: float) -> str:
        return time.strftime("%d.%m %H:%M", time.localtime(timestamp))

    async def history(self, event, rest: str) -> None:
        limit = int(rest) if rest.strip().isdigit() else 10
        limit = max(1, min(limit, self.max_list))
        rows = await self.run_blocking(self.history_list, event.chat_id, limit)
        if not rows:
            await event.reply("🕒 История пуста.")
            return
        lines = [
            f"#{row['id']} · {self._format_ts(row['ts'])} · {self._history_icon(row)} "
            f"{short_name(row['remote_name'], 70)}" + (" ↩️" if row["undone"] else "")
            for row in rows
        ]
        await event.reply(
            "🕒 Последние загрузки:\n" + "\n".join(lines)
            + "\n\n/undo [номер] — отменить загрузку (↩️ — уже отменена)"
        )

    @staticmethod
    def _undo_note(choice: Optional[str]) -> str:
        if choice == "yes":
            return "↩️ Отменяю…"
        if choice is None:
            return "⌛ Ответа не дождался — ничего не удалено."
        return "✖️ Оставляю как есть."

    async def undo(self, event, rest: str) -> None:
        arg = rest.strip()
        if arg and not arg.isdigit():
            await event.reply("Формат: /undo — последняя загрузка, или /undo <номер из /history>")
            return
        row = await self.run_blocking(self.history_get, event.chat_id, int(arg) if arg else None)
        if row is None:
            await event.reply("🤷 Нечего отменять." if not arg else f"🤷 Загрузки #{arg} нет в истории.")
            return
        if row["undone"]:
            await event.reply(f"↩️ Загрузка #{row['id']} уже отменена.")
            return

        url, user, pw = self.targets[row["kind"]]
        name = row["remote_name"]
        library_items: list = []
        if row["kind"] == "music":
            if await self.get_remote_size(url, user, pw, name) is not None:
                question = f"↩️ Удалить с сервера «{short_name(name, 80)}»?"
            else:
                library_items = await self.find_library_matches(Path(name).stem) if self.beets_ready() else []
                if not library_items:
                    await event.reply(
                        f"🤷 «{short_name(name, 80)}» уже нет на сервере, а в библиотеке я его не нашёл "
                        "(найти вручную: /find)."
                    )
                    return
                listing = "\n".join(self.item_line(item) for item in library_items[:5])
                question = (
                    "↩️ Файла уже нет на сервере — похоже, трек импортирован в библиотеку.\n"
                    f"Удалить из библиотеки (и с диска)?\n{listing}"
                )
        else:
            question = f"↩️ Удалить «{short_name(name, 80)}» с сервера фильмов?"
        if row["replaced"]:
            question += "\n⚠️ Эта загрузка заменила старый файл — прежнюю версию вернуть нельзя."

        answer = await self.ask_choice(
            event, question,
            [[("↩️ Отменить загрузку", "yes"), ("✖️ Нет", "cancel")]],
            self.yes_aliases, "да — отменить, нет — оставить", self._undo_note,
        )
        if answer != "yes":
            return

        if library_items:
            done, problems = 0, []
            for item in library_items:
                try:
                    await self.run_beet("remove", "-d", "-f", f"id:{item['id']}")
                    done += 1
                except self.library_error as exc:
                    problems.append(f"#{item['id']}: {exc}")
            if done:
                await self.run_blocking(self.history_mark_undone, row["id"])
            scan = await self.navidrome_scan() if done else None
            text = f"✅ Удалено из библиотеки: {done} из {len(library_items)}." + self.scan_note(scan)
            if problems:
                text += "\n⚠️ Не удалось:\n" + "\n".join(problems)
            await event.reply(text)
            return

        if not await self.delete_remote_quietly(url, user, pw, name):
            await event.reply("❌ Не удалось удалить файл с сервера — вероятно, у пользователя copyparty нет права delete.")
            return
        await self.run_blocking(self.history_mark_undone, row["id"])
        note = ""
        if row["kind"] == "movie":
            await self.prune_empty_dirs(url, user, pw, name.split("/")[:-1])
            scan = await self.refresh_jellyfin()
            if scan is True:
                note = "\n🔄 Сканирование Jellyfin запущено."
            elif scan is False:
                note = "\n⚠️ Jellyfin не ответил — запустите сканирование вручную."
        await event.reply(f"✅ Загрузка #{row['id']} отменена: «{short_name(name, 80)}» удалён с сервера." + note)
