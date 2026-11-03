"""Runtime facade for the beets library and its Telegram command handlers."""

from pathlib import Path
import shutil
from typing import Optional

from integrations.beets_client import BeetsClient, LibraryError
from integrations.library import LibraryCommands


class LibraryRuntime:
    def __init__(
        self, *, beets_bin, beets_config, beets_plugins, beets_timeout_sec,
        library_admin, normalize_text, ask_choice, yes_aliases,
        navidrome_scan,
    ):
        self.beets_bin = beets_bin
        self.beets_config = beets_config
        self.library_admin = library_admin
        self.client = BeetsClient(beets_bin, beets_config, beets_plugins, beets_timeout_sec)
        self.navidrome_scan = navidrome_scan
        self.commands = LibraryCommands(
            run_beet=self.client.run,
            find_items=self.client.find_items,
            get_item=self.client.get_item,
            library_error=LibraryError,
            normalize_text=normalize_text,
            ask_choice=ask_choice,
            yes_aliases=yes_aliases,
            navidrome_scan=navidrome_scan,
            scan_note=self.scan_note,
        )

    async def run_beet(self, *args: str) -> str:
        return await self.client.run(*args)

    async def find_items(self, terms: list) -> list:
        return await self.client.find_items(terms)

    def beets_ready(self) -> bool:
        return bool(
            self.library_admin and shutil.which(self.beets_bin)
            and Path(self.beets_config).exists()
        )

    @staticmethod
    def scan_note(scan: Optional[bool]) -> str:
        if scan is True:
            return "\n🔄 Сканирование Navidrome запущено."
        if scan is False:
            return "\n⚠️ Navidrome не ответил — запустите сканирование вручную."
        return ""
