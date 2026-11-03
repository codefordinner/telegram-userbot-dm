"""Beets command-line client and output parser."""

import asyncio
import contextlib
import logging
import os
import re


log = logging.getLogger("userbot-dm-uploader")
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
LIB_SEP = "\x1f"
LIB_KEYS = ("id", "artist", "title", "album", "genres", "year", "format", "bitrate", "path")
LIB_FORMAT = LIB_SEP.join(f"${key}" for key in LIB_KEYS)


class LibraryError(Exception):
    """A user-facing failure from a library management command."""


class BeetsClient:
    def __init__(self, binary: str, config: str, plugins: str, timeout_sec: int):
        self.binary = binary
        self.config = config
        self.plugins = plugins
        self.timeout_sec = timeout_sec

    async def run(self, *args: str) -> str:
        """Run beets without a shell and return cleaned output."""
        command = [self.binary, "-c", self.config, "-p", self.plugins, *args]
        try:
            proc = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env={**os.environ, "NO_COLOR": "1"},
            )
        except FileNotFoundError:
            raise LibraryError(
                "в контейнере бота не установлен beets (команда «%s» не найдена)" % self.binary
            ) from None
        try:
            raw, _ = await asyncio.wait_for(proc.communicate(), timeout=self.timeout_sec)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()
            raise LibraryError(f"beets не ответил за {self.timeout_sec} сек") from None
        text = _ANSI_RE.sub("", raw.decode("utf-8", errors="replace"))
        lines = [line for line in text.splitlines() if not line.startswith("Created database backup")]
        if proc.returncode != 0:
            tail = "\n".join(lines).strip()[-300:]
            log.warning("beet %s -> код %s: %s", " ".join(args[:2]), proc.returncode, tail)
            raise LibraryError(tail or f"beets завершился с кодом {proc.returncode}")
        return "\n".join(lines)

    @staticmethod
    def parse_items(output: str) -> list:
        items = []
        for line in output.splitlines():
            parts = line.split(LIB_SEP)
            if len(parts) == len(LIB_KEYS) and parts[0].isdigit():
                items.append(dict(zip(LIB_KEYS, parts)))
        return items

    async def find_items(self, terms: list) -> list:
        # Anything after -- is a search expression, even when it starts with a dash.
        return self.parse_items(await self.run("ls", "-f", LIB_FORMAT, "--", *terms))

    async def get_item(self, item_id: int):
        found = await self.find_items([f"id:{item_id}"])
        return found[0] if found else None
