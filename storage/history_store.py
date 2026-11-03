"""SQLite persistence for the bot's upload history.

The table and columns intentionally match the format used by earlier releases.
Database creation is lazy, so importing this module has no filesystem side effects.
"""

import contextlib
import sqlite3
import time
from pathlib import Path
from typing import Optional


_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS history ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, chat_id INTEGER NOT NULL, "
    "kind TEXT NOT NULL, remote_name TEXT NOT NULL, filename TEXT NOT NULL, size INTEGER DEFAULT 0, "
    "replaced INTEGER DEFAULT 0, source TEXT DEFAULT '', undone INTEGER DEFAULT 0)"
)


class HistoryStore:
    """Read and write history entries in the existing SQLite database format."""

    def __init__(self, path: Path):
        self.path = Path(path)

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute(_SCHEMA)
        return conn

    def add(
        self, chat_id: int, kind: str, remote_name: str, filename: str,
        size: int, replaced: bool, source: str,
    ) -> None:
        with contextlib.closing(self._connect()) as conn:
            with conn:
                conn.execute(
                    "INSERT INTO history (ts, chat_id, kind, remote_name, filename, size, replaced, source) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (time.time(), chat_id, kind, remote_name, filename, size or 0, int(replaced), source),
                )

    def list(self, chat_id: int, limit: int) -> list:
        with contextlib.closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM history WHERE chat_id=? ORDER BY id DESC LIMIT ?", (chat_id, limit)
            ).fetchall()
        return [dict(row) for row in rows]

    def get(self, chat_id: int, hid: Optional[int] = None) -> Optional[dict]:
        with contextlib.closing(self._connect()) as conn:
            if hid is not None:
                row = conn.execute(
                    "SELECT * FROM history WHERE id=? AND chat_id=?", (hid, chat_id)
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM history WHERE chat_id=? AND undone=0 ORDER BY id DESC LIMIT 1",
                    (chat_id,),
                ).fetchone()
        return dict(row) if row else None

    def mark_undone(self, hid: int) -> None:
        with contextlib.closing(self._connect()) as conn:
            with conn:
                conn.execute("UPDATE history SET undone=1 WHERE id=?", (hid,))
