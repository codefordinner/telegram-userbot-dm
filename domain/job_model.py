"""Domain model for a queued or resumable media transfer."""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class Job:
    """One accepted Telegram message or link item and its durable transfer state."""

    chat_id: int
    sender_id: int
    message_id: int
    kind: str  # "music" | "movie"
    filename: str  # server-side name without directories
    size: int = 0
    extract: bool = False
    series: Optional[dict] = None
    starts: int = 0
    url: Optional[str] = None
    part: int = 0
    subtitle: bool = False
    force: bool = False

    @property
    def key(self) -> tuple:
        if self.part:
            return (self.chat_id, self.message_id, self.part)
        return (self.chat_id, self.message_id)

    def local_path(self, tmp_dir: Path) -> Path:
        """Return the stable temporary filename used for this job."""
        tail = f"_{self.part}" if self.part else ""
        return Path(tmp_dir) / f"{self.sender_id}_{self.message_id}{tail}.part"

    def journal_path(self, journal_dir: Path) -> Path:
        """Return the existing JSON journal filename for this job."""
        tail = f"_{self.part}" if self.part else ""
        return Path(journal_dir) / f"{self.sender_id}_{self.message_id}{tail}.json"
