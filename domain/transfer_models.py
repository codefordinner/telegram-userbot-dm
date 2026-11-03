"""Transfer progress state and user-facing transfer exceptions."""

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass(eq=False)
class TransferState:
    filename: str
    total: int = 1
    phase: str = "download"
    current: int = 0
    attempt: int = 1
    waiting: bool = False
    disk_reserved: int = 0
    phase_started_at: float = field(default_factory=time.monotonic)
    phase_start_current: int = 0

    def begin(self, phase: str, attempt: int, current: int = 0, total: Optional[int] = None) -> None:
        """Start a new phase/attempt and reset speed and ETA measurements."""
        self.phase = phase
        self.attempt = attempt
        self.current = current
        self.phase_start_current = current
        self.phase_started_at = time.monotonic()
        if total is not None:
            self.total = max(total, 1)


class TransferError(Exception):
    """An error with a concise user-facing message and retry policy."""

    def __init__(self, user_message: str, retriable: bool = False, retry_after: Optional[float] = None):
        super().__init__(user_message)
        self.user_message = user_message
        self.retriable = retriable
        self.retry_after = retry_after


class StallError(TransferError):
    """Raised when a transfer attempt stops making progress for too long."""

    def __init__(self, seconds: float):
        super().__init__(f"нет прогресса больше {int(seconds)} сек (зависание)", retriable=True)


class RenameUnsupported(TransferError):
    """Raised when copyparty cannot move a temporary upload into its final name."""


ACTIVE_TRANSFERS = set()
SPEED_WINDOW_SEC = 15


class SpeedMeter:
    """Measure transfer speed and ETA over a sliding time window."""

    def __init__(self):
        self.samples = deque()
        self.key = None

    def measure(self, state: TransferState, now: float) -> tuple:
        current = state.current
        total = max(state.total, 1)
        key = (state.phase, state.attempt, state.phase_started_at)
        if key != self.key:
            self.key = key
            self.samples.clear()
            self.samples.append((state.phase_started_at, state.phase_start_current))
        self.samples.append((now, current))
        while len(self.samples) > 2 and self.samples[1][0] <= now - SPEED_WINDOW_SEC:
            self.samples.popleft()

        t0, c0 = self.samples[0]
        dt = now - t0
        speed = max((current - c0) / dt, 0.0) if dt > 0.5 else 0.0
        eta = (max(total - current, 0) / speed) if speed > 0 else float("inf")
        return speed, eta
