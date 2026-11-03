"""Small asyncio and filesystem helpers shared by application services."""

import asyncio
import contextlib
import functools
from pathlib import Path


class BackgroundTaskSupervisor:
    """Keep strong references to fire-and-forget tasks until they finish."""

    def __init__(self):
        self.tasks = set()

    def spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task


async def run_blocking(func, *args):
    """Run blocking filesystem or media work outside the event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args))


async def sleep_waiting(state, seconds: float) -> None:
    """Mark a transfer as waiting while it sleeps for a retry."""
    state.waiting = True
    try:
        await asyncio.sleep(seconds)
    finally:
        state.waiting = False


def remove_quietly(path: Path) -> None:
    with contextlib.suppress(OSError):
        path.unlink()
