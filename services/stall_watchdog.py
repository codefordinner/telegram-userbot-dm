"""Cancel stalled transfer attempts while allowing active waits and progress."""

import asyncio
import contextlib
import time


class StallWatchdog:
    def __init__(self, timeout_sec: float, tick_sec: float, stall_error):
        self.timeout_sec = timeout_sec
        self.tick_sec = tick_sec
        self.stall_error = stall_error

    async def __call__(self, coro, state):
        """Watch one download/upload attempt and cancel it when progress stops."""
        if self.timeout_sec <= 0:
            return await coro

        task = asyncio.ensure_future(coro)
        last_current = state.current
        last_move = time.monotonic()
        try:
            while True:
                done, _ = await asyncio.wait({task}, timeout=self.tick_sec)
                if done:
                    return task.result()
                now = time.monotonic()
                # After all bytes are sent, wait for the HTTP response under this watchdog.
                awaiting_response = state.phase == "upload" and state.current >= state.total
                if state.current != last_current or state.waiting or awaiting_response:
                    last_current = state.current
                    last_move = now
                    continue
                if now - last_move > self.timeout_sec:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task
                    raise self.stall_error(self.timeout_sec)
        finally:
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
