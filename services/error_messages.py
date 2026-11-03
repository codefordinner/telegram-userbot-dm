"""Convert transfer and infrastructure exceptions into user-facing messages."""

import asyncio
import errno

import httpx
from telethon.errors import FloodWaitError

from domain.transfer_models import TransferError


def describe_error(exc: BaseException) -> str:
    """Return a concise, stable error description for Telegram replies."""
    if isinstance(exc, TransferError):
        return exc.user_message
    if isinstance(exc, FloodWaitError):
        return f"Telegram просит подождать {exc.seconds} сек (FloodWait)"
    if isinstance(exc, (asyncio.TimeoutError, httpx.TimeoutException)):
        return "таймаут: сервер не отвечает"
    if isinstance(exc, httpx.ConnectError):
        return "не удалось подключиться к серверу"
    if isinstance(exc, httpx.TransportError):
        return f"сетевая ошибка ({type(exc).__name__})"
    if isinstance(exc, OSError):
        if exc.errno == errno.ENOSPC:
            return "на диске бота закончилось место"
        return f"ошибка диска ({exc.strerror or exc})"
    return f"внутренняя ошибка ({type(exc).__name__})"
