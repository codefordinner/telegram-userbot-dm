"""Telegram question/answer flows and the optional inline-button helper bot."""

import asyncio
import contextlib
import logging
import uuid
from typing import Optional

from telethon import Button, TelegramClient, events


log = logging.getLogger("userbot-dm-uploader")
CANCEL_WORDS = {"0", "отмена", "cancel"}


async def safe_edit(message, text: str) -> bool:
    try:
        await message.edit(text)
        return True
    except Exception as exc:
        log.warning("Не удалось обновить статусное сообщение: %s", exc)
        return False


class TelegramInteractions:
    def __init__(
        self, *, choice_timeout: int, allowed_senders: set, session_path: str,
        api_id: int, api_hash: str, helper_token: str, safe_edit,
    ):
        self.choice_timeout = choice_timeout
        self.allowed_senders = allowed_senders
        self.session_path = session_path
        self.api_id = api_id
        self.api_hash = api_hash
        self.helper_token = helper_token
        self.safe_edit = safe_edit
        self.pending_choices: dict = {}
        self.bot_client: Optional[TelegramClient] = None

    def sender_allowed(self, sender_id) -> bool:
        return not self.allowed_senders or sender_id in self.allowed_senders

    def resolve_text_choice(self, event) -> bool:
        """Resolve a pending question only when the sender replies to its prompt."""
        if not event.is_reply or not event.raw_text:
            return False
        entry = self.pending_choices.get(("reply", event.chat_id, event.reply_to_msg_id))
        if entry is None:
            return False
        future, aliases = entry
        text = event.raw_text.strip()
        if aliases is None:
            value = "" if text.lower() in CANCEL_WORDS else (text or None)
        else:
            value = aliases.get(text.lower())
        if value is None or future.done():
            return False
        future.set_result(value)
        return True

    async def ask_choice(self, event, question: str, rows: list, aliases: dict, hint: str, done_note) -> Optional[str]:
        future = asyncio.get_running_loop().create_future()
        token = uuid.uuid4().hex[:10]
        keys = []
        helper_msg = None

        if self.bot_client is not None:
            try:
                helper_msg = await self.bot_client.send_message(
                    event.sender_id,
                    question,
                    buttons=[
                        [Button.inline(label, f"ask:{token}:{value}".encode()) for label, value in row]
                        for row in rows
                    ],
                )
                keys.append(("cb", token))
            except Exception as exc:
                log.warning("Бот-помощник не смог отправить кнопки (нажмите в нём /start): %s", exc)

        how = "Нажмите кнопку в чате с ботом-помощником или ответьте" if helper_msg else "Ответьте на это сообщение"
        prompt = await event.reply(f"{question}\n{how}: {hint}.")
        keys.append(("reply", event.chat_id, prompt.id))
        for key in keys:
            self.pending_choices[key] = (future, aliases)

        try:
            choice = await asyncio.wait_for(future, timeout=self.choice_timeout)
        except asyncio.TimeoutError:
            choice = None
        finally:
            for key in keys:
                self.pending_choices.pop(key, None)

        note = done_note(choice)
        await self.safe_edit(prompt, note)
        if helper_msg is not None:
            with contextlib.suppress(Exception):
                await helper_msg.edit(note)
        return choice

    async def ask_text(self, event, question: str, hint: str) -> Optional[str]:
        future = asyncio.get_running_loop().create_future()
        prompt = await event.reply(f"{question}\nОтветьте на это сообщение: {hint}. (0 — отмена)")
        key = ("reply", event.chat_id, prompt.id)
        self.pending_choices[key] = (future, None)
        try:
            answer = await asyncio.wait_for(future, timeout=self.choice_timeout)
        except asyncio.TimeoutError:
            answer = None
        finally:
            self.pending_choices.pop(key, None)

        if answer is None:
            note = "⌛ Ответа не дождался — файл пропущен."
        elif answer == "":
            note = "✖️ Отменено."
        else:
            note = f"✅ Принял: {answer}"
        await self.safe_edit(prompt, note)
        return answer

    async def on_choice_button(self, event) -> None:
        if not self.sender_allowed(event.sender_id):
            await event.answer("Нет доступа", alert=True)
            return
        try:
            _, token, value = event.data.decode().split(":", 2)
        except ValueError:
            return
        entry = self.pending_choices.get(("cb", token))
        if entry is None or entry[0].done():
            await event.answer("Этот вопрос уже неактуален")
            return
        entry[0].set_result(value)
        await event.answer("Принято")

    async def on_helper_start(self, event) -> None:
        if event.is_private and self.sender_allowed(event.sender_id):
            await event.reply("Готово ✅ Когда юзербот не поймёт тип файла, кнопки придут сюда.")

    async def start_helper(self) -> None:
        """Start the optional helper bot; text answers remain available if startup fails."""
        if not self.helper_token:
            return
        candidate = TelegramClient(
            self.session_path + "_buttons", self.api_id, self.api_hash,
            connection_retries=-1, retry_delay=2, auto_reconnect=True,
        )
        candidate.parse_mode = None
        candidate.add_event_handler(self.on_choice_button, events.CallbackQuery(pattern=rb"^ask:"))
        candidate.add_event_handler(
            self.on_helper_start, events.NewMessage(pattern=r"^/start", incoming=True)
        )
        try:
            await candidate.start(bot_token=self.helper_token)
            me = await candidate.get_me()
        except Exception as exc:
            log.warning("Бот-помощник не запустился, вопросы будут текстовыми: %s", exc)
            with contextlib.suppress(Exception):
                await candidate.disconnect()
            return
        self.bot_client = candidate
        log.info("Бот-помощник для кнопок запущен: @%s", me.username)
