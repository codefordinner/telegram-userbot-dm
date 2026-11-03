"""Route incoming private messages and library commands."""


class IncomingMessageRouter:
    def __init__(
        self, *, allowed_sender_ids, active_jobs, is_active, is_link_message,
        command_handlers, library_admin, command_disabled_note,
        library_error, describe_error, resolve_text_choice, handle_links,
        process_document, logger,
    ):
        self.allowed_sender_ids = allowed_sender_ids
        self.active_jobs = active_jobs
        self.is_active = is_active
        self.is_link_message = is_link_message
        self.command_handlers = command_handlers
        self.library_admin = library_admin
        self.command_disabled_note = command_disabled_note
        self.library_error = library_error
        self.describe_error = describe_error
        self.resolve_text_choice = resolve_text_choice
        self.handle_links = handle_links
        self.process_document = process_document
        self.log = logger

    async def handle_library_command(self, event) -> None:
        cmd, _, rest = event.raw_text.strip().partition(" ")
        handler = self.command_handlers.get(cmd.split("@")[0].lower())
        if handler is None:
            return
        if not self.library_admin and handler not in (self.command_handlers.get("/undo"), self.command_handlers.get("/history")):
            await event.reply(self.command_disabled_note)
            return
        try:
            await handler(event, rest.strip())
        except self.library_error as exc:
            await event.reply(f"❌ {exc}")
        except Exception as exc:
            self.log.exception("library command failed")
            await event.reply(f"❌ Не получилось: {self.describe_error(exc)}.")

    async def handle_dm(self, event) -> None:
        if not event.is_private:
            return
        if self.allowed_sender_ids and event.sender_id not in self.allowed_sender_ids:
            self.log.info("Игнорирую сообщение от чужого sender_id=%s", event.sender_id)
            return

        if not (event.media and event.document):
            if self.is_link_message(event):
                link_key = (event.chat_id, event.message.id)
                if self.is_active(link_key):
                    self.log.info("Ссылка из сообщения %s уже обрабатывается — пропускаю повтор", event.message.id)
                    return
                self.active_jobs.add(link_key)
                try:
                    await self.process(event)
                finally:
                    self.active_jobs.discard(link_key)
                return
            await self.process(event)
            return

        key = (event.chat_id, event.message.id)
        if key in self.active_jobs:
            self.log.info("Сообщение %s уже обрабатывается — пропускаю повтор", event.message.id)
            return
        self.active_jobs.add(key)
        try:
            await self.process(event)
        finally:
            self.active_jobs.discard(key)

    async def process(self, event) -> None:
        doc = event.document
        if not event.media or not doc:
            if self.resolve_text_choice(event):
                return
            if event.raw_text and event.raw_text.startswith("/"):
                await self.handle_library_command(event)
            elif self.is_link_message(event):
                await self.handle_links(event)
            return
        await self.process_document(event)

