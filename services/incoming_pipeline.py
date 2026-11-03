"""Assemble the Telegram document processor and private-message router."""

from services.incoming_documents import IncomingDocumentProcessor
from services.incoming_messages import IncomingMessageRouter


class IncomingPipeline:
    def __init__(self, *, document_options, router_options):
        self.documents = IncomingDocumentProcessor(**document_options)
        self.router = IncomingMessageRouter(
            process_document=self.documents.process,
            **router_options,
        )

    async def handle_dm(self, event) -> None:
        await self.router.handle_dm(event)

    async def handle_library_command(self, event) -> None:
        await self.router.handle_library_command(event)

    async def process(self, event) -> None:
        await self.router.process(event)
