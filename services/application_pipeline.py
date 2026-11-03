"""Compose command handlers and the incoming Telegram message pipeline."""

from services.history_commands import HistoryCommands
from services.incoming_pipeline import IncomingPipeline


class ApplicationPipeline:
    def __init__(
        self, *, history_options, library_commands, document_options,
        router_options,
    ):
        self.history_commands = HistoryCommands(**history_options)
        self.command_handlers = {
            "/help": library_commands.help,
            "/start": library_commands.help,
            "/find": library_commands.find,
            "/last": library_commands.last,
            "/info": library_commands.info,
            "/rename": library_commands.rename,
            "/set": library_commands.set,
            "/del": library_commands.delete,
            "/stats": library_commands.stats,
            "/rescan": library_commands.rescan,
            "/undo": self.history_commands.undo,
            "/history": self.history_commands.history,
        }
        self.incoming = IncomingPipeline(
            document_options=document_options,
            router_options={
                **router_options,
                "command_handlers": self.command_handlers,
            },
        )
