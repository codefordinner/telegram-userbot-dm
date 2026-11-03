"""Own runtime resources and drive the application lifecycle."""

from dataclasses import dataclass
from typing import Optional

from services.app_lifecycle import BotLifecycle
from services.runtime_resources import RuntimeInitializer


@dataclass
class RuntimeState:
    client: object = None
    http_client: object = None
    upload_semaphore: object = None


class ApplicationRuntime:
    def __init__(
        self, *, resource_factory, resource_options, transfer_service,
        copyparty_gateway, job_recovery, startup_warnings,
        telegram_interactions, allowed_senders, version, logger,
        state: Optional[RuntimeState] = None,
    ):
        self.state = state or RuntimeState()
        self.resource_options = dict(resource_options)
        self.resource_options["on_client_created"] = self._remember_client
        self.resource_options["on_http_client_created"] = self._remember_http_client
        self.resource_initializer = RuntimeInitializer(
            resource_factory=resource_factory,
            resource_options=self.resource_options,
            transfer_service=transfer_service,
            copyparty_gateway=copyparty_gateway,
        )
        self.lifecycle = BotLifecycle(
            get_client=lambda: self.state.client,
            job_recovery=job_recovery,
            initialize_runtime=self.initialize_runtime,
            startup_warnings=startup_warnings,
            start_helper=telegram_interactions.start_helper,
            helper_client=lambda: telegram_interactions.bot_client,
            close_http=self.close_http,
            allowed_senders=allowed_senders,
            version=version,
            logger=logger,
        )

    def _remember_client(self, client) -> None:
        self.state.client = client

    def _remember_http_client(self, http_client) -> None:
        self.state.http_client = http_client

    def initialize_runtime(self) -> None:
        resources = self.resource_initializer.initialize()
        self.state.client = resources.client
        self.state.http_client = resources.http_client
        self.state.upload_semaphore = resources.upload_semaphore

    async def close_http(self) -> None:
        if self.state.http_client is not None:
            await self.state.http_client.aclose()

    async def run(self) -> None:
        await self.lifecycle.run()
