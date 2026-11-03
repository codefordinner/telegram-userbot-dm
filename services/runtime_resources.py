"""Create external clients and runtime resources during application startup."""

import asyncio
import inspect
from dataclasses import dataclass


@dataclass
class RuntimeResources:
    client: object
    download_service: object
    http_client: object
    copyparty_files: object
    upload_semaphore: asyncio.Semaphore


class RuntimeInitializer:
    """Create runtime resources and connect them to transfer and Copyparty services."""

    def __init__(self, *, resource_factory, resource_options, transfer_service, copyparty_gateway):
        self.resource_factory = resource_factory
        self.resource_options = resource_options
        self.transfer_service = transfer_service
        self.copyparty_gateway = copyparty_gateway

    def initialize(self) -> RuntimeResources:
        resources = self.resource_factory(**self.resource_options)
        self.transfer_service.attach_telegram_downloads(resources.download_service)
        self.copyparty_gateway.initialize(
            files=resources.copyparty_files,
            http_client=resources.http_client,
        )
        return resources


def create_runtime_resources(
    *, telegram_client_type, event_type, event_handler, catch_up, session_path,
    api_id, api_hash, download_service_type, download_options, httpx_module,
    http_timeout, copyparty_files_type, copyparty_options, max_transfers, logger,
    on_client_created, on_http_client_created,
):
    """Build clients without performing network operations until lifecycle starts them."""
    semaphore = asyncio.Semaphore(max_transfers)
    client_kwargs = {}
    if catch_up:
        if "catch_up" in inspect.signature(telegram_client_type.__init__).parameters:
            client_kwargs["catch_up"] = True
        else:
            logger.warning("Эта версия Telethon не поддерживает catch_up — пропущенные сообщения догонять не буду")

    client = telegram_client_type(
        session_path,
        api_id,
        api_hash,
        connection_retries=-1,
        retry_delay=2,
        auto_reconnect=True,
        **client_kwargs,
    )
    on_client_created(client)
    # Keep status messages literal; Telethon otherwise parses Markdown by default.
    client.parse_mode = None
    client.add_event_handler(event_handler, event_type(incoming=True))

    download_service = download_service_type(
        primary_client=client,
        api_id=api_id,
        api_hash=api_hash,
        **download_options,
    )
    http_client = httpx_module.AsyncClient(
        timeout=http_timeout,
        limits=httpx_module.Limits(max_connections=10, max_keepalive_connections=4),
    )
    on_http_client_created(http_client)
    copyparty_files = copyparty_files_type(
        http_client=http_client,
        **copyparty_options,
    )
    return RuntimeResources(
        client=client,
        download_service=download_service,
        http_client=http_client,
        copyparty_files=copyparty_files,
        upload_semaphore=semaphore,
    )
