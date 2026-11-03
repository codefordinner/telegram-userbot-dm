"""Application startup, resume scheduling, and client shutdown."""

import contextlib


class BotLifecycle:
    def __init__(
        self, *, get_client, job_recovery, initialize_runtime, startup_warnings,
        start_helper, helper_client, close_http, allowed_senders, version, logger,
    ):
        self.get_client = get_client
        self.job_recovery = job_recovery
        self.initialize_runtime = initialize_runtime
        self.startup_warnings = startup_warnings
        self.start_helper = start_helper
        self.helper_client = helper_client
        self.close_http = close_http
        self.allowed_senders = allowed_senders
        self.version = version
        self.log = logger

    async def run(self) -> None:
        pending_jobs = self.job_recovery.prepare()
        client = None
        try:
            self.initialize_runtime()
            client = self.get_client()
            self.startup_warnings()
            await self.start_helper()
            await client.start()
            me = await client.get_me()
            self.log.info(
                "Userbot started as %s (id=%s), watching DMs from %s, version %s",
                me.username, me.id, self.allowed_senders or "ВСЕХ (!)", self.version,
            )
            self.job_recovery.schedule(pending_jobs)
            await client.run_until_disconnected()
        finally:
            if client is None:
                client = self.get_client()
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.disconnect()
            helper = self.helper_client()
            if helper is not None:
                with contextlib.suppress(Exception):
                    await helper.disconnect()
            await self.close_http()
