"""Coordinate copyparty file and directory adapters behind lazy runtime access."""


class CopypartyGateway:
    def __init__(
        self, *, directories_type, series_lock, remote_url,
        ok_mkcol_statuses, request_timeout,
    ):
        self._files = None
        self._http = None
        self.directories_type = directories_type
        self.series_lock = series_lock
        self.remote_url = remote_url
        self.ok_mkcol_statuses = ok_mkcol_statuses
        self.request_timeout = request_timeout

    def initialize(self, *, files, http_client):
        """Attach resources produced during application startup."""
        self._files = files
        self._http = http_client

    def files(self):
        client = self._files
        if client is None:
            raise RuntimeError("Copyparty client has not been initialized")
        return client

    def directories(self):
        http_client = self._http
        if http_client is None:
            raise RuntimeError("HTTP client has not been initialized")
        return self.directories_type(
            http_client,
            self.series_lock,
            self.remote_url,
            self.ok_mkcol_statuses,
            self.request_timeout,
        )

    async def get_remote_size(self, url, user, password, filename):
        return await self.files().get_remote_size(url, user, password, filename)

    async def upload_with_retry(self, url, user, password, filename, local_path, state):
        return await self.files().upload_with_retry(url, user, password, filename, local_path, state)

    async def delete_remote_quietly(self, url, user, password, name):
        return await self.files().delete_remote_quietly(url, user, password, name)

    async def resolve_series_dirs(self, url, user, password, show, season):
        return await self.directories().resolve_series_dirs(url, user, password, show, season)

    async def list_remote_entries(self, url, user, password, segments):
        return await self.directories().list_remote_entries(url, user, password, segments)

    async def find_series_dirs(self, url, user, password, show, season):
        return await self.directories().find_series_dirs(url, user, password, show, season)

    async def prune_empty_dirs(self, url, user, password, segments):
        """Remove empty season/show folders, confirming emptiness before each delete."""
        while segments:
            entries = await self.list_remote_entries(url, user, password, segments)
            if entries is None or entries[0] or entries[1]:
                return
            if not await self.delete_remote_quietly(url, user, password, "/".join(segments)):
                return
            segments = segments[:-1]
