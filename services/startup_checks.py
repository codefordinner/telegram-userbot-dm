"""Non-fatal checks for optional tools and configuration at application startup."""

from pathlib import Path
import shutil


class StartupChecks:
    def __init__(self, *, settings, logger):
        self.settings = settings
        self.log = logger

    def run(self) -> None:
        settings = self.settings
        if settings.library_admin:
            if not shutil.which(settings.beets_bin):
                self.log.warning(
                    "Управление библиотекой: «%s» не найден — команды /find, /del и т.д. работать не будут",
                    settings.beets_bin,
                )
            elif not Path(settings.beets_config).exists():
                self.log.warning(
                    "Управление библиотекой: нет конфига %s — смонтируйте папку с конфигом beets",
                    settings.beets_config,
                )
        if settings.link_ingest and not shutil.which(settings.ytdlp_bin):
            self.log.warning(
                "Ссылки: «%s» не найден — установите yt-dlp (pip install yt-dlp), иначе ссылки работать не будут",
                settings.ytdlp_bin,
            )
