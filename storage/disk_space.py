"""Reserve disk capacity for concurrent downloads."""

import logging

from domain.transfer_models import ACTIVE_TRANSFERS


log = logging.getLogger("userbot-dm-uploader")


class DiskSpaceGuard:
    def __init__(
        self, *, tmp_dir, enabled, minimum_free_bytes, format_size,
        transfer_error, active_transfers=ACTIVE_TRANSFERS,
        disk_usage=None, logger=log,
    ):
        import shutil

        self.tmp_dir = tmp_dir
        self.enabled = enabled
        self.minimum_free_bytes = minimum_free_bytes
        self.format_size = format_size
        self.transfer_error = transfer_error
        self.active_transfers = active_transfers
        self.disk_usage = disk_usage or shutil.disk_usage
        self.log = logger

    def ensure(self, needed_bytes, state) -> None:
        """Check free space and reserve this transfer's expected disk growth."""
        state.disk_reserved = max(needed_bytes, 0)
        if not self.enabled or needed_bytes <= 0:
            return
        try:
            usage = self.disk_usage(self.tmp_dir)
        except OSError as exc:
            self.log.warning("Не удалось проверить свободное место в %s: %s", self.tmp_dir, exc)
            return
        pending_elsewhere = sum(
            transfer.disk_reserved
            for transfer in self.active_transfers
            if transfer is not state
        )
        free = usage.free - pending_elsewhere
        if free < needed_bytes * 1.1 or free - needed_bytes < self.minimum_free_bytes:
            state.disk_reserved = 0
            raise self.transfer_error(
                f"на диске бота мало места: нужно {self.format_size(needed_bytes)}, "
                f"свободно {self.format_size(max(free, 0))} "
                f"(должно остаться не меньше {self.format_size(self.minimum_free_bytes)})"
            )
