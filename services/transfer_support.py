"""Shared support components used by transfer execution and status reporting."""

from services.batch_status import BatchManager
from services.media_processing import MediaProcessor
from storage.disk_space import DiskSpaceGuard


class TransferSupport:
    def __init__(
        self, *, tmp_dir, check_disk, min_free_disk_after_bytes, format_size,
        transfer_error, active_transfers, disk_usage, logger,
        spawn, refresh_jellyfin, safe_edit, batch_grace_sec,
        progress_edit_interval_sec, max_batch_lines, upload_retries,
        download_retries, ffmpeg_bin, ffprobe_bin, ffmpeg_timeout_sec,
        music_video_max_minutes, unknown_duration_max_bytes, remove_quietly,
    ):
        self._refresh_jellyfin = refresh_jellyfin
        self.disk_space = DiskSpaceGuard(
            tmp_dir=tmp_dir,
            enabled=check_disk,
            minimum_free_bytes=min_free_disk_after_bytes,
            format_size=format_size,
            transfer_error=transfer_error,
            active_transfers=active_transfers,
            disk_usage=disk_usage,
            logger=logger,
        )
        self.batch_manager = BatchManager(
            spawn=spawn,
            refresh_jellyfin=refresh_jellyfin,
            safe_edit=safe_edit,
            batch_grace_sec=batch_grace_sec,
            progress_edit_interval_sec=progress_edit_interval_sec,
            max_batch_lines=max_batch_lines,
            upload_retries=upload_retries,
            download_retries=download_retries,
        )
        self.media_processor = MediaProcessor(
            ffmpeg_bin=ffmpeg_bin,
            ffprobe_bin=ffprobe_bin,
            timeout_sec=ffmpeg_timeout_sec,
            music_video_max_minutes=music_video_max_minutes,
            unknown_duration_max_bytes=unknown_duration_max_bytes,
            remove_quietly=remove_quietly,
        )

    def ensure_disk_space(self, needed_bytes, state) -> None:
        self.disk_space.ensure(needed_bytes, state)

    async def refresh_jellyfin(self):
        return await self._refresh_jellyfin()

    def join_batch(self, chat_id, send, item):
        return self.batch_manager.join(chat_id, send, item)
