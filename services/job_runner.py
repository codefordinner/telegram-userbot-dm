"""Execute one resumable transfer job and publish its result."""

import logging

from services.batch_status import BatchItem
from domain.transfer_models import ACTIVE_TRANSFERS, TransferState


log = logging.getLogger("userbot-dm-uploader")


class JobRunner:
    def __init__(
        self, *, join_batch, upload_semaphore, transfer_file, record_history,
        describe_error, drop_job_files, temp_dir,
    ):
        self.join_batch = join_batch
        self.upload_semaphore = upload_semaphore
        self.transfer_file = transfer_file
        self.record_history = record_history
        self.describe_error = describe_error
        self.drop_job_files = drop_job_files
        self.temp_dir = temp_dir

    async def run(self, job, message, send) -> None:
        item = BatchItem(job=job, state=TransferState(filename=job.filename, total=job.size or 1))
        batch = self.join_batch(job.chat_id, send, item)
        finished = False
        try:
            await batch.ensure_status()
            async with self.upload_semaphore():
                item.status = "running"
                ACTIVE_TRANSFERS.add(item.state)
                try:
                    existed, where, remote_name = await self.transfer_file(
                        message, job, job.local_path(self.temp_dir), item.state
                    )
                    item.mark_ok(existed, where)
                    await self.record_history(job, remote_name, existed)
                except Exception as exc:
                    log.exception("upload failed")
                    item.mark_error(self.describe_error(exc))
                finally:
                    ACTIVE_TRANSFERS.discard(item.state)
            finished = True
        finally:
            if finished:
                self.drop_job_files(job)
            batch.item_finished()
