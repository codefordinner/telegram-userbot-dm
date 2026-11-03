"""Compose transfer execution, history recording, and restart recovery."""

import logging

from services.job_runner import JobRunner
from services.job_recovery import JobRecovery


class JobRuntime:
    def __init__(
        self, *, transfer_service, history_store, job_journal, temp_dir,
        job_type, resume_on_start, active_jobs, upload_semaphore, join_batch,
        run_blocking, describe_error, spawn, get_client, logger: logging.Logger,
    ):
        self.history_store = history_store
        self.job_journal = job_journal
        self.run_blocking = run_blocking
        self.log = logger
        self.runner = JobRunner(
            join_batch=join_batch,
            upload_semaphore=upload_semaphore,
            transfer_file=transfer_service.transfer_file,
            record_history=self.record_history,
            describe_error=describe_error,
            drop_job_files=job_journal.drop_files,
            temp_dir=temp_dir,
        )
        self.recovery = JobRecovery(
            prepare_jobs=lambda: job_journal.prepare(job_type, resume_on_start),
            active_jobs=active_jobs,
            journal_write=job_journal.write,
            spawn=spawn,
            get_client=get_client,
            run_job=self.run,
            drop_job_files=job_journal.drop_files,
        )

    async def run(self, job, message, send) -> None:
        await self.runner.run(job, message, send)

    async def record_history(self, job, remote_name: str, existed: bool) -> None:
        """History failures must not turn a successful remote upload into a failed job."""
        source = "subtitle" if job.subtitle else ("link" if job.url else "telegram")
        try:
            await self.run_blocking(
                self.history_store.add,
                job.chat_id,
                job.kind,
                remote_name,
                job.filename,
                job.size,
                existed,
                source,
            )
        except Exception as exc:
            self.log.warning("Не удалось записать загрузку в историю: %s", exc)
