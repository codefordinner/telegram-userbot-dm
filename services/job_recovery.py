"""Prepare and resume jobs interrupted by a process restart."""

import functools
import logging


log = logging.getLogger("userbot-dm-uploader")


class JobRecovery:
    def __init__(
        self, *, prepare_jobs, active_jobs, journal_write, spawn,
        get_client, run_job, drop_job_files,
    ):
        self.prepare_jobs = prepare_jobs
        self.active_jobs = active_jobs
        self.journal_write = journal_write
        self.spawn = spawn
        self.get_client = get_client
        self.run_job = run_job
        self.drop_job_files = drop_job_files

    def prepare(self) -> list:
        jobs = self.prepare_jobs()
        for job in jobs:
            self.active_jobs.add(job.key)
        return jobs

    def schedule(self, jobs: list) -> None:
        for job in jobs:
            job.starts += 1
            self.journal_write(job)
            self.spawn(self.resume_one(job))

    async def resume_one(self, job) -> None:
        try:
            client = self.get_client()
            message = None
            if not job.url:
                message = await client.get_messages(job.chat_id, ids=job.message_id)
                if not message or not message.document:
                    log.warning("Сообщение %s пропало — задание «%s» отбрасываю", job.message_id, job.filename)
                    self.drop_job_files(job)
                    return
            log.info("Продолжаю после перезапуска: «%s»", job.filename)
            await self.run_job(job, message, functools.partial(client.send_message, job.chat_id))
        except Exception:
            log.exception("Не удалось продолжить задание «%s»", job.filename)
        finally:
            self.active_jobs.discard(job.key)
