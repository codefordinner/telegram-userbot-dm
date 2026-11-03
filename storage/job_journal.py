"""Disk journal and temporary-file cleanup for resumable upload jobs.

Job records remain JSON files in the existing ``TMP_DIR/jobs`` layout so older
deployments can continue pending work after upgrading the image.
"""

import json
import logging
import os
import time
from dataclasses import asdict
from pathlib import Path
from typing import Callable


log = logging.getLogger("userbot-dm-uploader")


def _remove_quietly(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


class JobJournal:
    def __init__(self, tmp_dir: Path, journal_dir: Path, max_age_sec: int, max_starts: int):
        self.tmp_dir = Path(tmp_dir)
        self.journal_dir = Path(journal_dir)
        self.max_age_sec = max_age_sec
        self.max_starts = max_starts

    @staticmethod
    def meta_path(local_path: Path) -> Path:
        """Return the resume metadata path associated with a partial file."""
        return local_path.with_name(local_path.name + ".meta")

    def drop_files(self, job) -> None:
        """Remove all temporary files and the journal entry associated with a job."""
        local_path = job.local_path(self.tmp_dir)
        _remove_quietly(local_path)
        _remove_quietly(self.meta_path(local_path))
        _remove_quietly(local_path.with_suffix(".m4a"))
        if job.url:
            for path in self.tmp_dir.glob(local_path.stem + ".*"):
                _remove_quietly(path)
        _remove_quietly(job.journal_path(self.journal_dir))

    def write(self, job) -> None:
        try:
            self.journal_dir.mkdir(parents=True, exist_ok=True)
            journal_path = job.journal_path(self.journal_dir)
            temporary = journal_path.with_name(journal_path.name + ".tmp")
            temporary.write_text(json.dumps(asdict(job), ensure_ascii=False), encoding="utf-8")
            os.replace(temporary, journal_path)
        except OSError as exc:
            log.warning("Не удалось записать задание в журнал (после перезапуска докачки не будет): %s", exc)

    def load(self, job_factory: Callable[..., object]) -> list:
        """Load fresh, recoverable jobs and discard malformed or exhausted records."""
        jobs = []
        now = time.time()
        for path in sorted(self.journal_dir.glob("*.json")):
            try:
                job = job_factory(**json.loads(path.read_text(encoding="utf-8")))
                fresh = now - path.stat().st_mtime < self.max_age_sec
            except (OSError, ValueError, TypeError):
                _remove_quietly(path)
                continue
            if not fresh or job.starts >= self.max_starts:
                log.info("Задание «%s» не возобновляю (старое или слишком часто падало) — удаляю", job.filename)
                self.drop_files(job)
                continue
            jobs.append(job)
        return jobs

    def prepare(self, job_factory: Callable[..., object], resume_on_start: bool) -> list:
        """Clean orphaned temporary files and return jobs that should resume."""
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.journal_dir.mkdir(parents=True, exist_ok=True)
        if resume_on_start:
            jobs = self.load(job_factory)
        else:
            jobs = []
            for path in self.journal_dir.glob("*"):
                _remove_quietly(path)

        for path in self.journal_dir.glob("*.tmp"):
            _remove_quietly(path)

        keep = set()
        for job in jobs:
            local_path = job.local_path(self.tmp_dir)
            keep.add(local_path.name)
            keep.add(self.meta_path(local_path).name)

        removed = 0
        for path in self.tmp_dir.iterdir():
            if path.is_file() and path.name not in keep:
                _remove_quietly(path)
                removed += 1
        if removed:
            log.info("Удалено %d ненужных временных файлов из %s", removed, self.tmp_dir)
        for job in jobs:
            log.info("К продолжению после перезапуска: «%s» (сообщение %s)", job.filename, job.message_id)
        return jobs
