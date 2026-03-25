from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from threading import Lock

from app.core.settings import get_settings
from app.domain.report.job_repository import Job, JobRepository, JobStatus


class InMemoryJobRepository(JobRepository):
    """Step 1 repository: non-persistent, used only to fix MVP contract.

    Step 2 will replace it with a persistent implementation (sqlite, with WAL).
    """

    def __init__(self, shared_jobs_root: str):
        self._shared_jobs_root = Path(shared_jobs_root)
        self._lock = Lock()
        self._jobs: dict[str, Job] = {}

    def _job_dir(self, job_id: str) -> Path:
        return self._shared_jobs_root / job_id

    def _input_path(self, job_id: str) -> str:
        return str(self._job_dir(job_id) / "input")

    def _stats_path(self, job_id: str) -> str:
        return str(self._job_dir(job_id) / "stats.sqlite")

    def _output_path(self, job_id: str) -> str:
        return str(self._job_dir(job_id) / "report.xlsx")

    def create_queued_job(self, job_id: str) -> Job:
        now = datetime.now(timezone.utc)
        job_dir = self._job_dir(job_id)
        job_dir.mkdir(parents=True, exist_ok=True)

        job = Job(
            job_id=job_id,
            status=JobStatus.queued,
            created_at=now,
            updated_at=now,
            input_path=self._input_path(job_id),
            stats_path=self._stats_path(job_id),
            output_path=self._output_path(job_id),
        )

        with self._lock:
            self._jobs[job_id] = job
        return job

    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            existing = self._jobs.get(job_id)
            if existing is None:
                return None
            # Avoid accidental external mutation.
            return replace(existing)


@lru_cache(maxsize=1)
def get_job_repository() -> JobRepository:
    settings = get_settings()
    return InMemoryJobRepository(shared_jobs_root=settings.shared_jobs_root)

