"""Job repository interface and in-memory-compatible job model.

Step 2 will replace the implementation with a persistent repository.
For Step 1 we keep the contract minimal, so HTTP handlers don't depend on
Celery AsyncResult payloads.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol


class JobStatus(StrEnum):
    queued = "queued"
    processing = "processing"
    done = "done"
    failed = "failed"


@dataclass(slots=True)
class Job:
    job_id: str
    status: JobStatus

    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    lease_expires_at: datetime | None = None
    finished_at: datetime | None = None

    input_path: str | None = None
    stats_path: str | None = None
    output_path: str | None = None

    line_count: int | None = None
    unique_lemma_count: int | None = None

    error_code: str | None = None
    error_message: str | None = None


class JobRepository(Protocol):
    def create_queued_job(self, job_id: str) -> Job: ...

    def get_job(self, job_id: str) -> Job | None: ...

