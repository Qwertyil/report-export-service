"""Job repository interface and persisted job model."""

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

    def claim_queued_job(self, job_id: str) -> Job | None: ...

    def mark_queued_job_failed(
        self,
        job_id: str,
        *,
        error_code: str,
        error_message: str | None = None,
    ) -> Job | None: ...

    def mark_job_done(self, job_id: str, *, line_count: int, unique_lemma_count: int) -> Job | None: ...

    def mark_job_failed(
        self,
        job_id: str,
        *,
        error_code: str,
        error_message: str | None = None,
    ) -> Job | None: ...

    def get_job(self, job_id: str) -> Job | None: ...
