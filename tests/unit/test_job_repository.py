from __future__ import annotations

from pathlib import Path

from app.domain.report.job_repository import JobStatus
from app.infrastructure.job_repository import JOBS_DB_FILENAME, SqliteJobRepository


def _repo(shared_jobs_root: Path, processing_timeout_seconds: int = 600) -> SqliteJobRepository:
    return SqliteJobRepository(
        shared_jobs_root=str(shared_jobs_root),
        processing_timeout_seconds=processing_timeout_seconds,
    )


def test_create_queued_job_persists_across_repository_instances(tmp_path: Path) -> None:
    repo = _repo(tmp_path)

    created_job = repo.create_queued_job("job-1")
    reloaded_job = _repo(tmp_path).get_job("job-1")

    assert reloaded_job is not None
    assert reloaded_job.job_id == created_job.job_id
    assert reloaded_job.status == JobStatus.queued
    assert reloaded_job.input_path == str(tmp_path / "job-1" / "input")
    assert reloaded_job.stats_path == str(tmp_path / "job-1" / "stats.sqlite")
    assert reloaded_job.output_path == str(tmp_path / "job-1" / "report.xlsx")
    assert (tmp_path / JOBS_DB_FILENAME).is_file()


def test_claim_queued_job_has_single_winner(tmp_path: Path) -> None:
    creator = _repo(tmp_path, processing_timeout_seconds=123)
    creator.create_queued_job("job-claim")

    first_worker = _repo(tmp_path, processing_timeout_seconds=123)
    second_worker = _repo(tmp_path, processing_timeout_seconds=123)

    claimed_job = first_worker.claim_queued_job("job-claim")
    duplicate_claim = second_worker.claim_queued_job("job-claim")
    persisted_job = creator.get_job("job-claim")

    assert claimed_job is not None
    assert claimed_job.status == JobStatus.processing
    assert claimed_job.started_at is not None
    assert claimed_job.lease_expires_at is not None
    assert duplicate_claim is None
    assert persisted_job is not None
    assert persisted_job.status == JobStatus.processing
    assert persisted_job.started_at == claimed_job.started_at
    assert persisted_job.lease_expires_at == claimed_job.lease_expires_at


def test_mark_job_done_requires_processing_status(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    repo.create_queued_job("job-done")

    assert repo.mark_job_done("job-done", line_count=10, unique_lemma_count=3) is None

    repo.claim_queued_job("job-done")
    done_job = repo.mark_job_done("job-done", line_count=10, unique_lemma_count=3)

    assert done_job is not None
    assert done_job.status == JobStatus.done
    assert done_job.finished_at is not None
    assert done_job.lease_expires_at is None
    assert done_job.line_count == 10
    assert done_job.unique_lemma_count == 3


def test_mark_job_failed_requires_processing_status(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    repo.create_queued_job("job-failed")

    assert repo.mark_job_failed("job-failed", error_code="queue_unavailable") is None

    repo.claim_queued_job("job-failed")
    failed_job = repo.mark_job_failed(
        "job-failed",
        error_code="queue_unavailable",
        error_message="queue is unavailable",
    )

    assert failed_job is not None
    assert failed_job.status == JobStatus.failed
    assert failed_job.finished_at is not None
    assert failed_job.lease_expires_at is None
    assert failed_job.error_code == "queue_unavailable"
    assert failed_job.error_message == "queue is unavailable"
