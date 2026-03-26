from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi import HTTPException, status

from app.api.routes import report as report_route
from app.domain.report.job_repository import Job, JobStatus


class _StubUpload:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = iter(chunks)

    async def read(self, chunk_size: int) -> bytes:
        return next(self._chunks)


def test_persist_upload_offloads_file_io(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[str] = []

    async def fake_run_in_threadpool(func, *args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(func.__name__)
        return func(*args, **kwargs)

    monkeypatch.setattr(report_route, "run_in_threadpool", fake_run_in_threadpool)

    destination = tmp_path / "upload.bin"
    upload = _StubUpload([b"ab", b"cd", b""])

    asyncio.run(
        report_route._persist_upload(
            upload=upload,  # type: ignore[arg-type]
            destination=destination,
            max_size_bytes=8,
            chunk_size=2,
        )
    )

    assert destination.read_bytes() == b"abcd"
    assert calls == ["open", "write", "write", "close"]


def test_persist_upload_rejects_over_limit_and_unlinks_partial(tmp_path: Path) -> None:
    upload = _StubUpload([b"hi", b"there", b""])
    destination = tmp_path / "out"

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            report_route._persist_upload(
                upload=upload,  # type: ignore[arg-type]
                destination=destination,
                max_size_bytes=4,
                chunk_size=10,
            )
        )

    assert excinfo.value.status_code == status.HTTP_413_CONTENT_TOO_LARGE
    assert not destination.exists()


def test_best_effort_unlink_swallows_oserror(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "f"

    def boom(self: Path, *args, **kwargs):  # type: ignore[no-untyped-def]
        raise OSError("refused")

    monkeypatch.setattr(Path, "unlink", boom)
    report_route._best_effort_unlink(path)


def test_best_effort_unlink_async_swallows_oserror(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "f"

    def boom(self: Path, *args, **kwargs):  # type: ignore[no-untyped-def]
        raise OSError("refused")

    monkeypatch.setattr(Path, "unlink", boom)
    asyncio.run(report_route._best_effort_unlink_async(path))


class _StubJobRepository:
    def __init__(self, job: Job | None) -> None:
        self.job = job
        self.repair_calls: list[tuple[str, str | None]] = []

    def get_job(self, job_id: str) -> Job | None:
        if self.job is None or self.job.job_id != job_id:
            return None
        return self.job

    def repair_artifact_missing(
        self,
        job_id: str,
        *,
        error_message: str | None = None,
    ) -> Job | None:
        self.repair_calls.append((job_id, error_message))
        if self.job is None or self.job.job_id != job_id:
            return None
        self.job = Job(
            job_id=self.job.job_id,
            status=JobStatus.failed,
            created_at=self.job.created_at,
            updated_at=self.job.updated_at,
            started_at=self.job.started_at,
            lease_expires_at=None,
            finished_at=self.job.finished_at,
            input_path=self.job.input_path,
            stats_path=self.job.stats_path,
            output_path=self.job.output_path,
            line_count=None,
            unique_lemma_count=None,
            error_code="artifact_missing",
            error_message=error_message,
        )
        return self.job


def _job(*, status: JobStatus, output_path: str | None) -> Job:
    now = datetime.now(timezone.utc)
    return Job(
        job_id="job-1",
        status=status,
        created_at=now,
        updated_at=now,
        finished_at=now if status in {JobStatus.done, JobStatus.failed} else None,
        input_path="/tmp/input",
        stats_path="/tmp/stats.sqlite",
        output_path=output_path,
        error_code=None,
        error_message=None,
    )


def test_get_job_for_public_read_repairs_done_job_when_output_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _StubJobRepository(_job(status=JobStatus.done, output_path=None))
    monkeypatch.setattr(report_route, "get_job_repository", lambda: repo)

    job = report_route._get_job_for_public_read("job-1")

    assert job.status == JobStatus.failed
    assert job.error_code == "artifact_missing"
    assert job.error_message == "report artifact is missing"
    assert repo.repair_calls == [("job-1", "report artifact is missing")]
