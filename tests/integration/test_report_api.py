from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.settings import get_settings
from app.domain.report.job_repository import JobStatus
from app.infrastructure.job_repository import SqliteJobRepository, get_job_repository
from app.main import create_app


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    test_client = TestClient(create_app())

    try:
        yield test_client
    finally:
        get_job_repository.cache_clear()
        get_settings.cache_clear()


def _repo() -> SqliteJobRepository:
    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    return repo


def test_export_submit_returns_202_and_job_links(client: TestClient) -> None:
    response = client.post(
        "/public/report/export",
        files={"file": ("sample.txt", b"hello world", "text/plain")},
    )

    assert response.status_code == 202

    payload = response.json()
    job_id = payload["job_id"]

    assert payload == {
        "job_id": job_id,
        "status": "queued",
        "status_url": f"/public/report/{job_id}/status",
        "download_url": f"/public/report/{job_id}/download",
    }

    repo = _repo()
    job = repo.get_job(job_id)

    assert job is not None
    assert job.status == JobStatus.queued
    assert job.input_path is not None
    assert Path(job.input_path).parent.is_dir()


def test_report_status_returns_queued_for_new_job(client: TestClient) -> None:
    submit_response = client.post(
        "/public/report/export",
        files={"file": ("sample.txt", b"hello world", "text/plain")},
    )
    job_id = submit_response.json()["job_id"]

    response = client.get(f"/public/report/{job_id}/status")

    assert response.status_code == 200
    assert response.json() == {
        "job_id": job_id,
        "status": "queued",
        "download_url": None,
        "error": None,
    }


def test_report_status_returns_failed_error_payload(client: TestClient) -> None:
    repo = _repo()
    job = repo.create_queued_job("job-failed")
    claimed_job = repo.claim_queued_job(job.job_id)
    assert claimed_job is not None

    failed_job = repo.mark_job_failed(
        job.job_id,
        error_code="queue_unavailable",
        error_message="queue is unavailable",
    )
    assert failed_job is not None

    response = client.get(f"/public/report/{job.job_id}/status")

    assert response.status_code == 200
    assert response.json() == {
        "job_id": job.job_id,
        "status": "failed",
        "download_url": None,
        "error": {
            "error_code": "queue_unavailable",
            "error_message": "queue is unavailable",
        },
    }


def test_download_returns_conflict_until_job_done(client: TestClient) -> None:
    submit_response = client.post(
        "/public/report/export",
        files={"file": ("sample.txt", b"hello world", "text/plain")},
    )
    job_id = submit_response.json()["job_id"]

    response = client.get(f"/public/report/{job_id}/download")

    assert response.status_code == 409
    assert response.json() == {"detail": "job is not ready for download"}


def test_download_returns_file_for_done_job(client: TestClient) -> None:
    repo = _repo()
    job = repo.create_queued_job("job-done")
    claimed_job = repo.claim_queued_job(job.job_id)
    assert claimed_job is not None

    assert job.output_path is not None
    output_path = Path(job.output_path)
    output_path.write_bytes(b"fake-xlsx")

    done_job = repo.mark_job_done(
        job.job_id,
        line_count=1,
        unique_lemma_count=1,
    )
    assert done_job is not None

    response = client.get(f"/public/report/{job.job_id}/download")

    assert response.status_code == 200
    assert response.content == b"fake-xlsx"
    assert response.headers["content-type"].startswith(
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


def test_unknown_job_returns_404_for_status_and_download(client: TestClient) -> None:
    status_response = client.get("/public/report/missing/status")
    download_response = client.get("/public/report/missing/download")

    assert status_response.status_code == 404
    assert status_response.json() == {"detail": "job_id not found"}
    assert download_response.status_code == 404
    assert download_response.json() == {"detail": "job_id not found"}
