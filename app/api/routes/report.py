import shutil
import uuid
from pathlib import Path
from typing import cast

from fastapi import APIRouter, File, HTTPException, UploadFile, status
from fastapi.responses import FileResponse
from starlette.concurrency import run_in_threadpool

from app.api.schemas.report import (
    ExportSubmitResponse,
    JobError,
    JobStatusResponse,
    validate_mvp_error_code,
)
from app.api.schemas.report import JobStatus as ResponseJobStatus
from app.core.settings import get_settings
from app.domain.report.job_repository import Job, JobRepository, JobStatus
from app.infrastructure.celery_app import enqueue_report_job
from app.infrastructure.job_repository import get_job_repository

router = APIRouter(prefix="/report", tags=["report"])


def _best_effort_unlink(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


async def _best_effort_unlink_async(path: Path) -> None:
    try:
        await run_in_threadpool(path.unlink, missing_ok=True)
    except OSError:
        pass


async def _best_effort_rmtree_async(path: Path) -> None:
    try:
        await run_in_threadpool(shutil.rmtree, str(path), True)
    except OSError:
        pass


def _accepted_submit_response(job_id: str, *, submit_status: JobStatus = JobStatus.queued) -> ExportSubmitResponse:
    settings = get_settings()
    return ExportSubmitResponse(
        job_id=job_id,
        status=cast(ResponseJobStatus, submit_status.value),
        status_url=f"{settings.api_prefix}/report/{job_id}/status",
        download_url=f"{settings.api_prefix}/report/{job_id}/download",
    )


async def _persist_upload(upload: UploadFile, destination: Path, max_size_bytes: int, chunk_size: int) -> None:
    bytes_written = 0
    output = None
    cleanup_path = False

    try:
        output = await run_in_threadpool(destination.open, "wb")
        while True:
            chunk = await upload.read(chunk_size)
            if not chunk:
                break

            bytes_written += len(chunk)
            if bytes_written > max_size_bytes:
                cleanup_path = True
                raise HTTPException(
                    status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                    detail="payload too large",
                )

            await run_in_threadpool(output.write, chunk)
    except Exception:
        cleanup_path = True
        raise
    finally:
        if output is not None:
            await run_in_threadpool(output.close)
        if cleanup_path:
            await _best_effort_unlink_async(destination)


def _job_dir(shared_jobs_root: str, job_id: str) -> Path:
    return Path(shared_jobs_root) / job_id


def _done_job_output_exists(job: Job) -> bool:
    if job.output_path is None:
        return False
    return Path(job.output_path).is_file()


def _repair_done_job_missing_artifact(repo: JobRepository, job: Job) -> Job:
    repaired_job = repo.repair_artifact_missing(
        job.job_id,
        error_message="report artifact is missing",
    )
    if repaired_job is not None:
        return repaired_job

    refreshed_job = repo.get_job(job.job_id)
    if refreshed_job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_id not found")
    return refreshed_job


def _get_job_for_public_read(job_id: str) -> Job:
    repo = get_job_repository()
    job = repo.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_id not found")

    if job.status != JobStatus.done or _done_job_output_exists(job):
        return job

    return _repair_done_job_missing_artifact(repo, job)


@router.post(
    "/export",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ExportSubmitResponse,
)
async def export_report(file: UploadFile | None = File(None)) -> ExportSubmitResponse:
    if file is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="file is required")

    settings = get_settings()
    job_id = str(uuid.uuid4())
    job_dir = _job_dir(settings.shared_jobs_root, job_id)
    input_path = job_dir / "input"
    job_dir.mkdir(parents=True, exist_ok=True)

    try:
        await _persist_upload(
            file,
            input_path,
            max_size_bytes=settings.max_upload_size_bytes,
            chunk_size=settings.read_chunk_size,
        )
    except Exception:
        # Upload failed before any durable job row exists: remove partial input and job dir.
        await _best_effort_rmtree_async(job_dir)
        raise
    finally:
        await file.close()

    repo = get_job_repository()

    try:
        repo.create_queued_job(job_id)
    except Exception:
        _best_effort_unlink(input_path)
        raise

    try:
        enqueue_report_job(job_id)
    except Exception:
        failed_job = repo.mark_queued_job_failed(
            job_id,
            error_code="queue_unavailable",
            error_message="queue is unavailable",
        )
        if failed_job is not None:
            _best_effort_unlink(input_path)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="queue unavailable",
            ) from None
        # Broker may have accepted the task before the client saw an error; if the
        # job is no longer queued (e.g. a worker claimed it), return job_id so the
        # client can poll instead of a 500 with no id.
        existing = repo.get_job(job_id)
        if existing is not None:
            return _accepted_submit_response(job_id, submit_status=existing.status)
        _best_effort_unlink(input_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="job state transition failed",
        ) from None

    return _accepted_submit_response(job_id)


@router.get("/{job_id}/status", response_model=JobStatusResponse)
async def get_report_status(job_id: str) -> JobStatusResponse:
    job = _get_job_for_public_read(job_id)

    settings = get_settings()
    download_url = f"{settings.api_prefix}/report/{job_id}/download"

    if job.status == JobStatus.done:
        return JobStatusResponse(
            job_id=job.job_id,
            status=cast(ResponseJobStatus, job.status.value),
            download_url=download_url,
            error=None,
        )

    if job.status == JobStatus.failed:
        if job.error_code is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="failed job has no valid error_code",
            )

        error = JobError(
            error_code=validate_mvp_error_code(job.error_code),
            error_message=job.error_message,
        )
        return JobStatusResponse(
            job_id=job.job_id,
            status=cast(ResponseJobStatus, job.status.value),
            download_url=None,
            error=error,
        )

    return JobStatusResponse(
        job_id=job.job_id,
        status=cast(ResponseJobStatus, job.status.value),
        download_url=None,
        error=None,
    )


@router.get("/{job_id}/download")
async def download_report(job_id: str) -> FileResponse:
    repo = get_job_repository()
    job = _get_job_for_public_read(job_id)

    if job.status != JobStatus.done:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="job is not ready for download")

    if job.output_path is None:
        _repair_done_job_missing_artifact(repo, job)
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="job is not ready for download")

    output_path = Path(job.output_path)
    if not output_path.is_file():
        _repair_done_job_missing_artifact(repo, job)
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="job is not ready for download")

    return FileResponse(
        str(output_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"report-{job_id}.xlsx",
    )
