import uuid
from pathlib import Path
from typing import cast

from fastapi import APIRouter, File, HTTPException, UploadFile, status
from fastapi.responses import FileResponse

from app.api.schemas.report import (
    ExportSubmitResponse,
    JobError,
    JobStatusResponse,
    validate_mvp_error_code,
)
from app.api.schemas.report import JobStatus as ResponseJobStatus
from app.core.settings import get_settings
from app.domain.report.job_repository import JobStatus
from app.infrastructure.job_repository import get_job_repository

router = APIRouter(prefix="/report", tags=["report"])


@router.post(
    "/export",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ExportSubmitResponse,
)
async def export_report(file: UploadFile = File(...)) -> ExportSubmitResponse:
    # Step 1 contract: async submit that returns immediately with job identifiers.
    if file is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="file is required")

    settings = get_settings()
    job_id = str(uuid.uuid4())

    repo = get_job_repository()
    repo.create_queued_job(job_id)

    status_url = f"{settings.api_prefix}/report/{job_id}/status"
    download_url = f"{settings.api_prefix}/report/{job_id}/download"

    return ExportSubmitResponse(
        job_id=job_id,
        status="queued",
        status_url=status_url,
        download_url=download_url,
    )


@router.get("/{job_id}/status", response_model=JobStatusResponse)
async def get_report_status(job_id: str) -> JobStatusResponse:
    repo = get_job_repository()
    job = repo.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_id not found")

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
    job = repo.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_id not found")

    if job.status != JobStatus.done:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="job is not ready for download")

    if job.output_path is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="artifact missing")

    output_path = Path(job.output_path)
    if not output_path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="artifact missing")

    return FileResponse(
        str(output_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"report-{job_id}.xlsx",
    )
