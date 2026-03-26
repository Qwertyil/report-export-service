from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from app.domain.report.constants import MVP_ERROR_CODES

JobStatus = Literal["queued", "processing", "done", "failed"]

JobErrorCode = Literal[
    "queue_unavailable",
    "unsupported_encoding",
    "processing_timeout",
    "xlsx_cell_limit",
    "xlsx_row_limit",
    "artifact_missing",
]


class JobError(BaseModel):
    error_code: JobErrorCode
    error_message: str | None = None


class ExportSubmitResponse(BaseModel):
    job_id: str
    status: JobStatus
    status_url: str
    download_url: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    download_url: str | None = None
    error: JobError | None = None


def validate_mvp_error_code(value: str) -> JobErrorCode:
    # Runtime guard to keep the response contract strict.
    if value not in MVP_ERROR_CODES:
        raise ValueError(f"Unknown MVP error_code: {value}")
    return value  # type: ignore[return-value]
