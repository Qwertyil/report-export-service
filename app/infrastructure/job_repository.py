from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

from app.core.settings import get_settings
from app.domain.report.job_repository import Job, JobRepository, JobStatus

SQLITE_BUSY_TIMEOUT_MS = 5_000
JOBS_DB_FILENAME = "jobs.sqlite3"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SqliteJobRepository(JobRepository):
    """Persistent job repository backed by SQLite in the shared jobs root."""

    def __init__(self, shared_jobs_root: str, processing_timeout_seconds: int):
        self._shared_jobs_root = Path(shared_jobs_root)
        self._processing_timeout_seconds = processing_timeout_seconds
        self._database_path = self._shared_jobs_root / JOBS_DB_FILENAME

        self._shared_jobs_root.mkdir(parents=True, exist_ok=True)
        self._initialize_database()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._database_path,
            timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        return connection

    def _initialize_database(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL CHECK (status IN ('queued', 'processing', 'done', 'failed')),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    lease_expires_at TEXT,
                    finished_at TEXT,
                    input_path TEXT NOT NULL,
                    stats_path TEXT NOT NULL,
                    output_path TEXT NOT NULL,
                    line_count INTEGER,
                    unique_lemma_count INTEGER,
                    error_code TEXT,
                    error_message TEXT
                )
                """
            )

    def _job_dir(self, job_id: str) -> Path:
        return self._shared_jobs_root / job_id

    def _input_path(self, job_id: str) -> str:
        return str(self._job_dir(job_id) / "input")

    def _stats_path(self, job_id: str) -> str:
        return str(self._job_dir(job_id) / "stats.sqlite")

    def _output_path(self, job_id: str) -> str:
        return str(self._job_dir(job_id) / "report.xlsx")

    def _row_to_job(self, row: sqlite3.Row) -> Job:
        return Job(
            job_id=row["job_id"],
            status=JobStatus(row["status"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            started_at=self._parse_datetime(row["started_at"]),
            lease_expires_at=self._parse_datetime(row["lease_expires_at"]),
            finished_at=self._parse_datetime(row["finished_at"]),
            input_path=row["input_path"],
            stats_path=row["stats_path"],
            output_path=row["output_path"],
            line_count=row["line_count"],
            unique_lemma_count=row["unique_lemma_count"],
            error_code=row["error_code"],
            error_message=row["error_message"],
        )

    def _fetch_job(self, connection: sqlite3.Connection, job_id: str) -> Job | None:
        row = connection.execute(
            """
            SELECT
                job_id,
                status,
                created_at,
                updated_at,
                started_at,
                lease_expires_at,
                finished_at,
                input_path,
                stats_path,
                output_path,
                line_count,
                unique_lemma_count,
                error_code,
                error_message
            FROM jobs
            WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_job(row)

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if value is None:
            return None
        return datetime.fromisoformat(value)

    def create_queued_job(self, job_id: str) -> Job:
        now = _utcnow()
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

        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO jobs (
                        job_id,
                        status,
                        created_at,
                        updated_at,
                        started_at,
                        lease_expires_at,
                        finished_at,
                        input_path,
                        stats_path,
                        output_path,
                        line_count,
                        unique_lemma_count,
                        error_code,
                        error_message
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job.job_id,
                        job.status.value,
                        job.created_at.isoformat(),
                        job.updated_at.isoformat(),
                        None,
                        None,
                        None,
                        job.input_path,
                        job.stats_path,
                        job.output_path,
                        None,
                        None,
                        None,
                        None,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"job already exists: {job_id}") from exc

        return job

    def claim_queued_job(self, job_id: str) -> Job | None:
        now = _utcnow()
        lease_expires_at = now + timedelta(seconds=self._processing_timeout_seconds)

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE jobs
                SET
                    status = ?,
                    updated_at = ?,
                    started_at = ?,
                    lease_expires_at = ?,
                    finished_at = NULL,
                    error_code = NULL,
                    error_message = NULL
                WHERE job_id = ? AND status = ?
                """,
                (
                    JobStatus.processing.value,
                    now.isoformat(),
                    now.isoformat(),
                    lease_expires_at.isoformat(),
                    job_id,
                    JobStatus.queued.value,
                ),
            )
            if cursor.rowcount == 0:
                return None
            return self._fetch_job(connection, job_id)

    def mark_job_done(self, job_id: str, *, line_count: int, unique_lemma_count: int) -> Job | None:
        now = _utcnow()

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE jobs
                SET
                    status = ?,
                    updated_at = ?,
                    finished_at = ?,
                    lease_expires_at = NULL,
                    line_count = ?,
                    unique_lemma_count = ?,
                    error_code = NULL,
                    error_message = NULL
                WHERE job_id = ? AND status = ?
                """,
                (
                    JobStatus.done.value,
                    now.isoformat(),
                    now.isoformat(),
                    line_count,
                    unique_lemma_count,
                    job_id,
                    JobStatus.processing.value,
                ),
            )
            if cursor.rowcount == 0:
                return None
            return self._fetch_job(connection, job_id)

    def mark_job_failed(
        self,
        job_id: str,
        *,
        error_code: str,
        error_message: str | None = None,
    ) -> Job | None:
        now = _utcnow()

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE jobs
                SET
                    status = ?,
                    updated_at = ?,
                    finished_at = ?,
                    lease_expires_at = NULL,
                    line_count = NULL,
                    unique_lemma_count = NULL,
                    error_code = ?,
                    error_message = ?
                WHERE job_id = ? AND status = ?
                """,
                (
                    JobStatus.failed.value,
                    now.isoformat(),
                    now.isoformat(),
                    error_code,
                    error_message,
                    job_id,
                    JobStatus.processing.value,
                ),
            )
            if cursor.rowcount == 0:
                return None
            return self._fetch_job(connection, job_id)

    def get_job(self, job_id: str) -> Job | None:
        with self._connect() as connection:
            return self._fetch_job(connection, job_id)


@lru_cache(maxsize=1)
def get_job_repository() -> JobRepository:
    settings = get_settings()
    return SqliteJobRepository(
        shared_jobs_root=settings.shared_jobs_root,
        processing_timeout_seconds=settings.processing_timeout_seconds,
    )
