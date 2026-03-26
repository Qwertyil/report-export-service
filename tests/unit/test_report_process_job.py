from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.domain.report.job_repository import Job, JobStatus
from app.domain.report.normalizer import LemmaNormalizer
from app.infrastructure.celery_app import (
    REPORT_EXPORT_TASK_NAME,
    celery_app,
    enqueue_report_job,
)
from app.infrastructure.job_repository import SqliteJobRepository, get_job_repository
from app.infrastructure.report_stats_storage import SqliteReportStatsStorage
from app.workers.report_process_job import _collect_stats, _try_collect_stats, run_report_job


def test_process_job_task_is_registered() -> None:
    task = celery_app.tasks.get(REPORT_EXPORT_TASK_NAME)
    assert task is not None
    assert task.name == REPORT_EXPORT_TASK_NAME


def test_enqueue_report_job_send_task_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _capture(*args: object, **kwargs: object) -> None:
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(celery_app, "send_task", _capture)

    enqueue_report_job("job-queue")

    assert captured["args"] == (REPORT_EXPORT_TASK_NAME,)
    assert captured["kwargs"] == {
        "kwargs": {"job_id": "job-queue"},
        "queue": celery_app.conf.task_default_queue,
    }


def test_run_report_job_marks_done_with_placeholder_xlsx_and_stats(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-1")
    created = repo.get_job("job-1")
    assert created is not None
    assert created.input_path is not None
    assert created.stats_path is not None
    input_path = Path(created.input_path)
    input_path.write_bytes("Кошки\nкошку\n".encode("utf-8"))

    run_report_job("job-1")

    job = repo.get_job("job-1")
    assert job is not None
    assert job.status == JobStatus.done
    assert job.line_count == 2
    assert job.unique_lemma_count == 1
    assert job.output_path is not None
    assert Path(job.output_path).is_file()
    stats = SqliteReportStatsStorage(created.stats_path)
    assert stats.fetch_lemma_totals() == [("кошка", 2)]
    assert stats.fetch_line_counts() == [("кошка", 1, 1), ("кошка", 2, 1)]


def test_run_report_job_skips_when_not_queued(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-solo")
    solo = repo.get_job("job-solo")
    assert solo is not None
    assert solo.input_path is not None
    Path(solo.input_path).write_bytes(b"x")

    run_report_job("job-solo")
    run_report_job("job-solo")

    job = repo.get_job("job-solo")
    assert job is not None
    assert job.status == JobStatus.done


def test_run_report_job_fails_unknown_encoding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-bin")
    binary_job = repo.get_job("job-bin")
    assert binary_job is not None
    assert binary_job.input_path is not None
    Path(binary_job.input_path).write_bytes(bytes(range(256)))

    run_report_job("job-bin")

    job = repo.get_job("job-bin")
    assert job is not None
    assert job.status == JobStatus.failed
    assert job.error_code == "unsupported_encoding"


def test_run_report_job_empty_file_line_count_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-empty")
    empty_job = repo.get_job("job-empty")
    assert empty_job is not None
    assert empty_job.input_path is not None
    Path(empty_job.input_path).write_bytes(b"")

    run_report_job("job-empty")

    job = repo.get_job("job-empty")
    assert job is not None
    assert job.status == JobStatus.done
    assert job.line_count == 0
    assert job.unique_lemma_count == 0


def test_run_report_job_fails_when_input_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-no-input")
    # Directory exists but input file was never written.

    run_report_job("job-no-input")

    job = repo.get_job("job-no-input")
    assert job is not None
    assert job.status == JobStatus.failed
    assert job.error_code == "artifact_missing"


def test_run_report_job_fails_xlsx_line_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-long")
    long_job = repo.get_job("job-long")
    assert long_job is not None
    assert long_job.input_path is not None
    # 16385 newline-terminated lines -> exceeds MVP xlsx third-column limit.
    Path(long_job.input_path).write_bytes(b"\n" * 16385)

    run_report_job("job-long")

    job = repo.get_job("job-long")
    assert job is not None
    assert job.status == JobStatus.failed
    assert job.error_code == "xlsx_cell_limit"


def test_run_report_job_fails_when_output_write_raises_os_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-os")
    os_job = repo.get_job("job-os")
    assert os_job is not None
    assert os_job.input_path is not None
    Path(os_job.input_path).write_bytes(b"ok")

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("app.workers.report_process_job._write_minimal_xlsx", _boom)

    run_report_job("job-os")

    job = repo.get_job("job-os")
    assert job is not None
    assert job.status == JobStatus.failed
    assert job.error_code == "artifact_missing"
    assert job.error_message is not None
    assert "disk full" in job.error_message


def test_run_report_job_fails_when_stats_collection_raises_sqlite_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-stats-sqlite-error")
    created = repo.get_job("job-stats-sqlite-error")
    assert created is not None
    assert created.input_path is not None
    Path(created.input_path).write_bytes(b"ok")

    def _boom(*_args: object, **_kwargs: object) -> tuple[int, int]:
        raise sqlite3.OperationalError("database disk image is malformed")

    monkeypatch.setattr("app.workers.report_process_job._collect_stats", _boom)

    run_report_job("job-stats-sqlite-error")

    job = repo.get_job("job-stats-sqlite-error")
    assert job is not None
    assert job.status == JobStatus.failed
    assert job.error_code == "artifact_missing"
    assert job.error_message is not None
    assert "could not collect report stats" in job.error_message
    assert "database disk image is malformed" in job.error_message


def test_run_report_job_fails_when_no_codec_accepts_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With only UTF-8 allowed, arbitrary bytes may be undecodable end-to-end."""

    class _Utf8OnlySettings:
        supported_encodings = ["utf-8"]
        read_chunk_size = 1024
        normalizer_cache_size = 16
        stats_batch_size = 10
        processing_timeout_seconds = 60
        xlsx_max_data_rows = 100

    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    monkeypatch.setattr(
        "app.workers.report_process_job.get_settings",
        lambda: _Utf8OnlySettings(),
    )
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-bad")
    bad_job = repo.get_job("job-bad")
    assert bad_job is not None
    assert bad_job.input_path is not None
    Path(bad_job.input_path).write_bytes(b"\x80")

    run_report_job("job-bad")

    job = repo.get_job("job-bad")
    assert job is not None
    assert job.status == JobStatus.failed
    assert job.error_code == "unsupported_encoding"


def test_run_report_job_fails_on_invalid_read_chunk_size_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    class _InvalidChunkSizeSettings:
        supported_encodings = ["utf-8"]
        read_chunk_size = 0
        normalizer_cache_size = 16
        stats_batch_size = 10
        processing_timeout_seconds = 60
        xlsx_max_data_rows = 100

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-bad-chunk-size")
    created = repo.get_job("job-bad-chunk-size")
    assert created is not None
    assert created.input_path is not None
    Path(created.input_path).write_bytes(b"alpha\n")

    monkeypatch.setattr(
        "app.workers.report_process_job.get_settings",
        lambda: _InvalidChunkSizeSettings(),
    )

    run_report_job("job-bad-chunk-size")

    job = repo.get_job("job-bad-chunk-size")
    assert job is not None
    assert job.status == JobStatus.failed
    assert job.error_code == "artifact_missing"
    assert job.error_message is not None
    assert "invalid worker configuration" in job.error_message
    assert "read_chunk_size" in job.error_message


def test_run_report_job_fails_on_invalid_stats_batch_size_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    class _InvalidBatchSizeSettings:
        supported_encodings = ["utf-8"]
        read_chunk_size = 1024
        normalizer_cache_size = 16
        stats_batch_size = 0
        processing_timeout_seconds = 60
        xlsx_max_data_rows = 100

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-bad-batch-size")
    created = repo.get_job("job-bad-batch-size")
    assert created is not None
    assert created.input_path is not None
    Path(created.input_path).write_bytes(b"alpha\n")

    monkeypatch.setattr(
        "app.workers.report_process_job.get_settings",
        lambda: _InvalidBatchSizeSettings(),
    )

    run_report_job("job-bad-batch-size")

    job = repo.get_job("job-bad-batch-size")
    assert job is not None
    assert job.status == JobStatus.failed
    assert job.error_code == "artifact_missing"
    assert job.error_message is not None
    assert "invalid worker configuration" in job.error_message
    assert "stats_batch_size" in job.error_message


def test_run_report_job_fails_on_invalid_processing_timeout_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    class _InvalidTimeoutSettings:
        supported_encodings = ["utf-8"]
        read_chunk_size = 1024
        normalizer_cache_size = 16
        stats_batch_size = 10
        processing_timeout_seconds = 0
        xlsx_max_data_rows = 100

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-bad-timeout")
    created = repo.get_job("job-bad-timeout")
    assert created is not None
    assert created.input_path is not None
    Path(created.input_path).write_bytes(b"alpha\n")

    monkeypatch.setattr(
        "app.workers.report_process_job.get_settings",
        lambda: _InvalidTimeoutSettings(),
    )

    run_report_job("job-bad-timeout")

    job = repo.get_job("job-bad-timeout")
    assert job is not None
    assert job.status == JobStatus.failed
    assert job.error_code == "artifact_missing"
    assert job.error_message is not None
    assert "invalid worker configuration" in job.error_message
    assert "processing_timeout_seconds" in job.error_message


def test_run_report_job_fails_on_invalid_xlsx_max_data_rows_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    class _InvalidXlsxRowsSettings:
        supported_encodings = ["utf-8"]
        read_chunk_size = 1024
        normalizer_cache_size = 16
        stats_batch_size = 10
        processing_timeout_seconds = 60
        xlsx_max_data_rows = 0

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-bad-xlsx-rows")
    created = repo.get_job("job-bad-xlsx-rows")
    assert created is not None
    assert created.input_path is not None
    Path(created.input_path).write_bytes(b"alpha\n")

    monkeypatch.setattr(
        "app.workers.report_process_job.get_settings",
        lambda: _InvalidXlsxRowsSettings(),
    )

    run_report_job("job-bad-xlsx-rows")

    job = repo.get_job("job-bad-xlsx-rows")
    assert job is not None
    assert job.status == JobStatus.failed
    assert job.error_code == "artifact_missing"
    assert job.error_message is not None
    assert "invalid worker configuration" in job.error_message
    assert "xlsx_max_data_rows" in job.error_message


def test_run_report_job_fails_when_job_has_no_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(timezone.utc)
    repo = MagicMock()
    repo.claim_queued_job.return_value = Job(
        job_id="orphan",
        status=JobStatus.processing,
        created_at=now,
        updated_at=now,
        input_path=None,
        stats_path=None,
        output_path=None,
    )
    monkeypatch.setattr("app.workers.report_process_job.get_job_repository", lambda: repo)

    run_report_job("orphan")

    repo.mark_job_failed.assert_called_once_with(
        "orphan",
        error_code="artifact_missing",
        error_message="job is missing input, stats, or output path",
    )


def test_try_collect_stats_returns_none_on_incomplete_utf8_at_eof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    normalizer = LemmaNormalizer(cache_size=16)
    path = tmp_path / "bad_end"
    path.write_bytes(b"\xc3")
    assert (
        _try_collect_stats(
            path,
            encoding="utf-8",
            chunk_size=99,
            normalizer=normalizer,
            batch_size=10,
            max_data_rows=100,
            deadline=float("inf"),
            stats_path=tmp_path / "stats.sqlite",
        )
        is None
    )


def test_process_report_job_task_runs_sync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-task")
    tjob = repo.get_job("job-task")
    assert tjob is not None
    assert tjob.input_path is not None
    Path(tjob.input_path).write_bytes(b"z")

    from app.workers.report_process_job import process_report_job

    process_report_job.run("job-task")

    job = repo.get_job("job-task")
    assert job is not None
    assert job.status == JobStatus.done


def test_run_report_job_fails_on_null_byte(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-null")
    null_job = repo.get_job("job-null")
    assert null_job is not None
    assert null_job.input_path is not None
    Path(null_job.input_path).write_bytes(b"ab\x00c\n")

    run_report_job("job-null")

    job = repo.get_job("job-null")
    assert job is not None
    assert job.status == JobStatus.failed
    assert job.error_code == "unsupported_encoding"


def test_try_collect_stats_handles_crlf_split_across_chunks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    normalizer = LemmaNormalizer(cache_size=16)
    path = tmp_path / "crlf.txt"
    path.write_bytes(b"one\r\ntwo")

    assert _try_collect_stats(
        path,
        encoding="utf-8",
        chunk_size=4,
        normalizer=normalizer,
        batch_size=10,
        max_data_rows=100,
        deadline=float("inf"),
        stats_path=tmp_path / "stats.sqlite",
    ) == (2, 2)


def test_run_report_job_persists_streaming_stats_across_lines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    monkeypatch.setenv("REPORT_EXPORT_READ_CHUNK_SIZE", "3")
    monkeypatch.setenv("REPORT_EXPORT_STATS_BATCH_SIZE", "2")
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-stats")
    created = repo.get_job("job-stats")
    assert created is not None
    assert created.input_path is not None
    assert created.stats_path is not None
    Path(created.input_path).write_bytes("Кошки, кошку\nRUNNING running".encode("utf-8"))

    run_report_job("job-stats")

    job = repo.get_job("job-stats")
    assert job is not None
    assert job.status == JobStatus.done
    assert job.line_count == 2
    assert job.unique_lemma_count == 2

    stats = SqliteReportStatsStorage(created.stats_path)
    assert stats.fetch_lemma_totals() == [("running", 2), ("кошка", 2)]
    assert stats.fetch_line_counts() == [("running", 2, 2), ("кошка", 1, 2)]


def test_collect_stats_counts_last_line_without_trailing_newline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    monkeypatch.setenv("REPORT_EXPORT_READ_CHUNK_SIZE", "1")
    from app.core.settings import get_settings

    get_settings.cache_clear()
    path = tmp_path / "tail.txt"
    path.write_bytes(b"x")

    assert _collect_stats(path, tmp_path / "stats.sqlite") == (1, 1)


def test_collect_stats_counts_punctuation_only_last_line(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    monkeypatch.setenv("REPORT_EXPORT_READ_CHUNK_SIZE", "2")
    from app.core.settings import get_settings

    get_settings.cache_clear()
    path = tmp_path / "punctuation.txt"
    path.write_bytes(b"  !!!")

    assert _collect_stats(path, tmp_path / "stats.sqlite") == (1, 0)


def test_run_report_job_fails_when_unique_lemma_count_exceeds_xlsx_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    monkeypatch.setenv("REPORT_EXPORT_XLSX_MAX_DATA_ROWS", "1")
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-many-lemmas")
    created = repo.get_job("job-many-lemmas")
    assert created is not None
    assert created.input_path is not None
    Path(created.input_path).write_bytes("alpha beta".encode("utf-8"))

    run_report_job("job-many-lemmas")

    job = repo.get_job("job-many-lemmas")
    assert job is not None
    assert job.status == JobStatus.failed
    assert job.error_code == "xlsx_row_limit"


def test_run_report_job_fails_when_processing_timeout_is_exceeded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    monkeypatch.setenv("REPORT_EXPORT_PROCESSING_TIMEOUT_SECONDS", "1")
    from app.core.settings import get_settings

    get_settings.cache_clear()
    get_job_repository.cache_clear()

    repo = get_job_repository()
    assert isinstance(repo, SqliteJobRepository)
    repo.create_queued_job("job-timeout")
    created = repo.get_job("job-timeout")
    assert created is not None
    assert created.input_path is not None
    Path(created.input_path).write_bytes("alpha".encode("utf-8"))

    monotonic_values = iter([100.0, 102.0])
    monkeypatch.setattr(
        "app.workers.report_process_job.time.monotonic",
        lambda: next(monotonic_values),
    )

    run_report_job("job-timeout")

    job = repo.get_job("job-timeout")
    assert job is not None
    assert job.status == JobStatus.failed
    assert job.error_code == "processing_timeout"
