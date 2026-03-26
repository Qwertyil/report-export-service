from __future__ import annotations

import codecs
import sqlite3
import time
from pathlib import Path

from openpyxl import Workbook  # type: ignore[import-untyped]
from pydantic import ValidationError

from app.core.settings import get_settings
from app.domain.report.normalizer import LemmaNormalizer
from app.domain.report.tokenizer import LineCompletedEvent, TextTokenizer, TokenCompletedEvent
from app.infrastructure.celery_app import REPORT_EXPORT_TASK_NAME, celery_app
from app.infrastructure.job_repository import get_job_repository
from app.infrastructure.report_stats_storage import SqliteReportStatsStorage

# Step 5 fail-fast: third column cannot fit line_count commas for larger values.
_MVP_MAX_LINE_COUNT_FOR_XLSX = 16_384


class _NullByteInTextError(Exception):
    """Decoded stream contained U+0000."""


class _UnsupportedEncodingError(Exception):
    """Input bytes could not be decoded with the supported encoding allowlist."""


class _ProcessingTimeoutError(Exception):
    """Processing exceeded the configured wall-clock deadline."""


class _XlsxCellLimitError(Exception):
    """Input cannot fit into a single XLSX cell in the MVP output format."""


class _XlsxRowLimitError(Exception):
    """Input cannot fit into the single-sheet MVP output format."""


class _WorkerConfigurationError(Exception):
    """Worker runtime settings are invalid for report processing."""


class _AggregationBatch:
    def __init__(self, batch_size: int) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")

        self._batch_size = batch_size
        self._token_count = 0
        self._lemma_totals: dict[str, int] = {}
        self._line_counts: dict[tuple[str, int], int] = {}

    def add(self, lemma: str, line_no: int) -> bool:
        self._token_count += 1
        self._lemma_totals[lemma] = self._lemma_totals.get(lemma, 0) + 1

        key = (lemma, line_no)
        self._line_counts[key] = self._line_counts.get(key, 0) + 1

        return self._token_count >= self._batch_size

    def flush(self, storage: SqliteReportStatsStorage) -> int:
        new_lemma_count = storage.upsert_counts(
            lemma_totals=self._lemma_totals,
            line_counts=self._line_counts,
        )
        self._token_count = 0
        self._lemma_totals = {}
        self._line_counts = {}
        return new_lemma_count


def _check_processing_deadline(deadline: float) -> None:
    if time.monotonic() > deadline:
        raise _ProcessingTimeoutError


def _require_positive_worker_setting(name: str, value: int) -> None:
    if value < 1:
        raise _WorkerConfigurationError(f"invalid worker configuration: {name} must be positive")


def _update_line_count(current_line_count: int) -> int:
    line_count = current_line_count + 1
    if line_count > _MVP_MAX_LINE_COUNT_FOR_XLSX:
        raise _XlsxCellLimitError("line_count exceeds MVP limit for xlsx third column")
    return line_count


def _process_events(
    events: list[TokenCompletedEvent | LineCompletedEvent],
    *,
    normalizer: LemmaNormalizer,
    batch: _AggregationBatch,
    storage: SqliteReportStatsStorage,
    line_count: int,
    current_line_no: int,
    unique_lemma_count: int,
    max_data_rows: int,
    deadline: float,
) -> tuple[int, int, int]:
    for event in events:
        _check_processing_deadline(deadline)

        if isinstance(event, TokenCompletedEvent):
            lemma = normalizer.normalize(event.token)
            if batch.add(lemma, current_line_no):
                unique_lemma_count += batch.flush(storage)
                if unique_lemma_count > max_data_rows:
                    raise _XlsxRowLimitError("unique lemma count exceeds xlsx data row limit")
            continue

        line_count = _update_line_count(line_count)
        current_line_no = line_count + 1

    return line_count, current_line_no, unique_lemma_count


def _try_collect_stats(
    path: Path,
    *,
    encoding: str,
    chunk_size: int,
    normalizer: LemmaNormalizer,
    batch_size: int,
    max_data_rows: int,
    deadline: float,
    stats_path: Path,
) -> tuple[int, int] | None:
    decoder_factory = codecs.getincrementaldecoder(encoding)
    decoder = decoder_factory()
    tokenizer = TextTokenizer()
    storage = SqliteReportStatsStorage(stats_path, reset=True)
    batch = _AggregationBatch(batch_size)
    line_count = 0
    current_line_no = 1
    unique_lemma_count = 0

    try:
        with path.open("rb") as handle:
            while True:
                _check_processing_deadline(deadline)
                chunk = handle.read(chunk_size)
                if not chunk:
                    break
                try:
                    text = decoder.decode(chunk, final=False)
                except UnicodeDecodeError:
                    storage.delete_file()
                    return None
                if "\x00" in text:
                    raise _NullByteInTextError

                line_count, current_line_no, unique_lemma_count = _process_events(
                    tokenizer.feed(text),
                    normalizer=normalizer,
                    batch=batch,
                    storage=storage,
                    line_count=line_count,
                    current_line_no=current_line_no,
                    unique_lemma_count=unique_lemma_count,
                    max_data_rows=max_data_rows,
                    deadline=deadline,
                )

            try:
                tail = decoder.decode(b"", final=True)
            except UnicodeDecodeError:
                storage.delete_file()
                return None
            if "\x00" in tail:
                raise _NullByteInTextError

            line_count, current_line_no, unique_lemma_count = _process_events(
                tokenizer.feed(tail),
                normalizer=normalizer,
                batch=batch,
                storage=storage,
                line_count=line_count,
                current_line_no=current_line_no,
                unique_lemma_count=unique_lemma_count,
                max_data_rows=max_data_rows,
                deadline=deadline,
            )

        line_count, current_line_no, unique_lemma_count = _process_events(
            tokenizer.finish(),
            normalizer=normalizer,
            batch=batch,
            storage=storage,
            line_count=line_count,
            current_line_no=current_line_no,
            unique_lemma_count=unique_lemma_count,
            max_data_rows=max_data_rows,
            deadline=deadline,
        )

        unique_lemma_count += batch.flush(storage)
        if unique_lemma_count > max_data_rows:
            storage.delete_file()
            raise _XlsxRowLimitError("unique lemma count exceeds xlsx data row limit")
    except (_NullByteInTextError, _ProcessingTimeoutError, _XlsxCellLimitError, _XlsxRowLimitError):
        storage.delete_file()
        raise

    return line_count, unique_lemma_count


def _collect_stats(input_path: Path, stats_path: Path) -> tuple[int, int]:
    try:
        settings = get_settings()
    except ValidationError as exc:
        raise _WorkerConfigurationError(f"invalid worker configuration: {exc}") from exc

    _require_positive_worker_setting("read_chunk_size", settings.read_chunk_size)
    _require_positive_worker_setting("normalizer_cache_size", settings.normalizer_cache_size)
    _require_positive_worker_setting("stats_batch_size", settings.stats_batch_size)
    _require_positive_worker_setting("processing_timeout_seconds", settings.processing_timeout_seconds)
    _require_positive_worker_setting("xlsx_max_data_rows", settings.xlsx_max_data_rows)

    deadline = time.monotonic() + settings.processing_timeout_seconds
    try:
        normalizer = LemmaNormalizer(cache_size=settings.normalizer_cache_size)
    except ValueError as exc:
        raise _WorkerConfigurationError(f"invalid worker configuration: {exc}") from exc

    for encoding in settings.supported_encodings:
        try:
            result = _try_collect_stats(
                input_path,
                encoding=encoding,
                chunk_size=settings.read_chunk_size,
                normalizer=normalizer,
                batch_size=settings.stats_batch_size,
                max_data_rows=settings.xlsx_max_data_rows,
                deadline=deadline,
                stats_path=stats_path,
            )
        except _NullByteInTextError:
            raise
        except ValueError as exc:
            raise _WorkerConfigurationError(f"invalid worker configuration: {exc}") from exc
        if result is not None:
            return result

    raise _UnsupportedEncodingError("could not decode file with supported encodings")


def _write_minimal_xlsx(output_path: Path) -> None:
    """Header-only workbook until Step 6 streaming writer reads stats sqlite."""
    part_path = output_path.with_suffix(output_path.suffix + ".part")
    workbook = Workbook(write_only=True)
    worksheet = workbook.create_sheet()
    worksheet.append(["lemma", "total_count", "counts_per_line"])
    workbook.save(str(part_path))
    part_path.replace(output_path)


def run_report_job(job_id: str) -> None:
    repo = get_job_repository()
    job = repo.claim_queued_job(job_id)
    if job is None:
        return

    input_path_str = job.input_path
    output_path_str = job.output_path
    stats_path_str = job.stats_path
    if input_path_str is None or output_path_str is None or stats_path_str is None:
        repo.mark_job_failed(
            job_id,
            error_code="artifact_missing",
            error_message="job is missing input, stats, or output path",
        )
        return

    input_path = Path(input_path_str)
    stats_path = Path(stats_path_str)
    output_path = Path(output_path_str)

    if not input_path.is_file():
        repo.mark_job_failed(
            job_id,
            error_code="artifact_missing",
            error_message="input file is missing",
        )
        return

    try:
        line_count, unique_lemma_count = _collect_stats(input_path, stats_path)
    except _NullByteInTextError:
        repo.mark_job_failed(
            job_id,
            error_code="unsupported_encoding",
            error_message="decoded text contains null byte",
        )
        return
    except _ProcessingTimeoutError:
        repo.mark_job_failed(
            job_id,
            error_code="processing_timeout",
            error_message="job exceeded processing timeout",
        )
        return
    except _XlsxCellLimitError as exc:
        repo.mark_job_failed(
            job_id,
            error_code="xlsx_cell_limit",
            error_message=str(exc),
        )
        return
    except _XlsxRowLimitError as exc:
        repo.mark_job_failed(
            job_id,
            error_code="xlsx_row_limit",
            error_message=str(exc),
        )
        return
    except (OSError, sqlite3.Error) as exc:
        repo.mark_job_failed(
            job_id,
            error_code="artifact_missing",
            error_message=f"could not collect report stats: {exc}",
        )
        return
    except _WorkerConfigurationError as exc:
        repo.mark_job_failed(
            job_id,
            error_code="artifact_missing",
            error_message=str(exc),
        )
        return
    except _UnsupportedEncodingError as exc:
        repo.mark_job_failed(
            job_id,
            error_code="unsupported_encoding",
            error_message=str(exc),
        )
        return

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        _write_minimal_xlsx(output_path)
    except OSError as exc:
        repo.mark_job_failed(
            job_id,
            error_code="artifact_missing",
            error_message=f"could not write report output: {exc}",
        )
        return

    repo.mark_job_done(
        job_id,
        line_count=line_count,
        unique_lemma_count=unique_lemma_count,
    )


@celery_app.task(name=REPORT_EXPORT_TASK_NAME)
def process_report_job(job_id: str) -> None:
    run_report_job(job_id)
