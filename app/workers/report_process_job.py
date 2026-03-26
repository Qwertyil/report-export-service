from __future__ import annotations

import codecs
from pathlib import Path

from openpyxl import Workbook  # type: ignore[import-untyped]

from app.core.settings import get_settings
from app.domain.report.tokenizer import LineCompletedEvent, TextTokenizer
from app.infrastructure.celery_app import REPORT_EXPORT_TASK_NAME, celery_app
from app.infrastructure.job_repository import get_job_repository

# Step 5 fail-fast: third column cannot fit line_count commas for larger values.
_MVP_MAX_LINE_COUNT_FOR_XLSX = 16_384


class _NullByteInTextError(Exception):
    """Decoded stream contained U+0000."""


def _try_count_lines(path: Path, encoding: str, chunk_size: int) -> int | None:
    decoder_factory = codecs.getincrementaldecoder(encoding)
    decoder = decoder_factory()
    tokenizer = TextTokenizer()
    line_count = 0

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            try:
                text = decoder.decode(chunk, final=False)
            except UnicodeDecodeError:
                return None
            if "\x00" in text:
                raise _NullByteInTextError

            for event in tokenizer.feed(text):
                if isinstance(event, LineCompletedEvent):
                    line_count += 1

        try:
            tail = decoder.decode(b"", final=True)
        except UnicodeDecodeError:
            return None
        if "\x00" in tail:
            raise _NullByteInTextError

        for event in tokenizer.feed(tail):
            if isinstance(event, LineCompletedEvent):
                line_count += 1

    for event in tokenizer.finish():
        if isinstance(event, LineCompletedEvent):
            line_count += 1

    return line_count


def _decode_and_count_lines(input_path: Path) -> int:
    settings = get_settings()
    for encoding in settings.supported_encodings:
        try:
            result = _try_count_lines(input_path, encoding, settings.read_chunk_size)
        except _NullByteInTextError:
            raise
        if result is not None:
            return result

    raise ValueError("could not decode file with supported encodings")


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
    if input_path_str is None or output_path_str is None:
        repo.mark_job_failed(
            job_id,
            error_code="artifact_missing",
            error_message="job is missing input or output path",
        )
        return

    input_path = Path(input_path_str)
    output_path = Path(output_path_str)

    if not input_path.is_file():
        repo.mark_job_failed(
            job_id,
            error_code="artifact_missing",
            error_message="input file is missing",
        )
        return

    try:
        line_count = _decode_and_count_lines(input_path)
    except _NullByteInTextError:
        repo.mark_job_failed(
            job_id,
            error_code="unsupported_encoding",
            error_message="decoded text contains null byte",
        )
        return
    except ValueError as exc:
        repo.mark_job_failed(
            job_id,
            error_code="unsupported_encoding",
            error_message=str(exc),
        )
        return

    if line_count > _MVP_MAX_LINE_COUNT_FOR_XLSX:
        repo.mark_job_failed(
            job_id,
            error_code="xlsx_row_limit",
            error_message="line_count exceeds MVP limit for xlsx third column",
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

    repo.mark_job_done(job_id, line_count=line_count, unique_lemma_count=0)


@celery_app.task(name=REPORT_EXPORT_TASK_NAME)
def process_report_job(job_id: str) -> None:
    run_report_job(job_id)
