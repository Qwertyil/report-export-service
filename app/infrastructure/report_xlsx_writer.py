from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from openpyxl import Workbook  # type: ignore[import-untyped]

from app.infrastructure.report_stats_storage import SqliteReportStatsStorage


class XlsxCellLimitExceededError(Exception):
    """A generated counts-per-line cell exceeded the configured XLSX cell limit."""


def _append_cell_value(parts: list[str], value: str, *, cell_length: int, cell_char_limit: int) -> int:
    next_length = cell_length + len(value)
    if parts:
        next_length += 1
    if next_length > cell_char_limit:
        raise XlsxCellLimitExceededError("counts_per_line exceeds xlsx cell character limit")
    parts.append(value)
    return next_length


def _build_counts_per_line(
    *,
    line_count: int,
    cell_char_limit: int,
    current_entry: tuple[str, int, int] | None,
    target_lemma: str,
    line_counts_iter: Iterator[tuple[str, int, int]],
) -> tuple[str, tuple[str, int, int] | None]:
    parts: list[str] = []
    cell_length = 0
    expected_line_no = 1
    entry = current_entry

    while entry is not None and entry[0] == target_lemma:
        _, line_no, count = entry
        while expected_line_no < line_no:
            cell_length = _append_cell_value(
                parts,
                "0",
                cell_length=cell_length,
                cell_char_limit=cell_char_limit,
            )
            expected_line_no += 1

        cell_length = _append_cell_value(
            parts,
            str(count),
            cell_length=cell_length,
            cell_char_limit=cell_char_limit,
        )
        expected_line_no += 1
        entry = next(line_counts_iter, None)

    while expected_line_no <= line_count:
        cell_length = _append_cell_value(
            parts,
            "0",
            cell_length=cell_length,
            cell_char_limit=cell_char_limit,
        )
        expected_line_no += 1

    return ",".join(parts), entry


def write_report_xlsx(
    *,
    stats_path: str | Path,
    output_path: str | Path,
    line_count: int,
    cell_char_limit: int,
) -> None:
    if line_count < 0:
        raise ValueError("line_count must not be negative")
    if cell_char_limit < 1:
        raise ValueError("cell_char_limit must be positive")

    storage = SqliteReportStatsStorage(stats_path)
    final_path = Path(output_path)
    part_path = final_path.with_suffix(final_path.suffix + ".part")
    final_path.parent.mkdir(parents=True, exist_ok=True)

    workbook = Workbook(write_only=True)
    worksheet = workbook.create_sheet(title="report")
    worksheet.append(["lemma", "total_count", "counts_per_line"])
    saved = False

    line_counts_iter = iter(storage.iter_line_counts())
    current_entry = next(line_counts_iter, None)

    try:
        for lemma, total_count in storage.iter_lemma_totals():
            counts_per_line, current_entry = _build_counts_per_line(
                line_count=line_count,
                cell_char_limit=cell_char_limit,
                current_entry=current_entry,
                target_lemma=lemma,
                line_counts_iter=line_counts_iter,
            )
            worksheet.append([lemma, total_count, counts_per_line])

        workbook.save(str(part_path))
        saved = True
        part_path.replace(final_path)
    except Exception:
        try:
            worksheet.close()
        except Exception:
            pass
        part_path.unlink(missing_ok=True)
        raise
    finally:
        if saved:
            workbook.close()
