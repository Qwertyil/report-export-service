from __future__ import annotations

from pathlib import Path

from app.infrastructure.report_stats_storage import SqliteReportStatsStorage


def test_stats_storage_upserts_counts_and_tracks_new_lemmas(tmp_path: Path) -> None:
    storage = SqliteReportStatsStorage(tmp_path / "stats.sqlite", reset=True)

    first_inserted = storage.upsert_counts(
        lemma_totals={"alpha": 2, "beta": 1},
        line_counts={("alpha", 1): 2, ("beta", 3): 1},
    )
    second_inserted = storage.upsert_counts(
        lemma_totals={"alpha": 4, "gamma": 1},
        line_counts={("alpha", 1): 1, ("alpha", 2): 3, ("gamma", 2): 1},
    )

    assert first_inserted == 2
    assert second_inserted == 1
    assert storage.fetch_lemma_totals() == [("alpha", 6), ("beta", 1), ("gamma", 1)]
    assert storage.fetch_line_counts() == [
        ("alpha", 1, 3),
        ("alpha", 2, 3),
        ("beta", 3, 1),
        ("gamma", 2, 1),
    ]


def test_stats_storage_reset_recreates_empty_database(tmp_path: Path) -> None:
    database_path = tmp_path / "stats.sqlite"

    storage = SqliteReportStatsStorage(database_path, reset=True)
    storage.upsert_counts(
        lemma_totals={"alpha": 1},
        line_counts={("alpha", 1): 1},
    )

    reset_storage = SqliteReportStatsStorage(database_path, reset=True)

    assert reset_storage.fetch_lemma_totals() == []
    assert reset_storage.fetch_line_counts() == []
