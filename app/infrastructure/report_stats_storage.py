from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

SQLITE_BUSY_TIMEOUT_MS = 5_000


class SqliteReportStatsStorage:
    """Job-local SQLite storage for streaming report aggregates."""

    def __init__(self, database_path: str | Path, *, reset: bool = False) -> None:
        self._database_path = Path(database_path)
        self._database_path.parent.mkdir(parents=True, exist_ok=True)

        if reset:
            self.delete_file()

        self._initialize_database()

    @property
    def database_path(self) -> Path:
        return self._database_path

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._database_path,
            timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        return connection

    def _initialize_database(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS lemma_totals (
                    lemma TEXT PRIMARY KEY,
                    total_count INTEGER NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS line_counts (
                    lemma TEXT NOT NULL,
                    line_no INTEGER NOT NULL,
                    count INTEGER NOT NULL,
                    PRIMARY KEY (lemma, line_no)
                )
                """
            )

    def upsert_counts(
        self,
        *,
        lemma_totals: dict[str, int],
        line_counts: dict[tuple[str, int], int],
    ) -> int:
        if not lemma_totals and not line_counts:
            return 0

        new_lemma_count = 0

        with self._connect() as connection:
            if lemma_totals:
                known_lemmas = self._fetch_existing_lemmas(connection, tuple(lemma_totals))
                new_lemma_count = sum(1 for lemma in lemma_totals if lemma not in known_lemmas)

                connection.executemany(
                    """
                    INSERT INTO lemma_totals (lemma, total_count)
                    VALUES (?, ?)
                    ON CONFLICT(lemma) DO UPDATE
                    SET total_count = lemma_totals.total_count + excluded.total_count
                    """,
                    tuple(lemma_totals.items()),
                )

            if line_counts:
                connection.executemany(
                    """
                    INSERT INTO line_counts (lemma, line_no, count)
                    VALUES (?, ?, ?)
                    ON CONFLICT(lemma, line_no) DO UPDATE
                    SET count = line_counts.count + excluded.count
                    """,
                    ((lemma, line_no, count) for (lemma, line_no), count in line_counts.items()),
                )

        return new_lemma_count

    def fetch_lemma_totals(self) -> list[tuple[str, int]]:
        return list(self.iter_lemma_totals())

    def fetch_line_counts(self) -> list[tuple[str, int, int]]:
        return list(self.iter_line_counts())

    def iter_lemma_totals(self, *, batch_size: int = 1_000) -> Iterator[tuple[str, int]]:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")

        with self._connect() as connection:
            cursor = connection.execute(
                """
                SELECT lemma, total_count
                FROM lemma_totals
                ORDER BY lemma ASC
                """
            )
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    yield row["lemma"], row["total_count"]

    def iter_line_counts(self, *, batch_size: int = 1_000) -> Iterator[tuple[str, int, int]]:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")

        with self._connect() as connection:
            cursor = connection.execute(
                """
                SELECT lemma, line_no, count
                FROM line_counts
                ORDER BY lemma ASC, line_no ASC
                """
            )
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    yield row["lemma"], row["line_no"], row["count"]

    def delete_file(self) -> None:
        for path in (
            self._database_path,
            self._database_path.with_name(f"{self._database_path.name}-shm"),
            self._database_path.with_name(f"{self._database_path.name}-wal"),
        ):
            if path.exists():
                path.unlink()

    def _fetch_existing_lemmas(
        self,
        connection: sqlite3.Connection,
        lemmas: tuple[str, ...],
    ) -> set[str]:
        if not lemmas:
            return set()

        placeholders = ", ".join("?" for _ in lemmas)
        rows = connection.execute(
            f"""
            SELECT lemma
            FROM lemma_totals
            WHERE lemma IN ({placeholders})
            """,
            lemmas,
        ).fetchall()
        return {row["lemma"] for row in rows}
