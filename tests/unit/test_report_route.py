from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from fastapi import HTTPException, status

from app.api.routes import report as report_route


class _StubUpload:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = iter(chunks)

    async def read(self, chunk_size: int) -> bytes:
        return next(self._chunks)


def test_persist_upload_offloads_file_io(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[str] = []

    async def fake_run_in_threadpool(func, *args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(func.__name__)
        return func(*args, **kwargs)

    monkeypatch.setattr(report_route, "run_in_threadpool", fake_run_in_threadpool)

    destination = tmp_path / "upload.bin"
    upload = _StubUpload([b"ab", b"cd", b""])

    asyncio.run(
        report_route._persist_upload(
            upload=upload,  # type: ignore[arg-type]
            destination=destination,
            max_size_bytes=8,
            chunk_size=2,
        )
    )

    assert destination.read_bytes() == b"abcd"
    assert calls == ["open", "write", "write", "close"]


def test_persist_upload_rejects_over_limit_and_unlinks_partial(tmp_path: Path) -> None:
    upload = _StubUpload([b"hi", b"there", b""])
    destination = tmp_path / "out"

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            report_route._persist_upload(
                upload=upload,  # type: ignore[arg-type]
                destination=destination,
                max_size_bytes=4,
                chunk_size=10,
            )
        )

    assert excinfo.value.status_code == status.HTTP_413_CONTENT_TOO_LARGE
    assert not destination.exists()


def test_best_effort_unlink_swallows_oserror(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "f"

    def boom(self: Path, *args, **kwargs):  # type: ignore[no-untyped-def]
        raise OSError("refused")

    monkeypatch.setattr(Path, "unlink", boom)
    report_route._best_effort_unlink(path)


def test_best_effort_unlink_async_swallows_oserror(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "f"

    def boom(self: Path, *args, **kwargs):  # type: ignore[no-untyped-def]
        raise OSError("refused")

    monkeypatch.setattr(Path, "unlink", boom)
    asyncio.run(report_route._best_effort_unlink_async(path))
