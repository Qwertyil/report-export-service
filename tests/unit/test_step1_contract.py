from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from app.api.schemas.report import validate_mvp_error_code
from app.core.settings import Settings, get_settings
from app.domain.report.constants import MVP_ERROR_CODES, MVP_SUPPORTED_ENCODINGS
from app.main import create_app


def test_settings_require_absolute_shared_jobs_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", "relative/jobs")
    get_settings.cache_clear()

    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize(
    ("env_name", "env_value", "message"),
    [
        ("REPORT_EXPORT_READ_CHUNK_SIZE", "0", "read_chunk_size must be positive"),
        ("REPORT_EXPORT_NORMALIZER_CACHE_SIZE", "0", "normalizer_cache_size must be positive"),
        ("REPORT_EXPORT_STATS_BATCH_SIZE", "0", "stats_batch_size must be positive"),
        ("REPORT_EXPORT_PROCESSING_TIMEOUT_SECONDS", "0", "processing_timeout_seconds must be positive"),
        ("REPORT_EXPORT_XLSX_MAX_DATA_ROWS", "0", "xlsx_max_data_rows must be positive"),
    ],
)
def test_settings_require_positive_runtime_settings(
    monkeypatch: pytest.MonkeyPatch,
    env_name: str,
    env_value: str,
    message: str,
) -> None:
    monkeypatch.setenv(env_name, env_value)
    get_settings.cache_clear()

    with pytest.raises(ValidationError, match=message):
        Settings()


def test_create_app_prepares_shared_jobs_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    shared_jobs_root = tmp_path / "shared-jobs-root"
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(shared_jobs_root))
    get_settings.cache_clear()

    assert not shared_jobs_root.exists()

    create_app()

    assert shared_jobs_root.is_dir()

    get_settings.cache_clear()


def test_step1_fixed_mvp_contract_constants() -> None:
    assert MVP_SUPPORTED_ENCODINGS == ["UTF-8-SIG", "UTF-8", "CP1251"]
    assert MVP_ERROR_CODES == [
        "queue_unavailable",
        "unsupported_encoding",
        "processing_timeout",
        "xlsx_cell_limit",
        "xlsx_row_limit",
        "artifact_missing",
    ]

    for error_code in MVP_ERROR_CODES:
        assert validate_mvp_error_code(error_code) == error_code

    with pytest.raises(ValueError, match="Unknown MVP error_code"):
        validate_mvp_error_code("unexpected_error")


def test_export_openapi_requires_multipart_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPORT_EXPORT_SHARED_JOBS_ROOT", str(tmp_path))
    get_settings.cache_clear()

    try:
        schema = create_app().openapi()
    finally:
        get_settings.cache_clear()

    operation = schema["paths"]["/public/report/export"]["post"]
    assert operation["requestBody"]["required"] is True

    body_ref = operation["requestBody"]["content"]["multipart/form-data"]["schema"]["$ref"]
    body_name = body_ref.rsplit("/", maxsplit=1)[-1]
    body_schema = schema["components"]["schemas"][body_name]

    assert body_schema["type"] == "object"
    assert body_schema["required"] == ["file"]
    assert body_schema["properties"]["file"] == {
        "title": "File",
        "type": "string",
        "format": "binary",
    }
