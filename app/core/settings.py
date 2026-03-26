from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.domain.report.constants import MVP_SUPPORTED_ENCODINGS


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="REPORT_EXPORT_",
        env_file=".env",
        env_file_encoding="utf-8",
    )

    app_name: str = "Report Export Service"
    debug: bool = False
    api_prefix: str = "/public"

    redis_url: str = Field(default="redis://localhost:6379/0")
    celery_broker_url: str | None = None
    celery_result_backend: str | None = None

    shared_jobs_root: str = "/tmp/report-export-shared-jobs"
    max_upload_size_bytes: int = 50 * 1024 * 1024

    # Fixed order MVP decoding allowlist: UTF-8-SIG -> UTF-8 -> CP1251.
    supported_encodings: list[str] = Field(default_factory=lambda: MVP_SUPPORTED_ENCODINGS)

    read_chunk_size: int = 1024 * 1024
    normalizer_cache_size: int = 100_000
    stats_batch_size: int = 10_000
    processing_timeout_seconds: int = 600

    xlsx_cell_char_limit: int = 32767
    xlsx_max_data_rows: int = 1_048_575

    @field_validator("shared_jobs_root")
    @classmethod
    def validate_shared_jobs_root(cls, value: str) -> str:
        path = Path(value)
        if not path.is_absolute():
            raise ValueError("shared_jobs_root must be an absolute path")
        return str(path)

    @field_validator(
        "read_chunk_size",
        "normalizer_cache_size",
        "stats_batch_size",
        "processing_timeout_seconds",
        "xlsx_max_data_rows",
    )
    @classmethod
    def validate_positive_runtime_settings(cls, value: int, info: object) -> int:
        if value < 1:
            field_name = getattr(info, "field_name", "value")
            raise ValueError(f"{field_name} must be positive")
        return value

    @property
    def effective_celery_broker_url(self) -> str:
        return self.celery_broker_url or self.redis_url

    @property
    def effective_celery_result_backend(self) -> str:
        return self.celery_result_backend or self.redis_url


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
