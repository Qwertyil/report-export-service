"""MVP constants shared across API and application layers."""

# Fixed order MVP allowlist for decoding.
MVP_SUPPORTED_ENCODINGS: list[str] = ["UTF-8-SIG", "UTF-8", "CP1251"]

# Fixed list of MVP error codes (used both in code and schemas).
MVP_ERROR_CODES: list[str] = [
    "queue_unavailable",
    "unsupported_encoding",
    "processing_timeout",
    "xlsx_cell_limit",
    "xlsx_row_limit",
    "artifact_missing",
]

