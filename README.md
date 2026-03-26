# Report Export Service

Service for asynchronous processing of large text files and exporting lemma statistics to `xlsx`.

## Quick Start

The easiest way to start the whole service:

```bash
cp .env.example .env
docker compose up --build
```

The API will be available at `http://localhost:8000`, and the OpenAPI UI at `http://localhost:8000/docs`.

For local process-based runs you will need:

- Python `3.12`
- Poetry
- Redis

Install dependencies:

```bash
poetry install
```

## MVP Contract

### `POST /public/report/export`

- accepts `multipart/form-data` with a `file` field;
- saves the upload to `shared_jobs_root/<job_id>/input` with application-level size validation;
- creates a job in the persistent `job_repository`;
- publishes a task to the queue;
- returns `202 Accepted` with `job_id`, `status`, `status_url`, `download_url`.

Possible errors:

- `400` if `file` is missing;
- `413` if the upload size exceeds `max_upload_size_bytes` (partial input is deleted);
- `503` if publishing to the queue fails (the job is moved to `failed`, input is deleted on a best-effort basis).

### `GET /public/report/{job_id}/status`

- reads status only from `job_repository`;
- returns `queued | processing | done | failed`;
- for `done`, returns `download_url`;
- for `failed`, returns `{ error_code, error_message }`;
- returns `404` if the job does not exist.

Repair-check:

- if the job metadata says `done` but the artifact is missing, the job is repair-transitioned to `failed` with `error_code = artifact_missing`.

### `GET /public/report/{job_id}/download`

- returns `200` and the `xlsx` file only for `done`;
- returns `409` for `queued`, `processing`, and `failed`;
- returns `404` for an unknown `job_id`;
- uses the same `artifact_missing` repair-check as `/status`.

## Processing Rules

### Encodings

The worker tries to decode the file strictly in this order:

1. `UTF-8-SIG`
2. `UTF-8`
3. `CP1251`

If none of the encodings work, the job finishes as `failed` with `error_code = unsupported_encoding`.
If `\x00` appears in successfully decoded text, the job also finishes as `unsupported_encoding`.

### Tokenization

- a token is a continuous sequence of letters;
- digits are not considered tokens;
- a hyphen splits tokens;
- punctuation and other noise are ignored;
- text is lowercased;
- `ё` is normalized to `е`;
- Latin letters are supported as regular tokens;
- a token longer than `max_token_length` fails the job with `xlsx_cell_limit`;
- empty lines are included in `line_count`;
- the last line is counted even without a trailing `\n`.

### Normalization

- `pymorphy3` is used for tokens containing Cyrillic;
- for tokens without Cyrillic, the lowercase token is returned;
- a bounded LRU cache is used.

## XLSX Output

A single worksheet is generated with this header row:

- `lemma`
- `total_count`
- `counts_per_line`

`counts_per_line` contains values for all lines from `1` to `line_count`, including zeros.

## MVP Limits

- maximum cell text length: `xlsx_cell_char_limit` (default: `32767`);
- maximum number of data rows: `xlsx_max_data_rows` (default: `1_048_575`);
- fail-fast on line count: if `line_count > 16_384`, the job fails with `xlsx_cell_limit`;
- if `unique_lemma_count > xlsx_max_data_rows`, the job fails with `xlsx_row_limit`.

## Source Of Truth

- public job status is always read from `job_repository`;
- `Celery`/`AsyncResult` is not used as the source of user-visible status;
- `api` and `worker` must see the same `shared_jobs_root` at the same absolute path.

## Error Codes

Fixed MVP `error_code` list:

- `queue_unavailable`
- `unsupported_encoding`
- `processing_timeout`
- `xlsx_cell_limit`
- `xlsx_row_limit`
- `artifact_missing`

## Configuration

Environment variables with the `REPORT_EXPORT_` prefix:

- `REDIS_URL` (default: `redis://localhost:6379/0`)
- `CELERY_BROKER_URL` (optional, defaults to `REDIS_URL`)
- `CELERY_RESULT_BACKEND` (optional, defaults to `REDIS_URL`)
- `SHARED_JOBS_ROOT` (default: `/tmp/report-export-shared-jobs`)
- `MAX_UPLOAD_SIZE_BYTES` (default: `50 * 1024 * 1024`)
- `READ_CHUNK_SIZE` (default: `1_048_576`)
- `NORMALIZER_CACHE_SIZE` (default: `100_000`)
- `STATS_BATCH_SIZE` (default: `10_000`)
- `MAX_TOKEN_LENGTH` (default: `100_000`)
- `PROCESSING_TIMEOUT_SECONDS` (default: `600`)
- `XLSX_CELL_CHAR_LIMIT` (default: `32767`)
- `XLSX_MAX_DATA_ROWS` (default: `1_048_575`)

Create a local env file from the template:

```bash
cp .env.example .env
```

`docker compose` will automatically load `.env` from the project root.

`.env.example` is intended for container-based runs and uses `REPORT_EXPORT_REDIS_URL=redis://redis:6379/0`.
For local host-based `uvicorn` and `celery` runs, replace that value with a reachable Redis address, for example:

```env
REPORT_EXPORT_REDIS_URL=redis://localhost:6379/0
```

## Local Run

Make sure Redis is already running and reachable via `REPORT_EXPORT_REDIS_URL`.

API:

```bash
poetry run uvicorn app.main:app --reload
```

Worker:

```bash
poetry run celery -A app.infrastructure.celery_app worker --loglevel=info
```

## Run In Containers

Prerequisites:

- Docker
- Docker Compose (v2, via `docker compose`)

Start all services (API + worker + Redis):

```bash
docker compose up --build
```

Run in background:

```bash
docker compose up -d --build
```

Stop and remove containers:

```bash
docker compose down
```

API will be available at:

- `http://localhost:8000`

Useful Make targets:

```bash
make docker-build
make docker-up
make docker-logs
make docker-down
```

## Example Flow

Submit file:

```bash
curl -F "file=@sample.txt" http://localhost:8000/public/report/export
```

Example response:

```json
{
  "job_id": "7b4a4cb2-7d60-4d33-b2be-cfd4d9c8af2e",
  "status": "queued",
  "status_url": "/public/report/7b4a4cb2-7d60-4d33-b2be-cfd4d9c8af2e/status",
  "download_url": "/public/report/7b4a4cb2-7d60-4d33-b2be-cfd4d9c8af2e/download"
}
```

Check status:

```bash
curl http://localhost:8000/public/report/<job_id>/status
```

Download result when status becomes `done`:

```bash
curl -OJ http://localhost:8000/public/report/<job_id>/download
```

## Tests

- unit: tokenizer, normalizer, streaming aggregation, `job_repository`, sqlite stats storage, xlsx writer;
- integration: submit/status/download contract, oversized upload (`413`), queue publish failure (`503`), full async flow, controlled processing failures, `404` unknown job, `409` download before completion.

Run tests:

```bash
poetry run pytest
```

Or via Make targets:

```bash
make test
make check
```
