# Report Export Service

Сервис для асинхронной обработки больших текстовых файлов и экспорта статистики по леммам в `xlsx`.

## MVP Contract

### `POST /public/report/export`

- принимает `multipart/form-data` с полем `file`;
- сохраняет upload в `shared_jobs_root/<job_id>/input` с прикладной проверкой размера;
- создает job в persistent `job_repository`;
- публикует задачу в очередь;
- возвращает `202 Accepted` с `job_id`, `status`, `status_url`, `download_url`.

Возможные ошибки:

- `400` если `file` не передан;
- `413` если размер upload превышает `max_upload_size_bytes` (partial input удаляется);
- `503` если публикация в очередь не удалась (job переводится в `failed`, input удаляется best-effort).

### `GET /public/report/{job_id}/status`

- читает состояние только из `job_repository`;
- возвращает `queued | processing | done | failed`;
- для `done` возвращает `download_url`;
- для `failed` возвращает `{ error_code, error_message }`;
- возвращает `404`, если job не существует.

Repair-check:

- если в metadata job указано `done`, но артефакт отсутствует, job ремонтно переводится в `failed` с `error_code = artifact_missing`.

### `GET /public/report/{job_id}/download`

- возвращает `200` и `xlsx` только для `done`;
- возвращает `409` для `queued`, `processing` и `failed`;
- возвращает `404` для неизвестного `job_id`;
- использует тот же repair-check `artifact_missing`, что и `/status`.

## Processing Rules

### Encodings

Worker пробует декодировать файл строго в порядке:

1. `UTF-8-SIG`
2. `UTF-8`
3. `CP1251`

Если ни одна кодировка не подходит, job завершается как `failed` с `error_code = unsupported_encoding`.
Если в успешно декодированном тексте встречается `\x00`, job также завершается как `unsupported_encoding`.

### Tokenization

- токен = непрерывная последовательность букв;
- цифры не считаются токенами;
- дефис разбивает токены;
- пунктуация и прочий шум игнорируются;
- регистр приводится к нижнему;
- `ё` нормализуется к `е`;
- латиница поддерживается как обычные токены;
- пустые строки учитываются в `line_count`;
- последняя строка учитывается даже без завершающего `\n`.

### Normalization

- для токенов с кириллицей используется `pymorphy3`;
- для токенов без кириллицы возвращается lowercase token;
- используется bounded LRU-кэш.

## XLSX Output

Формируется один worksheet с header row:

- `lemma`
- `total_count`
- `counts_per_line`

`counts_per_line` содержит значения для всех строк от `1` до `line_count`, включая нули.

## MVP Limits

- максимальная длина текста в ячейке: `xlsx_cell_char_limit` (по умолчанию `32767`);
- максимальное число data rows: `xlsx_max_data_rows` (по умолчанию `1_048_575`);
- fail-fast по числу строк: если `line_count > 16_384`, job завершается с `xlsx_cell_limit`;
- если `unique_lemma_count > xlsx_max_data_rows`, job завершается с `xlsx_row_limit`.

## Source Of Truth

- публичный статус job всегда читается из `job_repository`;
- `Celery`/`AsyncResult` не используется как источник пользовательского статуса;
- `api` и `worker` должны видеть один и тот же `shared_jobs_root` по одному и тому же абсолютному пути.

## Error Codes

Фиксированный список MVP `error_code`:

- `queue_unavailable`
- `unsupported_encoding`
- `processing_timeout`
- `xlsx_cell_limit`
- `xlsx_row_limit`
- `artifact_missing`

## Configuration

Переменные окружения с префиксом `REPORT_EXPORT_`:

- `SHARED_JOBS_ROOT` (default: `/tmp/report-export-shared-jobs`)
- `MAX_UPLOAD_SIZE_BYTES` (default: `50 * 1024 * 1024`)
- `READ_CHUNK_SIZE` (default: `1_048_576`)
- `NORMALIZER_CACHE_SIZE` (default: `100_000`)
- `STATS_BATCH_SIZE` (default: `10_000`)
- `PROCESSING_TIMEOUT_SECONDS` (default: `600`)
- `XLSX_CELL_CHAR_LIMIT` (default: `32767`)
- `XLSX_MAX_DATA_ROWS` (default: `1_048_575`)

Создайте локальный env-файл из шаблона:

```bash
cp .env.example .env
```

`docker compose` автоматически подхватит `.env` из корня проекта.

## Local Run

```bash
uvicorn app.main:app --reload
```

Worker:

```bash
celery -A app.infrastructure.celery_app worker --loglevel=info
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

## Tests

- unit: tokenizer, normalizer, streaming aggregation, `job_repository`, sqlite stats storage, xlsx writer;
- integration: submit/status/download contract, oversized upload (`413`), queue publish failure (`503`), full async flow, controlled processing failures, `404` unknown job, `409` download before completion.
