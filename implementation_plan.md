# Implementation Plan

## Цель

Довести текущий репозиторий до рабочего сервиса для тестового задания так, чтобы решение:

- принимало большие текстовые файлы через `POST /public/report/export`;
- не держало тяжелую обработку внутри HTTP-запроса;
- считало статистику по леммам по всему документу и по строкам;
- строило корректный `xlsx`-отчет;
- выглядело как аккуратная инженерная работа без лишнего scope creep.

## Текущая точка

В репозитории уже есть базовый каркас, поэтому план начинается не с полностью пустого проекта, но большая часть рабочего pipeline еще остается заглушками:

- зависимости уже добавлены в `pyproject.toml`;
- минимальный `FastAPI` app уже поднят;
- `Celery` и `Redis` уже подключены на базовом уровне;
- есть заглушки для `report`-роута, domain/storage/export модулей и базовые smoke-тесты.

Это значит, что дальше нужно реализовать не bootstrap, а рабочий pipeline:

`upload -> persistent job -> Celery task -> streaming aggregation -> xlsx -> status/download`.

## MVP

### Шаг 1. Зафиксировать MVP-контракт и выровнять конфигурацию под текущий код

Нужно реализовать и задокументировать именно тот контракт, который будет жить в коде:

- `POST /public/report/export` принимает `multipart/form-data` с полем `file`;
- `POST /public/report/export` не возвращает `xlsx` синхронно, а создает job и отвечает `202 Accepted`;
- ответ `POST /public/report/export` должен содержать `job_id`, `status`, `status_url`, `download_url`;
- `GET /public/report/{job_id}/status` нужен для polling;
- `GET /public/report/{job_id}/download` нужен для скачивания готового файла.

В этом же шаге нужно расширить `settings` только теми параметрами, без которых MVP не заработает:

- `shared_jobs_root`;
- `max_upload_size_bytes`;
- список поддерживаемых кодировок в фиксированном порядке: `UTF-8-SIG`, `UTF-8`, `CP1251`;
- `read_chunk_size`;
- `normalizer_cache_size`;
- `stats_batch_size`;
- `processing_timeout_seconds`;
- `xlsx_cell_char_limit` со значением по умолчанию `32767`;
- `xlsx_max_data_rows` со значением по умолчанию `1_048_575`.

Важно явно зафиксировать в коде и документации:

- публичным источником статуса job является только `job_repository`;
- `Celery` result backend можно временно оставить включенным для локальных smoke-тестов, но `status/download` не должны читать состояние из `AsyncResult`;
- `enqueue_report_job`, HTTP-ответы и пользовательские тесты не должны использовать payload из `AsyncResult` как источник статуса job; отдельный `Celery` smoke-тест, если остается, проверяет только факт публикации и выполнения фоновой задачи;
- `shared_jobs_root` является обязательным runtime-инвариантом: `api` и `worker` обязаны видеть один и тот же mounted volume по одному и тому же абсолютному пути;
- для production-like запуска deployment должен применять внешний request body size limit на уровне ingress / reverse proxy / ASGI-сервера не выше `max_upload_size_bytes`; для MVP в репозитории прикладная проверка при сохранении upload остается обязательной линией защиты и cleanup-механизмом для partial-файлов;
- `v1` пишет отчет в один worksheet с header row;
- `v1` поддерживает только асинхронную трактовку `/public/report/export`.
- для MVP список `error_code` фиксируется прямо в коде и тестах: `queue_unavailable`, `unsupported_encoding`, `processing_timeout`, `xlsx_cell_limit`, `xlsx_row_limit`, `artifact_missing`.
- для MVP `processing_timeout_seconds` исполняется внутри worker-пайплайна как wall-clock deadline текущей обработки job; при превышении дедлайна worker сам переводит job в `failed` с `error_code = processing_timeout`;
- для MVP `processing_timeout_seconds` не является механизмом восстановления после внезапного падения процесса; зависшие `processing` job закрываются отдельным hardening-шагом через heartbeat/reconcile.

Готово, когда:

- схемы ответов и `settings` отражают реальный MVP-контракт;
- в коде нет скрытых зависимостей от result backend как от источника истины.

### Шаг 2. Реализовать постоянную модель job и `job_repository`

Нужно сделать persistent repository, который переживает рестарт `api` и `worker`.

Для MVP достаточно хранить:

- `job_id`;
- `status`;
- `created_at`;
- `updated_at`;
- `started_at`;
- `lease_expires_at`;
- `finished_at`;
- `input_path`;
- `stats_path`;
- `output_path`;
- `line_count`;
- `unique_lemma_count`;
- `error_code`;
- `error_message`.

Для MVP достаточно поддержать статусы:

- `queued`;
- `processing`;
- `done`;
- `failed`.

Что должно быть реализовано прямо в этом шаге:

- детерминированные пути артефактов по `job_id` внутри `<shared_jobs_root>/<job_id>/`;
- создание job в статусе `queued` до публикации в очередь;
- атомарный CAS-переход `queued -> processing` с выставлением `started_at` и `lease_expires_at = now + processing_timeout_seconds`, чтобы один `job_id` не обрабатывался дважды;
- terminal-переходы `processing -> done` и `processing -> failed`;
- чтение job по `job_id` для `status` и `download`.

Если repository реализуется на `sqlite`, нужно сразу сделать минимально безопасную конфигурацию для параллельной работы:

- `WAL`;
- `busy_timeout`;
- короткие write-транзакции;
- одношаговые `UPDATE ... WHERE status = ...` для claim-переходов.

Готово, когда:

- статус job не теряется после рестарта процессов;
- повторный claim того же `job_id` выигрывает только один worker;
- API может получить состояние job без обращения к `Celery`.

### Шаг 3. Реализовать `POST /public/report/export`

Нужно заменить заглушку в роуте на реальный submit-flow:

- принять `multipart/form-data` с полем `file`;
- проверить, что файл действительно передан;
- не использовать клиентский `Content-Type` или расширение файла как источник истины о том, что файл "текстовый";
- реальную валидность текста определять позже, на этапе декодирования worker'ом;
- создать `job_id`;
- создать директорию `<shared_jobs_root>/<job_id>/`;
- сохранить upload как детерминированный `input_path`;
- при сохранении считать записанные байты и останавливать копирование при превышении `max_upload_size_bytes`;
- при превышении лимита удалить partial-файл и вернуть `413 Payload Too Large`;
- после успешного сохранения записать job в repository;
- затем опубликовать задачу в `Celery`;
- если publish в очередь не удался, перевести job в `failed` с `error_code = queue_unavailable`, удалить сохраненный input best-effort и вернуть `503 Service Unavailable`;
- возвращать `202 Accepted` только после успешной публикации.

Что сознательно не обещаем в MVP:

- строгий admission-control по диску/backlog до начала upload;
- защиту от нескольких одновременных огромных upload'ов сильнее, чем request-size limit и cleanup partial-файлов.

Готово, когда:

- тяжелая обработка не исполняется внутри HTTP-обработчика;
- oversized upload не оставляет partial input;
- сбой публикации в очередь не оставляет "висящую" `queued` job.

### Шаг 4. Реализовать токенизацию и нормализацию словоформ

Нужно реализовать два явных доменных компонента: tokenizer и normalizer.

Tokenizer должен:

- принимать поток декодированных text chunk'ов;
- сохранять хвост незавершенного токена между chunk'ами;
- корректно обрабатывать `\n` и `\r\n`;
- сообщать агрегатору о завершении токена и завершении строки;
- не требовать загрузки всей строки в память.

Чтобы поведение было предсказуемым, правила токенизации нужно зафиксировать прямо в коде и тестах:

- токеном считается непрерывная последовательность букв;
- цифры не считаются токенами;
- дефис разбивает токены;
- пунктуация и прочий шум отбрасываются;
- регистр нормализуется к нижнему;
- `ё` нормализуется к `е`;
- последовательности латинских букв поддерживаются как обычные токены.
- пустой файл дает `line_count = 0` и пустой отчет без data rows;
- последняя строка учитывается даже если файл не оканчивается `\n`;
- пустые строки не создают токенов, но учитываются в `line_count`;
- третий столбец для каждой леммы содержит counts по всем строкам от `1` до `line_count`, включая нули для строк без вхождений.

Normalizer должен:

- принимать уже токенизированное слово;
- приводить его к лемме через `pymorphy3`;
- для токенов без кириллицы не вызывать `pymorphy3`, а возвращать lowercase token как есть;
- использовать bounded LRU-кэш;
- инициализировать морфоанализатор один раз на процесс worker'а.

Готово, когда:

- слова в разных падежах схлопываются в одну лемму;
- токенизация на границах chunk'ов работает детерминированно;
- правила токенизации не спрятаны в случайных regex, а проверены unit-тестами.

### Шаг 5. Реализовать потоковую обработку файла и disk-backed агрегаты

Нужно собрать рабочий worker pipeline:

- открыть `input_path`;
- выбрать кодировку из allowlist в фиксированном порядке: `UTF-8-SIG`, `UTF-8`, `CP1251`;
- не использовать `chardet`;
- не использовать `errors="replace"`, чтобы не искажать статистику молча;
- если файл не декодируется ни одной поддерживаемой кодировкой, завершать job как `failed` с понятным `error_code`;
- для MVP не считать input валидным текстом, если при успешном декодировании в потоке встречается `\x00`; такой файл завершать как `failed` с `error_code = unsupported_encoding`;
- читать файл фиксированными byte chunk'ами, а не через `readline()`;
- через incremental decoder и stateful scanner выделять токены и границы строк;
- нормализовать токены;
- считать `line_count`;
- считать `unique_lemma_count`;
- писать агрегаты не в большие Python-словари, а в job-local `sqlite`.

Для MVP структура stats storage должна быть реализована сразу:

- `lemma_totals(lemma TEXT PRIMARY KEY, total_count INTEGER NOT NULL)`;
- `line_counts(lemma TEXT NOT NULL, line_no INTEGER NOT NULL, count INTEGER NOT NULL, PRIMARY KEY (lemma, line_no))`.

Писать в `sqlite` нужно не по одному токену, а micro-batch'ами:

- внутри bounded batch схлопывать повторы `(lemma, line_no)`;
- затем делать batched `UPSERT` в `lemma_totals` и `line_counts`.

В этом же шаге нужно реализовать fail-fast ограничения формата:

- если `line_count > 16384`, завершать job как `failed`, потому что даже строка `"0,0,0,..."` уже не поместится в ячейку `xlsx`;
- если `unique_lemma_count > 1_048_575`, завершать job как `failed`, потому что в `v1` поддерживается только один worksheet с header row.

Готово, когда:

- worker не читает файл целиком в память;
- RAM зависит от размера chunk'а, scanner tail, LRU-кэша и размера batch, а не от всего документа;
- промежуточная статистика собирается полностью из job-local `sqlite`.

### Шаг 6. Реализовать `xlsx` writer

Нужно сделать writer, который:

- пишет `xlsx` через `openpyxl`;
- работает в `write_only` режиме;
- создает один worksheet с header row;
- пишет три столбца: лемма, total count, counts per line;
- читает данные из `sqlite`, а не из гигантских in-memory структур;
- читает `line_counts` streaming-обходом через упорядоченный курсор/батчи с `ORDER BY lemma ASC, line_no ASC`, а не отдельными запросами на каждую лемму;
- сортирует строки детерминированно через `ORDER BY lemma ASC`.

Ограничения формата должны быть реализованы именно здесь, а не только описаны в документе:

- отдельный полный preflight-проход по `line_counts` для MVP не нужен: fail-fast по `line_count > 16384` выполняется на шаге 5;
- во время сборки каждой строки нужно считать фактическую длину третьего столбца и при превышении `xlsx_cell_char_limit` завершать job как `failed` с отдельным `error_code`;
- писать сначала в `report.xlsx.part`, а затем атомарно переименовывать в финальный `output_path`;
- переводить job в `done` только после успешного завершения writer и rename.

Готово, когда:

- файл открывается в Excel/LibreOffice;
- порядок строк не плавает между запусками;
- невозможный для формата результат отбрасывается контролируемо, а не ломается внутри `openpyxl`.

### Шаг 7. Реализовать `status` и `download` endpoint'ы

Нужно добавить два публичных endpoint'а поверх `job_repository`.

`GET /public/report/{job_id}/status` должен:

- читать job через тот же application/repository read-path, который проверяет runtime-инвариант: `status = done` допустим только при существующем `output_path`;
- если metadata говорит `done`, но `output_path` отсутствует, ремонтно переводить job в `failed` с `error_code = artifact_missing` до формирования HTTP-ответа;
- возвращать `job_id`;
- возвращать один из статусов `queued | processing | done | failed`;
- возвращать `download_url` для `done`;
- возвращать `error` для `failed`;
- возвращать `404 Not Found` для неизвестного `job_id`.

`GET /public/report/{job_id}/download` должен:

- возвращать `200 OK` и `xlsx` только для `done`;
- возвращать `409 Conflict` для `queued`, `processing` и `failed`;
- возвращать `404 Not Found` для неизвестного `job_id`;
- использовать тот же repair-check, что и `status`: если metadata говорит `done`, но `output_path` отсутствует, ремонтно переводить job в `failed` с `error_code = artifact_missing`, а клиенту возвращать контролируемую ошибку вместо пустого `200`.

Готово, когда:

- клиент может безопасно poll'ить job;
- скачивание не требует знания внутренних путей;
- API читает статус только из repository.

### Шаг 8. Покрыть MVP тестами и README

Для MVP обязательно нужно реализовать тесты, которые проверяют именно рабочие требования, а не только каркас.

Unit-тесты должны покрывать:

- tokenizer;
- normalizer;
- streaming aggregator;
- `job_repository` и atomic claim;
- `sqlite` stats storage;
- `xlsx` writer.

Integration-тесты должны покрывать:

- успешный `POST /public/report/export`;
- `413` на oversized upload с cleanup partial input;
- `503` при неуспешном publish в очередь;
- успешный сценарий `export -> processing -> done -> download`;
- decode failure для неподдерживаемой/битой кодировки;
- controlled failure при превышении лимита строки `xlsx`;
- controlled failure при превышении лимита числа data rows;
- `404` для неизвестного `job_id`;
- `409` для скачивания незавершенной job.

Тесты, которые фиксируют runtime-инварианты отдельных шагов, добавляются вместе с реализацией соответствующих шагов, а на этом этапе только доводятся до полного MVP-покрытия.

README для MVP должен явно описывать:

- что `/public/report/export` работает асинхронно;
- какие endpoint'ы есть помимо submit;
- какие кодировки поддерживаются;
- какие правила токенизации приняты;
- какие ограничения накладывает `xlsx`;
- что `job_repository`, а не `Celery`, является источником статусов.
- README для MVP должен описывать только фактически реализованный MVP-контракт; `expired`, TTL, admission control и reconcile не должны подаваться как уже готовое поведение до реализации соответствующих шагов.

Готово, когда:

- можно локально пройти путь от upload до скачивания результата;
- ревьюер видит не только архитектурный замысел, но и реально работающий контракт.

## Hardening

### Шаг 9. Добавить shared runtime topology и best-effort admission control

После готового MVP можно усиливать доступность под конкурентной нагрузкой.

Нужно реализовать:

- полноценный `docker-compose` с `api`, `worker`, `redis`;
- общий volume с одинаковым абсолютным `shared_jobs_root` для `api` и `worker`;
- best-effort guard'ы перед приемом upload:
  - минимум свободного места;
  - лимит активных job;
  - лимит backlog `queued`;
- контролируемый `503 Service Unavailable` при срабатывании guard'ов;
- опциональный `Retry-After`.

Важно честно зафиксировать ограничение:

- такие guard'ы без резервирования места или глобального upload-семафора являются best-effort и не дают абсолютной защиты от нескольких одновременных огромных upload'ов;
- если нужна строгая защита, ее нужно реализовывать отдельно через reservation/slot-based admission.

Готово, когда:

- сервис умеет быстро отказывать новой работе под перегрузкой;
- в документации не обещается то, что система реально не гарантирует.

### Шаг 10. Добавить heartbeat worker'а и reconcile зависших job

После того как базовый lifecycle заработает, можно расширить модель job.

Нужно добавить поля:

- `attempt_no`;
- `downloaded_at`;
- `expires_at`;
- `expired_at`.

Нужно добавить статус:

- `expired`.

Что должно быть реализовано:

- worker обновляет `updated_at` как heartbeat, пока job в `processing`;
- отдельная reconcile-задача ищет stale `processing` job по `updated_at`;
- такие job переводятся в terminal `failed`, а не автоматически в `queued`;
- partial `stats.sqlite`, `.part` и другие незавершенные артефакты удаляются best-effort;
- duplicate delivery того же `job_id` после неуспешного CAS остается no-op.

Готово, когда:

- зависший worker не оставляет job в вечном `processing` даже без чтения через `status/download`;
- повторная доставка не создает двойную обработку и дубли артефактов.

### Шаг 11. Добавить TTL, cleanup и `expired` tombstone

Нужно реализовать отдельную cleanup-задачу, а не синхронное удаление из HTTP-обработчиков.

Что должно быть реализовано:

- TTL считается от `finished_at` или `downloaded_at`;
- по истечении TTL удаляются тяжелые артефакты: `input`, `stats.sqlite`, `xlsx`;
- metadata job не удаляется полностью, а переводится в `expired`;
- `GET /status` для такой job возвращает `expired`;
- `GET /download` для такой job возвращает `410 Gone`;
- неизвестный `job_id` по-прежнему возвращает `404 Not Found`.

Готово, когда:

- cleanup не мешает happy-path запросам;
- истекшая job отличима от несуществующей.

### Шаг 12. Усилить конкурентную и recovery-семантику

После базовой версии и reconcile нужно закрыть оставшиеся углы конкурентной обработки.

Нужно реализовать и проверить:

- идемпотентность повторного сервисного запуска обработки того же `job_id`;
- repair-flow для случая, когда metadata говорит `done`, а артефакт отсутствует;
- дисциплину публикации результата:
  - writer завершает `.part`;
  - выполняется атомарный rename;
  - только после этого job переводится в `done`;
- cleanup partial job-local артефактов при `failed` best-effort, без разрушения диагностических metadata.

Готово, когда:

- сбои в середине обработки не оставляют ложный `done`;
- состояние артефактов и состояние job не расходятся бесконтрольно.

### Шаг 13. Настроить `sqlite` под нагрузку и прогнать throughput/load smoke

После того как архитектура уже работает end-to-end, нужно проверить, что storage не стал главным bottleneck.

Нужно реализовать:

- отдельный throughput-smoke на синтетически большом файле;
- проверку, что `sqlite` batch size подобран разумно;
- проверки на отсутствие спорадических `database is locked` при штатной параллельной нагрузке;
- smoke на нескольких конкурентных задачах;
- проверку, что API отвечает, пока worker занят.

Если именно `sqlite` становится доминирующим bottleneck, допускается упростить внутреннюю storage-стратегию, но:

- без изменения публичного HTTP-контракта;
- без отказа от persistent `job_repository`;
- с явной фиксацией trade-off в README.

Готово, когда:

- есть не только архитектурная гипотеза, но и базовое подтверждение производительности.

### Шаг 14. Финальная полировка

На последнем этапе нужно довести решение до аккуратного инженерного вида:

- прогнать `ruff`;
- прогнать `mypy`;
- прогнать `pytest`;
- проверить названия сущностей и файлов;
- убрать временные заглушки и мертвый код;
- обновить README под фактическую реализацию, а не под идеальный замысел;
- проверить, что история коммитов или хотя бы логика изменений читается последовательно.

Готово, когда:

- репозиторий выглядит как завершенная работа, а не как набор отдельных экспериментов.

## Рекомендуемый порядок коммитов

1. `implement persistent job repository and job lifecycle`
2. `add async report export endpoint and upload persistence`
3. `implement tokenizer and lemmatization rules`
4. `add streaming aggregation with sqlite-backed stats storage`
5. `implement xlsx export pipeline`
6. `add report status and download endpoints`
7. `cover mvp flow with unit and integration tests`
8. `document async contract and format limitations`
9. `add hardening for concurrency recovery and cleanup`

## Приоритеты, если времени мало

Сначала обязательно сделать:

1. persistent `job_repository`;
2. рабочий async submit через `POST /public/report/export`;
3. потоковую обработку файла;
4. лемматизацию;
5. `sqlite`-backed агрегацию;
6. корректный `xlsx` writer;
7. `status` и `download`;
8. базовые unit/integration тесты;
9. честный README.

Можно отложить в hardening:

- admission-control по диску/backlog;
- heartbeat и reconcile;
- TTL и `expired` tombstones;
- расширенные load/smoke сценарии;
- отключение `Celery` result backend, если он оставлен только ради локального smoke.

## Что особенно важно для хорошего впечатления

- Не делать синхронную обработку внутри endpoint.
- Не использовать `AsyncResult` как источник пользовательских статусов.
- Не хранить totals и line-counts целиком в памяти.
- Честно описать ограничения формата `xlsx`.
- Не полагаться на MIME клиента при проверке "это текстовый файл".
- Не обещать абсолютный admission-control там, где реализован только best-effort.
- Покрыть код тестами на реальные пограничные сценарии, а не только на happy path.
