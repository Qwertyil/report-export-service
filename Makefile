.PHONY: ruff mypy test check run run-api run-worker run-redis stop-redis

ruff:
	poetry run ruff check .

mypy:
	poetry run mypy app tests

test:
	poetry run pytest --cov=app --cov-report=term-missing --cov-fail-under=90

check: ruff mypy test
