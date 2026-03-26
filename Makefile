.PHONY: ruff mypy test check

ruff:
	poetry run ruff check .

mypy:
	poetry run mypy app tests

test:
	poetry run pytest --cov=app --cov-report=term-missing --cov-fail-under=95

check: ruff mypy test
