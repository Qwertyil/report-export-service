.PHONY: ruff mypy test check docker-build docker-up docker-down docker-logs

ruff:
	poetry run ruff check .

mypy:
	poetry run mypy app tests

test:
	poetry run pytest --cov=app --cov-report=term-missing --cov-fail-under=90

check: ruff mypy test

docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f
