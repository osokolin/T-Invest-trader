.PHONY: build up down logs restart test lint shell psql status

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

restart:
	docker compose restart app

test:
	.venv/bin/pytest

lint:
	.venv/bin/ruff check .

shell:
	docker compose exec app bash

psql:
	docker compose exec postgres psql -U tinvest -d tinvest

status:
	docker compose ps
