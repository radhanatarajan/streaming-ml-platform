.PHONY: up down ps lint test

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

lint:
	uv run ruff check .

test:
	uv run pytest
