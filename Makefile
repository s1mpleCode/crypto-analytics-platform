.PHONY: up down restart logs ps lint test

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f

ps:
	docker compose ps

lint:
	pre-commit run --all-files

test:
	uv run pytest
