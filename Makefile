.PHONY: install lint test up down skeleton

install:
	pip install -r requirements.txt
	pre-commit install

lint:
	ruff check src tests && black --check src tests

test:
	LOG_TO_FILE=0 pytest tests/unit -q --cov=src/fraud_lakehouse

up:
	bash scripts/local_up.sh

down:
	docker compose down -v

skeleton:
	python skeleton.py
