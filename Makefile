.PHONY: install lint test itest up down skeleton produce stream

install:
	pip install -r requirements.txt
	pre-commit install

lint:
	ruff check src tests && black --check src tests

test:
	LOG_TO_FILE=0 pytest tests/unit -q --cov=src/fraud_lakehouse

itest:
	pytest tests/integration -m integration -q

produce:
	python -m fraud_lakehouse.streaming.producer

stream:
	python -m fraud_lakehouse.streaming.stream_job

up:
	bash scripts/local_up.sh

down:
	docker compose down -v

skeleton:
	python skeleton.py
