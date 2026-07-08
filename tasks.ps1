<#
tasks.ps1 -- the Makefile, but for Windows PowerShell.

Usage (from the repo root, venv activated):
    .\tasks.ps1 install     # deps + editable install + pre-commit
    .\tasks.ps1 test        # all unit gates
    .\tasks.ps1 lint        # ruff + black check
    .\tasks.ps1 up          # local Kafka + MinIO (needs Docker Desktop)
    .\tasks.ps1 down        # stop + wipe local stack
    .\tasks.ps1 itest       # integration gates (needs `up` first)
    .\tasks.ps1 skeleton    # regenerate missing structure
#>

param([Parameter(Position = 0)][string]$Task = "help")

$ErrorActionPreference = "Stop"

switch ($Task) {
    "install" {
        pip install -r requirements.txt
        pre-commit install
    }
    "test" {
        $env:LOG_TO_FILE = "0"          # PowerShell way to set an env var
        pytest tests/unit -q --cov=src/fraud_lakehouse
    }
    "lint" {
        ruff check src tests
        black --check src tests
    }
    "up" {
        docker compose up -d
        Start-Sleep -Seconds 8
        docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server localhost:9092 `
            --create --if-not-exists --topic onchain.transactions --partitions 3
        Write-Host "kafka topic OK. MinIO console -> http://localhost:9001"
    }
    "down"     { docker compose down -v }
    "itest"    { pytest tests/integration -m integration -q }
    "skeleton" { python skeleton.py }
    "produce"  { python -m fraud_lakehouse.streaming.producer }
    "stream"   { python -m fraud_lakehouse.streaming.stream_job }
    default {
        Write-Host "tasks: install | test | lint | up | down | itest | skeleton | produce | stream"
    }
}
