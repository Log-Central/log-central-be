#!/bin/bash
set -e

# Celery 워커 실행 (OTEL 계측 포함)
exec poetry run opentelemetry-instrument \
    celery -A config worker --loglevel=INFO
