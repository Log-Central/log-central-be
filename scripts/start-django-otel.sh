#!/bin/bash
set -euo pipefail

poetry run python manage.py migrate

exec poetry run opentelemetry-instrument \
  python manage.py runserver 0.0.0.0:8000 \
  --noreload \
  --settings=config.settings
