#!/bin/bash
poetry run celery -A config worker --loglevel=DEBUG --concurrency=1
