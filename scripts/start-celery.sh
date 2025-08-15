#!/bin/bash
poetry run celery -A config worker -l info --pool=prefork --concurrency=2 -E
