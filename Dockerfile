FROM python:3.11.5-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl gcc libpq-dev procps \
    && pip install --upgrade pip \
    && pip install "poetry==1.8.3" \
    && poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --only main

COPY . .

RUN chmod +x ./scripts/*.sh
