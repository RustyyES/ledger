# syntax=docker/dockerfile:1.7
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirements.txt
COPY profiles.py generator.py backfill.py ./

RUN useradd --create-home --uid 10002 loadgen && chown -R loadgen:loadgen /app
USER loadgen

EXPOSE 9102

# Live mode is the long-running default. The backfill is a one-shot and is
# invoked explicitly by `make backfill`, which overrides this command.
CMD ["python", "generator.py"]
