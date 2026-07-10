# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1

# librdkafka is a C library; confluent-kafka's wheel bundles it, but the build
# tools are needed if pip ever falls back to a source build on a new platform.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirements.txt

FROM base AS runtime
COPY sink.py schema_guard.py storage.py config.py bulk_export.py ./
# `make verify-cdc` and `make chaos-kill-sink` exec this inside the container.
COPY scripts/parquet_count.py ./scripts/parquet_count.py

# The schema registry is a FILE and it must survive a restart. Losing it means
# the guard treats every table as newly-seen on the next batch, which silently
# accepts a schema change it would otherwise have rejected. Mounted as a volume
# in compose.
RUN mkdir -p /var/lib/ledger \
    && useradd --create-home --uid 10003 sink \
    && chown -R sink:sink /app /var/lib/ledger
USER sink
VOLUME ["/var/lib/ledger"]
EXPOSE 9103

HEALTHCHECK --interval=20s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:9103/metrics',timeout=3).status==200 else 1)"

CMD ["python", "sink.py"]

FROM base AS test
COPY requirements-dev.txt .
RUN pip install -r requirements-dev.txt
COPY . .
CMD ["pytest", "-q"]
