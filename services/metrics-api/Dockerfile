# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirements.txt

FROM base AS runtime
COPY app/ ./app/
RUN useradd --create-home --uid 10004 metrics && chown -R metrics:metrics /app
USER metrics
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health',timeout=2).status==200 else 1)"
# One worker. The warehouse is a single DuckDB FILE and every worker opens its
# own read-only connection; more workers multiply the file handles without
# multiplying the throughput, because the bottleneck is the file, not the CPU.
# On Snowflake this would be 4.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log"]
