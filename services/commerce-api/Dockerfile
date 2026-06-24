# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS base

# Python 3.12 rather than 3.13/3.14: psycopg[binary], pyarrow and the dbt
# adapters all ship prebuilt wheels for it. Chasing the newest interpreter here
# buys nothing and costs a source build of Arrow.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first, as their own layer: application code changes on every
# commit, requirements.txt changes monthly. Reversing these two COPYs turns a
# 3-second rebuild into a 90-second one.
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

FROM base AS runtime

COPY alembic.ini .
COPY alembic/ ./alembic/
COPY app/ ./app/

# Non-root. The container writes nothing to disk, so there is no reason for it
# to be able to.
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health', timeout=2).status==200 else 1)"

# --proxy-headers so X-Forwarded-* is honoured behind the compose network.
# Two workers is deliberate at this scale: the bottleneck is Postgres, and more
# workers just means more idle connections held out of the pool.
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "2", "--proxy-headers", "--no-access-log"]

FROM base AS test
COPY requirements-dev.txt .
RUN pip install -r requirements-dev.txt
COPY . .
CMD ["pytest", "-q", "--cov=app", "--cov-report=term-missing", "--cov-fail-under=70"]
