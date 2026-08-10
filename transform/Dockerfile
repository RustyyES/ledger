# syntax=docker/dockerfile:1.7
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 DBT_PROFILES_DIR=/opt/ledger/transform

RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/ledger/transform
RUN pip install "dbt-core==1.9.1" "dbt-duckdb==1.9.1"

# Packages are installed at BUILD time, not at run time. Running `dbt deps` on
# every DAG run makes the pipeline depend on the dbt hub being reachable, which
# turns somebody else's outage into ours.
COPY packages.yml dbt_project.yml profiles.yml ./
RUN dbt deps

COPY macros/ ./macros/
COPY models/ ./models/
COPY seeds/ ./seeds/
COPY snapshots/ ./snapshots/
COPY tests/ ./tests/

CMD ["dbt", "build"]
