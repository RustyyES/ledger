# syntax=docker/dockerfile:1.7
FROM apache/airflow:2.10.4-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl git \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# dbt lives in the SAME image as Airflow rather than being shelled out to a
# separate container. The alternative -- DockerOperator or a k8s pod per task --
# is the right answer at scale and pure overhead here: it needs a Docker socket
# mount (a privilege escalation vector) to run a process that takes 40 seconds.
COPY orchestration/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY orchestration/dags/    /opt/airflow/dags/
COPY orchestration/plugins/ /opt/airflow/plugins/

# The sink's storage module, so the ingest DAG can list object-store prefixes
# without reimplementing S3 access.
#
# NOTE the build context is the REPOSITORY ROOT, not orchestration/ -- see the
# `airflow` service in docker-compose.yml. Docker cannot COPY from above the
# context, so `COPY ../services/...` is not merely discouraged, it is invalid.
COPY services/cdc-sink/storage.py /opt/airflow/dags/storage.py
COPY services/cdc-sink/config.py  /opt/airflow/dags/sink_config.py
COPY scripts/ /opt/ledger/scripts/

ENV PYTHONPATH=/opt/airflow/dags:/opt/ledger/transform
