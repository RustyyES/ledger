# Ledger -- production ELT platform with a live OLTP source.
#
# `make help` lists everything. `make up` is the entry point.

SHELL := /bin/bash
.DEFAULT_GOAL := help
.ONESHELL:

COMPOSE        ?= docker compose
PY             ?= python
DBT_DIR        := transform
SINK_DIR       := services/cdc-sink
API_DIR        := services/commerce-api
METRICS_DIR    := services/metrics-api
LOADGEN_DIR    := services/loadgen

# Laptop-sized by default. `make backfill SCALE=1.0` for the full 5M orders /
# 500k customers the spec calls for -- that takes ~12 minutes and ~6GB.
SCALE          ?= 0.02
START          ?= $(shell date -u -d '30 days ago' +%Y-%m-%d)
END            ?= $(shell date -u -d 'yesterday' +%Y-%m-%d)

export COMPOSE_BAKE = true

# --------------------------------------------------------------------------- #
##@ Getting started
# --------------------------------------------------------------------------- #

.PHONY: help
help:  ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nLedger\n\nUsage:\n  make \033[36m<target>\033[0m\n"} \
	/^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 } \
	/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)
	@echo

.PHONY: up
up: ## Bring the whole stack up, backfill history, and start CDC (~15 min)
	@set -euo pipefail
	$(MAKE) preflight
	@echo "==> 1/6  Building images"
	$(COMPOSE) build --parallel
	@echo "==> 2/6  Starting the source system"
	$(COMPOSE) up -d postgres
	$(COMPOSE) run --rm migrate
	$(COMPOSE) up -d commerce-api
	@echo "==> 3/6  Starting ingestion infrastructure"
	$(COMPOSE) up -d redpanda minio connect
	@echo "==> 4/6  Generating $(SCALE)x historical data"
	$(MAKE) backfill SCALE=$(SCALE)
	@echo "==> 5/6  Bulk-exporting history and wiring CDC"
	$(MAKE) setup-cdc
	$(COMPOSE) up -d cdc-sink loadgen
	@echo "==> 6/6  Building the warehouse and starting serving"
	$(COMPOSE) up -d airflow
	$(MAKE) warehouse
	$(COMPOSE) up -d metrics-api dashboard prometheus
	@$(MAKE) --no-print-directory urls

.PHONY: preflight
preflight: ## Check the machine can actually run this
	@set -euo pipefail
	command -v docker >/dev/null || { echo "docker is required"; exit 1; }
	$(COMPOSE) version >/dev/null || { echo "docker compose v2 is required"; exit 1; }
	docker info >/dev/null 2>&1 || { echo "the docker daemon is not running"; exit 1; }
	avail=$$(df -Pk . | awk 'NR==2 {print int($$4/1024/1024)}')
	if [ "$$avail" -lt 10 ]; then echo "need ~10GB free, found $${avail}GB"; exit 1; fi
	@echo "preflight OK"

.PHONY: urls
urls: ## Print every service URL
	@printf '\n\033[1mLedger is up\033[0m\n\n'
	@printf '  %-22s %s\n' "Commerce API"      "http://localhost:8000/docs"
	@printf '  %-22s %s\n' "Metrics API"       "http://localhost:8001/docs"
	@printf '  %-22s %s\n' "Quality dashboard" "http://localhost:8501"
	@printf '  %-22s %s\n' "Airflow"           "http://localhost:8080  (admin/admin)"
	@printf '  %-22s %s\n' "MinIO console"     "http://localhost:9001  (minioadmin/minioadmin)"
	@printf '  %-22s %s\n' "Prometheus"        "http://localhost:9090"
	@printf '  %-22s %s\n' "Kafka Connect"     "http://localhost:8083/connectors"
	@printf '\n  Try:  curl -H "X-API-Key: dev-key-change-me" localhost:8001/metrics/mrr | jq\n\n'

.PHONY: down
down: ## Stop everything, keep the data
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop everything and DELETE all data
	$(COMPOSE) down -v --remove-orphans
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/logs ci_warehouse.duckdb

.PHONY: logs
logs: ## Tail logs (SERVICE=cdc-sink to narrow)
	$(COMPOSE) logs -f --tail=100 $(SERVICE)

.PHONY: ps
ps: ## Show container status
	$(COMPOSE) ps

# --------------------------------------------------------------------------- #
##@ Data
# --------------------------------------------------------------------------- #

.PHONY: backfill
backfill: ## Generate historical data (SCALE=1.0 for the full 5M orders)
	$(COMPOSE) run --rm \
		-e LOADGEN_SCALE=$(SCALE) \
		loadgen python backfill.py --scale $(SCALE) --truncate

.PHONY: setup-cdc
setup-cdc: ## Bulk-export history, create the slot at that LSN, start Debezium
	$(COMPOSE) exec -T postgres sh -c 'true'
	PGHOST=localhost PGPORT=5432 bash scripts/setup_cdc.sh

.PHONY: verify-cdc
verify-cdc: ## Insert a row and prove it reaches Parquet within 90s
	@set -euo pipefail
	echo "==> inserting a customer through the API"
	id=$$(curl -sS -X POST localhost:8000/customers \
		-H 'Content-Type: application/json' \
		-H "Idempotency-Key: verify-$$(date +%s)-cdc" \
		-d '{"email":"cdc-probe-'"$$(date +%s)"'@example.com","name":"CDC Probe","country_code":"US"}' \
		| $(PY) -c 'import sys,json; print(json.load(sys.stdin)["id"])')
	echo "    customer $$id"
	echo "==> waiting up to 90s for it to appear in Parquet"
	for i in $$(seq 1 30); do \
		if $(COMPOSE) exec -T cdc-sink python /app/scripts/parquet_count.py customers --id "$$id" >/dev/null 2>&1; then \
			echo "    FOUND after $$((i*3))s"; exit 0; \
		fi; \
		sleep 3; \
	done; \
	echo "    NOT FOUND after 90s -- check 'make logs SERVICE=cdc-sink'"; exit 1

.PHONY: schema-change
schema-change: ## Apply migration 0003 mid-flight and watch the guard handle it
	@set -euo pipefail
	echo "==> applying migration 0003 (adds orders.channel) to the LIVE database"
	$(COMPOSE) run --rm migrate alembic upgrade head
	echo "==> populating channel on recently-updated orders"
	$(COMPOSE) exec -T postgres psql -U ledger -d ledger -c \
		"update orders set channel = (array['web','mobile','api'])[1 + (abs(hashtext(id::text)) % 3)], \
		 updated_at = now() where updated_at > now() - interval '2 days';"
	echo "==> the sink should now classify 'channel' as ADDITIVE. Watch:"
	echo "    make logs SERVICE=cdc-sink | grep schema_"
	echo "    then: make warehouse && check ops.schema_changes"

.PHONY: warehouse
warehouse: ## Build the dbt warehouse from scratch
	$(COMPOSE) exec -T airflow bash -lc 'cd /opt/ledger/transform && dbt build --full-refresh'

.PHONY: warehouse-incremental
warehouse-incremental: ## Incremental dbt build (what transform_dag runs)
	$(COMPOSE) exec -T airflow bash -lc 'cd /opt/ledger/transform && dbt build'

.PHONY: docs
docs: ## Generate and serve the dbt lineage graph on :8082
	$(COMPOSE) exec -T airflow bash -lc 'cd /opt/ledger/transform && dbt docs generate'
	$(COMPOSE) exec airflow bash -lc 'cd /opt/ledger/transform && dbt docs serve --port 8082'

# --------------------------------------------------------------------------- #
##@ Tests
# --------------------------------------------------------------------------- #

.PHONY: test
test: test-api test-loadgen test-sink test-dags test-dbt ## Run every test suite

.PHONY: test-api
test-api: ## Commerce API tests (needs Postgres)
	$(COMPOSE) up -d postgres
	cd $(API_DIR) && COMMERCE_TEST_DATABASE_URL=postgresql+psycopg://ledger:ledger@localhost:5432/ledger_test \
		$(PY) -m pytest -q --cov=app --cov-report=term-missing --cov-fail-under=70

.PHONY: test-loadgen
test-loadgen: ## Load generator behavioural-model tests (no services needed)
	cd $(LOADGEN_DIR) && $(PY) -m pytest tests/ -q

.PHONY: test-sink
test-sink: ## CDC sink tests (no Kafka needed -- the consumer is faked)
	cd $(SINK_DIR) && $(PY) -m pytest -q

.PHONY: test-dags
test-dags: ## Airflow DAG integrity tests
	cd orchestration && AIRFLOW_HOME=/tmp/airflow-test \
		PYTHONPATH=dags:../services/cdc-sink $(PY) -m pytest tests/ -q

.PHONY: test-dbt
test-dbt: ## dbt build + full test suite
	cd $(DBT_DIR) && dbt build

.PHONY: test-metrics
test-metrics: ## Metrics API tests (needs a built warehouse)
	cd $(METRICS_DIR) && PYTHONPATH=. $(PY) -m pytest -q

.PHONY: lint
lint: ## ruff + sqlfluff + mypy
	ruff check services/ orchestration/ scripts/
	ruff format --check services/ orchestration/ scripts/
	cd $(DBT_DIR) && sqlfluff lint models/ --processes 4
	# Per-service, NOT in one invocation. Both services have a top-level module
	# named `app`, and mypy resolves modules by name -- given both paths at once
	# it reports "Duplicate module named app" and checks neither. They are
	# separate deployables with separate dependency trees, so checking them
	# separately is also the honest thing to do.
	cd $(API_DIR)     && mypy app --ignore-missing-imports
	cd $(METRICS_DIR) && mypy app --ignore-missing-imports
	cd $(SINK_DIR)    && mypy . --ignore-missing-imports --exclude tests

.PHONY: fmt
fmt: ## Auto-fix formatting
	ruff check --fix services/ orchestration/ scripts/
	ruff format services/ orchestration/ scripts/
	cd $(DBT_DIR) && sqlfluff fix models/ --processes 4 --force

# --------------------------------------------------------------------------- #
##@ Proofs -- claims this project makes, and the commands that verify them
# --------------------------------------------------------------------------- #

.PHONY: proofs
proofs: prove-idempotency prove-lookback backfill-proof ## Run every proof

.PHONY: prove-idempotency
prove-idempotency: ## Replaying a partition is byte-identical -> results/
	$(PY) scripts/prove_idempotency.py --rows 100000
	@echo "proof written to results/idempotency_proof.json"

.PHONY: prove-lookback
prove-lookback: ## A 12-day-late refund still lands in fct_payments
	$(PY) scripts/prove_lookback.py

.PHONY: backfill-proof
backfill-proof: ## Re-running a date range reproduces it exactly -> results/
	$(COMPOSE) exec -T airflow bash -lc \
		'cd /opt/ledger && python scripts/backfill_proof.py --start $(START) --end $(END) --db /data/warehouse/ledger.duckdb'

.PHONY: prove-catchup
prove-catchup: ## Clear a week of transform_dag runs and watch them backfill
	@set -euo pipefail
	echo "==> clearing the last 7 days of transform_dag"
	$(COMPOSE) exec -T airflow airflow tasks clear transform_dag \
		--start-date $$(date -u -d '7 days ago' +%Y-%m-%d) \
		--end-date $$(date -u +%Y-%m-%d) --yes
	echo "==> watch them re-run in logical-date order at http://localhost:8080"

.PHONY: test-sla
test-sla: ## Artificially delay a task to prove the SLA callback fires
	@set -euo pipefail
	echo "==> forcing dbt_run_marts to exceed its 75-minute SLA"
	$(COMPOSE) exec -T airflow airflow variables set dbt_sla_test_delay_minutes 80
	$(COMPOSE) exec -T airflow airflow dags trigger transform_dag
	echo "==> the SLA miss callback should fire; check the scheduler log for 'sla_missed'"

.PHONY: chaos-kill-sink
chaos-kill-sink: ## SIGKILL the sink mid-batch, prove no loss and no duplicates
	@set -euo pipefail
	before=$$($(COMPOSE) exec -T cdc-sink python /app/scripts/parquet_count.py orders --distinct id)
	echo "==> $$before distinct orders before the kill"
	echo "==> SIGKILL (not SIGTERM -- no graceful drain, so buffered records are lost)"
	$(COMPOSE) kill -s SIGKILL cdc-sink
	sleep 2
	$(COMPOSE) up -d cdc-sink
	echo "==> waiting 90s for it to re-consume from the last committed offset"
	sleep 90
	after=$$($(COMPOSE) exec -T cdc-sink python /app/scripts/parquet_count.py orders --distinct id)
	total=$$($(COMPOSE) exec -T cdc-sink python /app/scripts/parquet_count.py orders)
	echo "==> $$after distinct orders, $$total rows total"
	if [ "$$after" -lt "$$before" ]; then echo "FAILED: data was lost"; exit 1; fi
	if [ "$$after" -ne "$$total" ]; then echo "FAILED: $$((total - after)) duplicate row(s)"; exit 1; fi
	echo "PASSED: no loss, no duplicates."

# --------------------------------------------------------------------------- #
##@ Utilities
# --------------------------------------------------------------------------- #

.PHONY: psql
psql: ## Open a psql shell on the source database
	$(COMPOSE) exec postgres psql -U ledger -d ledger

.PHONY: duckdb
duckdb: ## Open a DuckDB shell on the warehouse
	$(COMPOSE) exec airflow python -c \
		"import duckdb; duckdb.connect('/data/warehouse/ledger.duckdb').sql('show all tables').show()"

.PHONY: mrr
mrr: ## Fetch MRR from the metrics API
	curl -sS -H "X-API-Key: $${METRICS_API_KEYS:-dev-key-change-me}" \
		'localhost:8001/metrics/mrr?granularity=month' | $(PY) -m json.tool

.PHONY: lag
lag: ## Show CDC consumer lag
	curl -sS localhost:9103/metrics | grep -E '^sink_consumer_lag_records'

.PHONY: messiness
messiness: ## Verify all six deliberate messiness patterns are present
	$(COMPOSE) exec -T postgres psql -U ledger -d ledger -f /dev/stdin < scripts/verify_messiness.sql
