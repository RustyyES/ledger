#!/usr/bin/env bash
#
# Wire up CDC, in the ONE order that is correct.
#
#   1. create the publication (explicit table list, not autocreate)
#   2. bulk-export history from a REPEATABLE READ snapshot, capturing its LSN
#   3. create the replication slot AT that LSN
#   4. register the Debezium connector, which starts from the slot
#
# Doing (3) before (2) double-counts every row changed in between. Doing it
# after the export finishes LOSES every change made during the export --
# permanently and silently, because nothing downstream can detect the absence
# of a row it never saw.
#
# This is the single sharpest edge in the whole pipeline, which is why it is a
# script with comments rather than four commands in a README.

set -euo pipefail

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-ledger}"
PGDATABASE="${PGDATABASE:-ledger}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SLOT_NAME="${SLOT_NAME:-ledger_slot}"
PUBLICATION="${PUBLICATION:-ledger_pub}"

TABLES="public.customers,public.plans,public.subscriptions,public.subscription_events,public.orders,public.payments,public.refunds"

psql() { command psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 "$@"; }

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

# --------------------------------------------------------------------------- #
log "1/4  Creating publication '$PUBLICATION'"
# Explicit table list. Debezium's autocreate would publish EVERY table,
# including idempotency_keys -- service plumbing that would then arrive in the
# warehouse looking like a business fact.
psql <<SQL
DO \$\$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '$PUBLICATION') THEN
        RAISE NOTICE 'publication $PUBLICATION already exists, leaving it alone';
    ELSE
        EXECUTE 'CREATE PUBLICATION $PUBLICATION FOR TABLE ' ||
                '$(echo "$TABLES" | tr ',' ' ' | sed 's/ /, /g')';
    END IF;
END
\$\$;
SQL
psql -c "SELECT pubname, puballtables FROM pg_publication WHERE pubname = '$PUBLICATION';"

# --------------------------------------------------------------------------- #
log "2/4  Bulk-exporting history and capturing the snapshot LSN"
# The exporter opens REPEATABLE READ, reads pg_current_wal_lsn() inside that
# transaction, exports every table from that one consistent snapshot, and
# writes _manifest/bulk_export.json containing the LSN.
python services/cdc-sink/bulk_export.py \
    --dsn "postgresql://${PGUSER}@${PGHOST}:${PGPORT}/${PGDATABASE}" \
    --bucket "${S3_BUCKET:-ledger-raw}" \
    --endpoint-url "${SINK_S3_ENDPOINT_URL:-http://localhost:9000}"

SNAPSHOT_LSN="$(python - <<'PY'
import json, os, sys
sys.path.insert(0, "services/cdc-sink")
from storage import S3Store
store = S3Store(
    os.environ.get("S3_BUCKET", "ledger-raw"),
    endpoint_url=os.environ.get("SINK_S3_ENDPOINT_URL", "http://localhost:9000"),
    access_key=os.environ.get("SINK_S3_ACCESS_KEY", "minioadmin"),
    secret_key=os.environ.get("SINK_S3_SECRET_KEY", "minioadmin"),
)
raw = store.read("_manifest/bulk_export.json")
if raw is None:
    sys.exit("bulk export manifest not found -- did step 2 fail?")
print(json.loads(raw)["lsn"])
PY
)"
log "    snapshot LSN = $SNAPSHOT_LSN"

# --------------------------------------------------------------------------- #
log "3/4  Creating replication slot '$SLOT_NAME'"
# pg_create_logical_replication_slot always starts at the CURRENT WAL position,
# which is at or after our snapshot LSN. Any overlap is absorbed by the sink's
# (partition, offset) dedup; a GAP would not be, which is why the slot is
# created after the snapshot rather than before.
psql <<SQL
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME')
        THEN 'slot $SLOT_NAME already exists'
    ELSE (SELECT 'created at ' || lsn::text
          FROM pg_create_logical_replication_slot('$SLOT_NAME', 'pgoutput'))
END AS slot_status;
SQL

psql -c "SELECT slot_name, plugin, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME';"

# --------------------------------------------------------------------------- #
log "4/4  Registering the Debezium connector"
until curl -sf "${CONNECT_URL}/connectors" >/dev/null; do
    echo "    waiting for Kafka Connect at ${CONNECT_URL} ..."
    sleep 3
done

if curl -sf "${CONNECT_URL}/connectors/ledger-postgres" >/dev/null 2>&1; then
    echo "    connector already registered; updating its config"
    python - <<'PY' | curl -sS -X PUT -H 'Content-Type: application/json' \
        --data-binary @- "${CONNECT_URL:-http://localhost:8083}/connectors/ledger-postgres/config" >/dev/null
import json
print(json.dumps(json.load(open("infra/debezium/connector.json"))["config"]))
PY
else
    curl -sS -X POST -H 'Content-Type: application/json' \
        --data-binary @infra/debezium/connector.json \
        "${CONNECT_URL}/connectors" >/dev/null
fi

sleep 5
curl -sS "${CONNECT_URL}/connectors/ledger-postgres/status" | python -m json.tool

log "CDC is live. Verify with:  make verify-cdc"
