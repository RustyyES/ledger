-- Runs once, on first container start, before the application connects.
--
-- Only things that must exist BEFORE Alembic runs belong here. The schema
-- itself is Alembic's job -- duplicating it here would create two sources of
-- truth for the same tables, and they would drift.

-- --------------------------------------------------------------------------
-- Logical replication prerequisites.
--
-- `wal_level=logical` is set on the command line in docker-compose.yml rather
-- than here, because it requires a restart and ALTER SYSTEM inside an init
-- script would not take effect until one.
-- --------------------------------------------------------------------------

-- Debezium's heartbeat target. Excluded from the publication on purpose: it is
-- plumbing that exists to keep the replication slot advancing while the real
-- tables are quiet. See infra/debezium/README.md.
CREATE TABLE IF NOT EXISTS public.debezium_heartbeat (
    id      integer PRIMARY KEY,
    beat_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO public.debezium_heartbeat (id) VALUES (1)
    ON CONFLICT (id) DO NOTHING;

-- --------------------------------------------------------------------------
-- A dedicated replication role.
--
-- Debezium connects as this role, NOT as the application user. Two reasons:
-- the application user can write, and a connector does not need to; and when
-- something is holding a replication slot open you want `pg_stat_activity` to
-- name it unambiguously.
-- --------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
        CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD 'debezium';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE ledger TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- The test database, so `make test` does not need a second container.
SELECT 'CREATE DATABASE ledger_test'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ledger_test')\gexec
