"""Alembic environment.

Two things worth noting for anyone reading this as a reference:

1. The URL comes from the environment, never from alembic.ini. A DSN with a
   password in a committed file is the most common way credentials leak out of
   a data project.
2. `compare_type=True` and `compare_server_default=True` are on. Without them
   autogenerate silently misses a varchar(50) -> text change, which is exactly
   the kind of drift that later shows up as a truncated column in the
   warehouse.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from app.models import Base
from sqlalchemy import engine_from_config, pool

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

database_url = os.environ.get("COMMERCE_DATABASE_URL")
if not database_url:
    raise RuntimeError("COMMERCE_DATABASE_URL is not set. Alembic will not guess a default.")
config.set_main_option("sqlalchemy.url", database_url)

target_metadata = Base.metadata


def include_object(obj, name, type_, reflected, compare_to) -> bool:
    """Keep Debezium's replication artefacts out of autogenerate."""
    return not (type_ == "table" and name in {"debezium_signal", "debezium_heartbeat"})


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            include_object=include_object,
            transaction_per_migration=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
