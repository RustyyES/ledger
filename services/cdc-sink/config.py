"""Sink configuration."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SinkSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SINK_", env_file=".env", extra="ignore")

    # --- kafka --------------------------------------------------------------
    bootstrap_servers: str = Field(default="redpanda:9092")
    consumer_group: str = Field(default="ledger-sink")
    topic_prefix: str = Field(default="ledger")
    tables: list[str] = Field(
        default=[
            "customers",
            "plans",
            "subscriptions",
            "subscription_events",
            "orders",
            "payments",
            "refunds",
        ]
    )
    # `earliest`, always. `latest` means a sink restart silently skips whatever
    # arrived while it was down, and the gap is invisible until a reconciliation
    # check catches it days later.
    auto_offset_reset: str = Field(default="earliest")
    poll_timeout_s: float = Field(default=1.0)

    # --- batching -----------------------------------------------------------
    batch_max_records: int = Field(default=50_000, ge=1)
    batch_max_seconds: float = Field(default=60.0, gt=0)

    # --- storage ------------------------------------------------------------
    s3_endpoint_url: str | None = Field(default="http://minio:9000")
    s3_bucket: str = Field(default="ledger-raw")
    s3_access_key: str = Field(default="minioadmin")
    s3_secret_key: str = Field(default="minioadmin")
    s3_region: str = Field(default="us-east-1")
    local_path: str | None = Field(
        default=None,
        description="When set, write to this local directory instead of S3. "
        "Used by tests and by the CI dbt job, which has no object store.",
    )

    # zstd, not snappy. Justified in DESIGN.md: ~2.2x better ratio on this
    # data at a decode cost DuckDB does not notice, and the files are read far
    # more often than they are written.
    compression: str = Field(default="zstd")
    compression_level: int = Field(default=3)
    row_group_size: int = Field(default=128 * 1024)

    # --- reliability --------------------------------------------------------
    dlq_prefix: str = Field(default="_dlq")
    schema_audit_table: str = Field(default="_schema_changes")
    metrics_port: int = Field(default=9103)
    lag_poll_seconds: float = Field(default=15.0)

    @property
    def topics(self) -> list[str]:
        return [f"{self.topic_prefix}.public.{t}" for t in self.tables]


@lru_cache(maxsize=1)
def get_settings() -> SinkSettings:
    return SinkSettings()
