"""Metrics API configuration."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="METRICS_", env_file=".env", extra="ignore")

    warehouse_path: str = Field(default="/data/warehouse/ledger.duckdb")
    service_name: str = Field(default="metrics-api")
    env: str = Field(default="local")
    log_level: str = Field(default="INFO")

    # --- auth ---------------------------------------------------------------
    # A set, not a single key: rotating a shared secret with one slot means a
    # window where either the old or the new key is rejected. Two slots means
    # you can add the new key, migrate callers, then remove the old one.
    api_keys: str = Field(
        default="dev-key-change-me",
        description="Comma-separated valid API keys.",
    )

    # --- caching ------------------------------------------------------------
    cache_ttl_seconds: int = Field(default=300)  # 5 minutes, per the spec
    cache_max_entries: int = Field(default=512)

    # --- freshness ----------------------------------------------------------
    # Beyond this the API returns 503 rather than serving stale numbers. Serving
    # stale data silently is worse than serving none: a dashboard that is six
    # hours behind looks exactly like a dashboard that is current.
    staleness_threshold_hours: float = Field(default=26.0)

    max_page_size: int = Field(default=200)
    default_page_size: int = Field(default=50)

    @property
    def valid_api_keys(self) -> set[str]:
        return {k.strip() for k in self.api_keys.split(",") if k.strip()}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
