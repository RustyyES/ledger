"""Runtime configuration for the commerce API.

Every value is environment-driven. Nothing here has a production-safe default
except the ones that are safe by construction (timeouts, page sizes); the
database URL deliberately has no default so a misconfigured container fails at
import time rather than silently pointing at localhost.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="COMMERCE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- database -----------------------------------------------------------
    database_url: PostgresDsn = Field(
        ...,
        description="SQLAlchemy DSN. Must use the psycopg (v3) driver.",
    )
    db_pool_size: int = Field(default=10, ge=1, le=100)
    db_max_overflow: int = Field(default=20, ge=0, le=200)
    db_pool_timeout_s: int = Field(default=10, ge=1)
    db_statement_timeout_ms: int = Field(
        default=15_000,
        ge=100,
        description="Server-side statement_timeout. Bounds the worst-case "
        "request so a bad query cannot pin a worker indefinitely.",
    )

    # --- service ------------------------------------------------------------
    env: str = Field(default="local")
    log_level: str = Field(default="INFO")
    service_name: str = Field(default="commerce-api")

    # --- behaviour ----------------------------------------------------------
    idempotency_ttl_hours: int = Field(
        default=24,
        ge=1,
        description="How long a replayed Idempotency-Key returns the stored "
        "response. Beyond this the key is reusable.",
    )
    require_idempotency_key: bool = Field(
        default=True,
        description="When true, POST without Idempotency-Key returns 400. "
        "Disabled in some tests to exercise the non-idempotent path.",
    )
    default_page_size: int = Field(default=50, ge=1, le=500)
    max_page_size: int = Field(default=500, ge=1, le=1000)

    @property
    def sqlalchemy_url(self) -> str:
        return str(self.database_url)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
