"""Read-only warehouse access, with freshness tracking and a TTL cache."""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import OrderedDict
from datetime import UTC, datetime
from typing import Any

import duckdb
import structlog

from app.config import get_settings

log = structlog.get_logger("warehouse")


class TTLCache:
    """Small LRU + TTL cache.

    In-process on purpose. A shared Redis would be the right answer for more
    than one replica, and it is the wrong answer here: it adds a service, a
    failure mode and a serialisation format to save a query that takes 8ms
    against a local DuckDB file. The `X-Cache` header makes the behaviour
    observable either way, so swapping the implementation later changes nothing
    a client can see.
    """

    def __init__(self, ttl_seconds: int, max_entries: int) -> None:
        self.ttl = ttl_seconds
        self.max_entries = max_entries
        self._data: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    @staticmethod
    def key(*parts: Any) -> str:
        return hashlib.sha256(json.dumps(parts, sort_keys=True, default=str).encode()).hexdigest()

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                self.misses += 1
                return None
            stored_at, value = entry
            if time.monotonic() - stored_at > self.ttl:
                del self._data[key]
                self.misses += 1
                return None
            self._data.move_to_end(key)
            self.hits += 1
            return value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = (time.monotonic(), value)
            self._data.move_to_end(key)
            while len(self._data) > self.max_entries:
                self._data.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


_settings = get_settings()
cache = TTLCache(_settings.cache_ttl_seconds, _settings.cache_max_entries)


def query(sql: str, params: list | None = None) -> list[dict]:
    """Run a read-only query and return dicts.

    A NEW read-only connection per query, not a long-lived one. DuckDB takes a
    file lock, and holding a connection open across the dbt rebuild blocks the
    rebuild -- the API would keep the warehouse from being refreshed, which is
    a genuinely unpleasant deadlock to diagnose because everything looks
    healthy on both sides.
    """
    settings = get_settings()
    started = time.perf_counter()
    with duckdb.connect(settings.warehouse_path, read_only=True) as con:
        cursor = con.execute(sql, params or [])
        columns = [d[0] for d in cursor.description]
        rows = [dict(zip(columns, r, strict=False)) for r in cursor.fetchall()]
    log.debug("query", ms=round((time.perf_counter() - started) * 1000, 2), rows=len(rows))
    return rows


#: Freshness is re-read at most this often. It is checked on EVERY request --
#: twice, by `guard_freshness` and again when building the response headers --
#: and each check opens a DuckDB connection costing ~37ms. Unmemoised that is
#: 75ms of the 200ms p95 budget spent re-answering a question whose answer
#: changes once a day. Ten seconds is short enough that a rebuild is visible
#: almost immediately and long enough that the cost disappears.
_FRESHNESS_TTL_SECONDS = 10.0
_freshness_cache: tuple[float, tuple[datetime | None, float | None]] | None = None
_freshness_lock = threading.Lock()


def data_freshness(*, force: bool = False) -> tuple[datetime | None, float | None]:
    """(last successful mart refresh, hours since).

    Read from the marts themselves rather than from a status table somebody
    has to remember to update. The most recent `_ingested_at` in fct_payments
    is the newest fact the warehouse has actually absorbed, which is what a
    consumer asking "how current is this" actually wants to know.
    """
    global _freshness_cache

    if not force:
        with _freshness_lock:
            cached_entry = _freshness_cache
        if cached_entry is not None:
            stored_at, value = cached_entry
            if time.monotonic() - stored_at < _FRESHNESS_TTL_SECONDS:
                # Recompute the AGE from the cached timestamp rather than
                # returning a stale age -- otherwise the reported freshness
                # freezes for the TTL, which is exactly the lie this header
                # exists to prevent.
                last_seen = value[0]
                if last_seen is None:
                    return None, None
                age = (datetime.now(UTC) - last_seen).total_seconds() / 3600
                return last_seen, age

    try:
        rows = query("select max(_ingested_at) as last_ingested from marts.fct_payments")
    except duckdb.Error as exc:
        log.warning("freshness_unavailable", error=str(exc))
        return None, None

    if not rows or rows[0]["last_ingested"] is None:
        with _freshness_lock:
            _freshness_cache = (time.monotonic(), (None, None))
        return None, None

    last = rows[0]["last_ingested"]
    if last.tzinfo is None:
        last = last.replace(tzinfo=UTC)
    hours = (datetime.now(UTC) - last).total_seconds() / 3600
    with _freshness_lock:
        _freshness_cache = (time.monotonic(), (last, hours))
    return last, hours


def reset_freshness_cache() -> None:
    """Test hook. Production never needs this."""
    global _freshness_cache
    with _freshness_lock:
        _freshness_cache = None
