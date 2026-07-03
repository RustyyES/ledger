"""Schema evolution policy for the CDC sink.

The failure this prevents
------------------------
A source column changes type from int32 to string. A naive sink coerces, or
worse, pyarrow infers a new type per batch and the Parquet files in one
partition disagree with the files in the next. Nothing errors. The warehouse
reads the union, silently nulls what it cannot cast, and every aggregate built
on that column is quietly wrong -- for months, until somebody notices revenue
does not tie out.

Silent coercion is the most common way a warehouse starts lying. This module
exists so that it cannot happen without somebody being told.

The policy
----------
    ADDITIVE      new nullable column       ACCEPT, log INFO,  audit
    WIDENING      int32->int64, f32->f64    ACCEPT, log WARN,  audit
    DROPPED       column disappears         ACCEPT, backfill null, WARN, alert
    INCOMPATIBLE  narrowing / type change   REJECT batch, DLQ, alert,
                                            halt THIS table's consumer only

That last clause matters. Halting the whole sink because `refunds` changed
shape stops `orders` from ingesting too, and turns a one-table problem into a
platform outage. Per-table isolation is the difference between an incident and
a page.
"""

from __future__ import annotations

import enum
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import structlog

log = structlog.get_logger("schema_guard")


class ChangeClass(str, enum.Enum):
    NONE = "none"
    ADDITIVE = "additive"
    WIDENING = "widening"
    DROPPED = "dropped"
    INCOMPATIBLE = "incompatible"


#: Type transitions that are lossless and therefore safe to accept.
#: Everything not listed here is incompatible by default -- an allowlist, not a
#: denylist, because the cost of wrongly allowing a change is silent data loss
#: and the cost of wrongly rejecting one is a five-minute human decision.
SAFE_WIDENINGS: set[tuple[str, str]] = {
    ("int8", "int16"),
    ("int8", "int32"),
    ("int8", "int64"),
    ("int16", "int32"),
    ("int16", "int64"),
    ("int32", "int64"),
    ("uint8", "int16"),
    ("uint8", "int32"),
    ("uint8", "int64"),
    ("uint16", "int32"),
    ("uint16", "int64"),
    ("uint32", "int64"),
    ("float", "double"),
    ("int32", "double"),
    ("int16", "double"),
    ("int8", "double"),
    ("date32[day]", "timestamp[us, tz=UTC]"),
    ("string", "large_string"),
    ("binary", "large_binary"),
}


@dataclass
class SchemaChange:
    table: str
    column: str
    change: ChangeClass
    from_type: str | None
    to_type: str | None
    detected_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_row(self) -> dict[str, Any]:
        return asdict(self) | {"change": self.change.value}


@dataclass
class GuardVerdict:
    """The decision for one batch."""

    accepted: bool
    changes: list[SchemaChange]
    reconciled_schema: pa.Schema | None
    reason: str | None = None

    @property
    def severity(self) -> ChangeClass:
        if not self.changes:
            return ChangeClass.NONE
        order = [
            ChangeClass.INCOMPATIBLE,
            ChangeClass.DROPPED,
            ChangeClass.WIDENING,
            ChangeClass.ADDITIVE,
        ]
        for level in order:
            if any(c.change is level for c in self.changes):
                return level
        return ChangeClass.NONE


class SchemaRegistry:
    """Last-known-good schema per table, persisted as JSON.

    Deliberately a file rather than Confluent Schema Registry. The registry is
    another service to run, back up and reason about, and it solves a producer
    co-ordination problem this pipeline does not have -- there is exactly one
    producer. What we need is a durable record of "what did this table look
    like last time", which is a file.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._cache: dict[str, pa.Schema] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        raw = json.loads(self.path.read_text())
        for table, fields in raw.items():
            self._cache[table] = pa.schema(
                [
                    pa.field(name, pa.type_for_alias(t) if _is_alias(t) else _parse_type(t))
                    for name, t in fields
                ]
            )

    def _persist(self) -> None:
        payload = {
            table: [(f.name, str(f.type)) for f in schema] for table, schema in self._cache.items()
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        tmp.replace(self.path)  # atomic; a torn registry is unrecoverable

    def get(self, table: str) -> pa.Schema | None:
        return self._cache.get(table)

    def set(self, table: str, schema: pa.Schema) -> None:
        self._cache[table] = schema
        self._persist()


def _is_alias(t: str) -> bool:
    try:
        pa.type_for_alias(t)
        return True
    except (ValueError, KeyError):
        return False


def _parse_type(t: str) -> pa.DataType:
    """Reconstruct the handful of non-alias types this pipeline produces."""
    if t.startswith("timestamp"):
        tz = None
        if "tz=" in t:
            tz = t.split("tz=")[1].rstrip("]").strip()
        unit = t.split("[")[1].split(",")[0].strip()
        return pa.timestamp(unit, tz=tz)
    if t.startswith("decimal128"):
        precision, scale = t[t.index("(") + 1 : t.index(")")].split(",")
        return pa.decimal128(int(precision), int(scale))
    if t == "date32[day]":
        return pa.date32()
    raise ValueError(f"cannot reconstruct arrow type from {t!r}")


class SchemaGuard:
    def __init__(self, registry: SchemaRegistry) -> None:
        self.registry = registry
        self.halted: set[str] = set()

    def is_halted(self, table: str) -> bool:
        return table in self.halted

    def resume(self, table: str) -> None:
        """Clear a halt after a human has decided what to do."""
        self.halted.discard(table)
        log.warning("table_consumer_resumed", table=table)

    def inspect(self, table: str, incoming: pa.Schema) -> GuardVerdict:
        known = self.registry.get(table)
        if known is None:
            self.registry.set(table, incoming)
            log.info("schema_registered", table=table, columns=len(incoming))
            return GuardVerdict(accepted=True, changes=[], reconciled_schema=incoming)

        changes: list[SchemaChange] = []
        known_fields = {f.name: f.type for f in known}
        incoming_fields = {f.name: f.type for f in incoming}

        for name, new_type in incoming_fields.items():
            if name not in known_fields:
                changes.append(SchemaChange(table, name, ChangeClass.ADDITIVE, None, str(new_type)))
                continue
            old_type = known_fields[name]
            if old_type.equals(new_type):
                continue
            if (str(old_type), str(new_type)) in SAFE_WIDENINGS:
                changes.append(
                    SchemaChange(table, name, ChangeClass.WIDENING, str(old_type), str(new_type))
                )
            else:
                changes.append(
                    SchemaChange(
                        table, name, ChangeClass.INCOMPATIBLE, str(old_type), str(new_type)
                    )
                )

        for name, old_type in known_fields.items():
            if name not in incoming_fields:
                changes.append(SchemaChange(table, name, ChangeClass.DROPPED, str(old_type), None))

        blocking = [c for c in changes if c.change is ChangeClass.INCOMPATIBLE]
        if blocking:
            self.halted.add(table)
            detail = "; ".join(f"{c.column}: {c.from_type} -> {c.to_type}" for c in blocking)
            log.error(
                "schema_incompatible_batch_rejected",
                table=table,
                changes=detail,
                action="consumer halted for this table only",
            )
            return GuardVerdict(
                accepted=False,
                changes=changes,
                reconciled_schema=None,
                reason=f"incompatible schema change on {table}: {detail}",
            )

        reconciled = self._reconcile(known, incoming, changes)
        for c in changes:
            if c.change is ChangeClass.ADDITIVE:
                log.info("schema_additive", table=table, column=c.column, type=c.to_type)
            elif c.change is ChangeClass.WIDENING:
                log.warning(
                    "schema_widened",
                    table=table,
                    column=c.column,
                    from_type=c.from_type,
                    to_type=c.to_type,
                )
            elif c.change is ChangeClass.DROPPED:
                log.warning(
                    "schema_column_dropped",
                    table=table,
                    column=c.column,
                    action="backfilling null to preserve the historical shape",
                )

        if changes:
            self.registry.set(table, reconciled)
        return GuardVerdict(accepted=True, changes=changes, reconciled_schema=reconciled)

    @staticmethod
    def _reconcile(known: pa.Schema, incoming: pa.Schema, changes: list[SchemaChange]) -> pa.Schema:
        """Build the schema to actually write.

        Union of both, keeping the widened type where one applies, and RETAINING
        dropped columns as nulls. Retention is the important half: physically
        removing a dropped column mid-partition means files in the same
        `_ingested_date=` prefix have different shapes, and every engine that
        reads that prefix has to guess. Carrying the null costs a few bytes
        under zstd and keeps the partition rectangular.
        """
        widened = {c.column: c.to_type for c in changes if c.change is ChangeClass.WIDENING}
        incoming_by_name = {f.name: f for f in incoming}
        fields: list[pa.Field] = []

        for f in known:
            if f.name in widened:
                fields.append(pa.field(f.name, incoming_by_name[f.name].type, nullable=True))
            elif f.name in incoming_by_name:
                fields.append(pa.field(f.name, f.type, nullable=True))
            else:
                fields.append(pa.field(f.name, f.type, nullable=True))  # dropped -> keep as null

        known_names = {f.name for f in known}
        for f in incoming:
            if f.name not in known_names:
                fields.append(pa.field(f.name, f.type, nullable=True))
        return pa.schema(fields)


def conform(batch: pa.Table, schema: pa.Schema) -> pa.Table:
    """Cast a batch to the reconciled schema, adding null columns as needed."""
    arrays = []
    for field_ in schema:
        if field_.name in batch.column_names:
            column = batch.column(field_.name)
            arrays.append(
                column if column.type.equals(field_.type) else column.cast(field_.type, safe=False)
            )
        else:
            arrays.append(pa.nulls(batch.num_rows, type=field_.type))
    return pa.Table.from_arrays(arrays, schema=schema)
