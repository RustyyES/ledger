"""The four schema-change classes, plus the isolation guarantee.

`test_incompatible_change_halts_only_the_offending_table` is the one that
matters operationally: it is the difference between a one-table incident and a
platform outage.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from schema_guard import ChangeClass, SchemaGuard, SchemaRegistry, conform


@pytest.fixture
def guard(tmp_path) -> SchemaGuard:
    return SchemaGuard(SchemaRegistry(tmp_path / "registry.json"))


BASE = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("amount_cents", pa.int32()),
        pa.field("status", pa.string()),
    ]
)


def test_first_sighting_registers_without_complaint(guard):
    verdict = guard.inspect("orders", BASE)
    assert verdict.accepted
    assert verdict.changes == []
    assert guard.registry.get("orders") is not None


def test_identical_schema_is_a_no_op(guard):
    guard.inspect("orders", BASE)
    verdict = guard.inspect("orders", BASE)
    assert verdict.accepted
    assert verdict.severity is ChangeClass.NONE


# --- 1. ADDITIVE ----------------------------------------------------------- #


def test_additive_column_is_accepted_and_recorded(guard):
    """This is migration 0003 (`orders.channel`) arriving through CDC."""
    guard.inspect("orders", BASE)
    evolved = pa.schema([*BASE, pa.field("channel", pa.string())])

    verdict = guard.inspect("orders", evolved)
    assert verdict.accepted
    assert verdict.severity is ChangeClass.ADDITIVE
    assert [c.column for c in verdict.changes] == ["channel"]
    assert "channel" in verdict.reconciled_schema.names
    assert not guard.is_halted("orders")


# --- 2. WIDENING ----------------------------------------------------------- #


@pytest.mark.parametrize(
    "old,new",
    [
        (pa.int32(), pa.int64()),
        (pa.float32(), pa.float64()),
        (pa.int16(), pa.int32()),
    ],
)
def test_type_widening_is_accepted_at_warn(guard, old, new):
    guard.inspect("t", pa.schema([pa.field("v", old)]))
    verdict = guard.inspect("t", pa.schema([pa.field("v", new)]))
    assert verdict.accepted
    assert verdict.severity is ChangeClass.WIDENING
    assert verdict.reconciled_schema.field("v").type.equals(new)


# --- 3. DROPPED ------------------------------------------------------------ #


def test_dropped_column_is_retained_as_null(guard):
    """Retention keeps the partition rectangular. See _reconcile's docstring."""
    guard.inspect("orders", BASE)
    reduced = pa.schema([pa.field("id", pa.string()), pa.field("amount_cents", pa.int32())])

    verdict = guard.inspect("orders", reduced)
    assert verdict.accepted
    assert verdict.severity is ChangeClass.DROPPED
    assert "status" in verdict.reconciled_schema.names, "dropped column was physically removed"

    batch = pa.table({"id": ["a"], "amount_cents": [1]})
    conformed = conform(batch, verdict.reconciled_schema)
    assert conformed.column("status").to_pylist() == [None]


# --- 4. INCOMPATIBLE ------------------------------------------------------- #


@pytest.mark.parametrize(
    "old,new",
    [
        (pa.int64(), pa.int32()),  # narrowing
        (pa.int32(), pa.string()),  # type change
        (pa.timestamp("us", tz="UTC"), pa.string()),
        (pa.float64(), pa.float32()),
    ],
)
def test_incompatible_change_is_rejected(guard, old, new):
    guard.inspect("payments", pa.schema([pa.field("v", old)]))
    verdict = guard.inspect("payments", pa.schema([pa.field("v", new)]))
    assert not verdict.accepted
    assert verdict.severity is ChangeClass.INCOMPATIBLE
    assert verdict.reason and "incompatible" in verdict.reason
    assert guard.is_halted("payments")


def test_incompatible_change_halts_only_the_offending_table(guard):
    """Per-table isolation. The whole point of the halt policy."""
    guard.inspect("payments", pa.schema([pa.field("v", pa.int64())]))
    guard.inspect("orders", BASE)

    guard.inspect("payments", pa.schema([pa.field("v", pa.string())]))

    assert guard.is_halted("payments")
    assert not guard.is_halted("orders"), "an unrelated table was halted"
    # orders keeps ingesting normally
    assert guard.inspect("orders", BASE).accepted


def test_rejected_schema_is_not_written_to_the_registry(guard):
    """A rejected batch must not poison the last-known-good schema."""
    guard.inspect("payments", pa.schema([pa.field("v", pa.int64())]))
    guard.inspect("payments", pa.schema([pa.field("v", pa.string())]))
    assert guard.registry.get("payments").field("v").type.equals(pa.int64())


def test_halt_can_be_cleared_by_an_operator(guard):
    guard.inspect("payments", pa.schema([pa.field("v", pa.int64())]))
    guard.inspect("payments", pa.schema([pa.field("v", pa.string())]))
    guard.resume("payments")
    assert not guard.is_halted("payments")


# --- registry durability --------------------------------------------------- #


def test_registry_survives_a_restart(tmp_path):
    path = tmp_path / "registry.json"
    schema = pa.schema(
        [
            pa.field("id", pa.string()),
            pa.field("ts", pa.timestamp("us", tz="UTC")),
            pa.field("n", pa.int64()),
        ]
    )
    SchemaGuard(SchemaRegistry(path)).inspect("orders", schema)

    reloaded = SchemaGuard(SchemaRegistry(path))
    assert reloaded.registry.get("orders").equals(schema)
    # And the reloaded registry still detects change correctly.
    verdict = reloaded.inspect("orders", pa.schema([*schema, pa.field("x", pa.string())]))
    assert verdict.severity is ChangeClass.ADDITIVE


def test_conform_casts_and_pads(guard):
    target = pa.schema(
        [
            pa.field("id", pa.string()),
            pa.field("n", pa.int64()),
            pa.field("missing", pa.string()),
        ]
    )
    batch = pa.table({"id": ["a", "b"], "n": pa.array([1, 2], pa.int32())})
    out = conform(batch, target)
    assert out.schema.equals(target)
    assert out.column("n").to_pylist() == [1, 2]
    assert out.column("missing").to_pylist() == [None, None]
