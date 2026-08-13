"""Customer timeline with cursor pagination.

WHY CURSOR AND NOT OFFSET.

`LIMIT 50 OFFSET 5000` makes the database scan and discard 5,000 rows to return
50, so page 100 costs a hundred times page 1. Worse, the result set is not
stable: a refund landing between two requests shifts every subsequent row by
one, so the reader sees a duplicate or misses an event entirely -- and this
timeline is exactly the kind of endpoint someone pages through while data is
arriving.

A keyset cursor encodes the sort key of the last row seen. The next page is
`WHERE (occurred_at, reference_id) < (cursor)`, which uses the index, costs the
same on page 100 as on page 1, and cannot skip or duplicate under concurrent
writes.

`reference_id` is in the key as a tiebreaker, not decoration: several events can
share a timestamp to the microsecond -- an order and its payment are written in
one transaction -- and a cursor on `occurred_at` alone would loop forever or
skip the tied rows.
"""

from __future__ import annotations

import base64
import binascii
import json
from datetime import datetime
from typing import Any


def encode_cursor(occurred_at: datetime, reference_id: str) -> str:
    payload = json.dumps({"t": occurred_at.isoformat(), "r": reference_id}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, str]:
    """Decode, or raise ValueError.

    Opaque to the client on purpose: base64 signals "do not construct this
    yourself". It is NOT a security boundary -- it is trivially decodable -- so
    it carries only the sort key, never anything the caller is not already
    entitled to see.
    """
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
        return datetime.fromisoformat(payload["t"]), str(payload["r"])
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError, KeyError, ValueError) as exc:
        raise ValueError(f"malformed cursor: {cursor!r}") from exc


TIMELINE_SQL = """
with events as (

    select
        placed_at_utc                       as occurred_at,
        'order'                             as event_category,
        'order_placed'                      as event_type,
        order_id                            as reference_id,
        amount_cents,
        currency_code,
        'status=' || order_status || ', channel=' || order_channel as detail
    from marts.fct_orders
    where customer_id = ?

    union all

    select
        coalesce(processed_at, created_at)  as occurred_at,
        'payment'                           as event_category,
        'payment_' || payment_status        as event_type,
        payment_id                          as reference_id,
        gross_amount_cents                  as amount_cents,
        null                                as currency_code,
        'method=' || payment_method         as detail
    from marts.fct_payments
    where customer_id = ?

    union all

    select
        last_refund_issued_at               as occurred_at,
        'refund'                            as event_category,
        'refund_issued'                     as event_type,
        payment_id                          as reference_id,
        refunded_amount_cents               as amount_cents,
        null                                as currency_code,
        'refunds=' || cast(refund_count as varchar) as detail
    from marts.fct_payments
    where customer_id = ? and refund_count > 0

    union all

    select
        occurred_at,
        'subscription'                      as event_category,
        'subscription_' || event_type       as event_type,
        cast(subscription_event_id as varchar) as reference_id,
        mrr_delta_cents                     as amount_cents,
        null                                as currency_code,
        coalesce(from_plan_code, 'none') || ' -> ' || coalesce(to_plan_code, 'none') as detail
    from marts.fct_subscription_events
    where customer_id = ?

)
select *
from events
where occurred_at is not null
  {cursor_predicate}
order by occurred_at desc, reference_id desc
limit ?
"""


def build_timeline_query(
    cursor: tuple[datetime, str] | None, customer_id: str, limit: int
) -> tuple[str, list[Any]]:
    """Build the keyset query. Fetches limit+1 to detect a further page."""
    params: list[Any] = [customer_id] * 4
    if cursor is None:
        predicate = ""
    else:
        # Row-value comparison, so the tiebreaker is applied correctly. Writing
        # this as `occurred_at < ? OR (occurred_at = ? AND reference_id < ?)`
        # is equivalent but is the form people get subtly wrong.
        predicate = "and (occurred_at, reference_id) < (?, ?)"
        params.extend([cursor[0], cursor[1]])
    params.append(limit + 1)
    return TIMELINE_SQL.format(cursor_predicate=predicate), params
