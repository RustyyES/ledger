/*
    A refund can never exceed the payment it reverses.

    The API enforces this under a row lock (`payments.py::create_refund`), so a
    violation here means one of three things, all worth knowing about:

      1. Someone wrote to the database directly, bypassing the API.
      2. The CDC dedup picked the wrong version of a payment row, so the
         warehouse is comparing a refund against a stale amount.
      3. The refund aggregation is joining incorrectly and summing another
         payment's refunds into this one.

    (3) is the realistic one, and it is exactly the kind of fan-out bug that
    every other test in the suite passes straight through: the keys stay unique,
    the foreign keys still resolve, the totals are just too big.
*/

select
    payment_id,
    order_id,
    gross_amount_cents,
    refunded_amount_cents,
    refund_count,
    refunded_amount_cents - gross_amount_cents as excess_cents
from {{ ref('fct_payments') }}
where refunded_amount_cents > gross_amount_cents
