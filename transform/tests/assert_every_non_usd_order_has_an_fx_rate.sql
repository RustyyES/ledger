/*
    Every non-USD order must convert.

    A missing FX rate makes `amount_usd_cents` NULL, and `sum()` skips nulls
    silently. The consequence is that total revenue simply omits the affected
    orders -- roughly 20% of the book is non-USD -- and the number that comes
    back looks entirely reasonable. It is just smaller than the truth, by an
    amount nobody can see without knowing to look.

    This is why `seed_fx_rates` carries an explicit USD -> USD row at 1.0: it
    means "no rate" is unambiguously an error rather than an implicit identity,
    and this test can be a strict equality rather than a judgement call.
*/

select
    order_id,
    customer_id,
    currency_code,
    amount_cents,
    placed_at_utc,
    cast(date_trunc('month', placed_at_utc) as date) as missing_rate_month
from {{ ref('fct_orders') }}
where is_fx_rate_missing
   or (currency_code != 'USD' and amount_usd_cents is null)
