/*
    MRR must be non-negative and must not move implausibly day to day.

    The bounds check is trivial and would never fire on its own. The DAY-OVER-
    DAY check is the useful half: a plan-period reconstruction bug does not
    usually produce a negative number, it produces a CLIFF -- MRR halving
    overnight because an interval's `valid_to` was computed as its own
    `valid_from`, or doubling because a `paused` interval was counted as
    earning.

    A real subscription business does not move 40% of its book in a day. A
    modelling bug does, and this is the shape it takes.

    The first day of the series is excluded: it has no predecessor, and the
    ramp-in at the start of history is not a defect.
*/

with daily as (

    select
        date_day,
        mrr_cents,
        lag(mrr_cents) over (order by date_day) as prev_mrr_cents
    from {{ ref('fct_mrr_daily') }}

)

select
    date_day,
    mrr_cents,
    prev_mrr_cents,
    round(abs(mrr_cents - prev_mrr_cents) * 100.0 / nullif(prev_mrr_cents, 0), 2) as pct_move,
    case
        when mrr_cents < 0 then 'negative MRR'
        else 'implausible day-over-day move'
    end as defect
from daily
where mrr_cents < 0
   or (
        prev_mrr_cents is not null
        and prev_mrr_cents > 100000                      -- ignore the ramp-in
        and abs(mrr_cents - prev_mrr_cents) > prev_mrr_cents * 0.40
      )
