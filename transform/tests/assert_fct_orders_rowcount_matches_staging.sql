/*
    The order fact must contain exactly as many rows as staging.

    A row-count check reads like the most trivial test in the suite. It is not:
    it is the only one that catches the two failure modes that leave every other
    test green.

      * LOSS. An inner join written where a left join was meant silently drops
        every order whose dimension lookup missed. Uniqueness holds, foreign
        keys hold, nothing is null -- there are simply fewer rows, and no test
        that examines rows can see rows that are not there.

      * FAN-OUT. Joining `dim_customer` on `customer_id` without an as-of or
        `is_current` predicate multiplies each order by its customer's version
        count. `unique(order_id)` catches this one, but only because the grain
        is a single column; on a composite grain it would not.

    Zero tolerance. Both models read the same raw prefix at the same instant
    within a single dbt invocation, so any difference is a bug in the model and
    not a race with the sink.
*/

with staging_count as (
    select count(*) as n from {{ ref('stg_orders') }}
),

fact_count as (
    select count(*) as n from {{ ref('fct_orders') }}
)

select
    staging_count.n as staging_rows,
    fact_count.n    as fact_rows,
    fact_count.n - staging_count.n as delta,
    case
        when fact_count.n < staging_count.n then 'rows lost -- suspect an inner join'
        else 'rows gained -- suspect a dimension fan-out'
    end as likely_cause
from staging_count, fact_count
where staging_count.n != fact_count.n
