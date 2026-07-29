{{ config(materialized='table', tags=['marts', 'finance', 'metric']) }}

/*
    Rolling revenue windows with gap fill. Grain: one row per day.

    ===========================================================================
    THE TRAP THIS EXISTS TO DEMONSTRATE -- and the one interview screens test.

    The obvious implementation:

        select order_date,
               sum(net) over (order by order_date rows between 6 preceding and current row)
        from daily_revenue

    On a dataset with no zero-order days it looks right. The moment a day has
    no orders -- Christmas week, an outage, a small market's first month --
    that day has NO ROW. And `rows between 6 preceding` counts ROWS, not DAYS.

    So a 7-day rolling window that spans a 3-day gap silently averages over
    10 calendar days. The number is not obviously wrong; it is just quietly
    smoothed, and every anomaly you were trying to detect is the thing it
    smooths away.

    `range between interval '6 days' preceding` fixes the arithmetic in engines
    that support it, but still emits no ROW for the missing day, so a chart
    drawn from it draws a straight line across the gap instead of a dip to zero.

    The fix is structural, not syntactic: LEFT JOIN from `dim_date` FIRST, so
    every calendar day exists with a zero, and only THEN apply the window.
    That is the whole reason `dim_date` is generated rather than derived from
    the facts -- a spine built from the data has gaps in exactly the places
    that matter.
    ===========================================================================
*/

with spine as (

    select
        date_day,
        year_month,
        is_weekend,
        is_holiday,
        holiday_name
    from {{ ref('dim_date') }}
    where
        date_day >= (select min(order_date) from {{ ref('fct_orders') }})
        and date_day <= cast({{ warehouse_now() }} as date)

),

daily_actuals as (

    select
        order_date,
        count(*)                       as order_count,
        count(distinct customer_id)    as customer_count,
        sum(gross_amount_usd_cents)    as gross_revenue_usd_cents,
        sum(refunded_amount_usd_cents) as refunded_usd_cents,
        sum(net_amount_usd_cents)      as net_revenue_usd_cents
    from {{ ref('fct_payments') }}
    where is_successful
    group by 1

),

-- THE GAP FILL. This LEFT JOIN is the whole point of the model.
gap_filled as (

    select
        spine.date_day,
        spine.year_month,
        spine.is_weekend,
        spine.is_holiday,
        spine.holiday_name,

        coalesce(daily_actuals.order_count, 0)             as order_count,
        coalesce(daily_actuals.customer_count, 0)          as customer_count,
        coalesce(daily_actuals.gross_revenue_usd_cents, 0) as gross_revenue_usd_cents,
        coalesce(daily_actuals.refunded_usd_cents, 0)      as refunded_usd_cents,
        coalesce(daily_actuals.net_revenue_usd_cents, 0)   as net_revenue_usd_cents,

        -- Distinguishes "genuinely zero" from "filled". A day with no orders
        -- is real information; hiding that it was interpolated is not.
        (daily_actuals.order_date is null)                 as is_gap_filled_day

    from spine
    left join daily_actuals on spine.date_day = daily_actuals.order_date

),

-- Only NOW is the window safe: every calendar day has exactly one row.
windowed as (

    select
        gap_filled.*,

        sum(gap_filled.net_revenue_usd_cents)
            over (
                order by gap_filled.date_day rows between 6 preceding and current row
            )
            as rolling_7d_net_usd_cents,
        avg(gap_filled.net_revenue_usd_cents)
            over (
                order by gap_filled.date_day rows between 6 preceding and current row
            )
            as rolling_7d_avg_net_usd_cents,
        sum(gap_filled.net_revenue_usd_cents)
            over (
                order by gap_filled.date_day rows between 27 preceding and current row
            )
            as rolling_28d_net_usd_cents,
        sum(gap_filled.order_count)
            over (
                order by gap_filled.date_day rows between 6 preceding and current row
            )
            as rolling_7d_order_count,

        -- Week-over-week on the same weekday, so a Monday is compared with a
        -- Monday. Comparing to "7 rows ago" only equals that because the spine
        -- guarantees one row per day -- another thing the gap fill buys.
        lag(gap_filled.net_revenue_usd_cents, 7) over (order by gap_filled.date_day)
            as net_usd_cents_7d_ago

    from gap_filled

)

select
    {{ date_key('date_day') }}    as date_key,
    windowed.*,
    case
        when windowed.net_usd_cents_7d_ago is null or windowed.net_usd_cents_7d_ago = 0 then null
        else round(
            (windowed.net_revenue_usd_cents - windowed.net_usd_cents_7d_ago) * 100.0
            / windowed.net_usd_cents_7d_ago, 2
        )
    end                        as wow_change_pct
from windowed
