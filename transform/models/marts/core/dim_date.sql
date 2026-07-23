{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

/*
    Generated date dimension with fiscal periods and holiday flags.

    Generated rather than derived from the fact tables, on purpose. A dimension
    built from `select distinct order_date from fct_orders` has GAPS -- exactly
    on the days with no orders, which are the days you most need to see as zero
    rather than as missing. Gap-filling rolling revenue is impossible without a
    complete spine, and it is the specific trap `fct_revenue_rolling` exists to
    demonstrate.

    Fiscal year starts in February, which is arbitrary but deliberate: a fiscal
    calendar that happens to equal the Gregorian one lets an off-by-one hide.
*/

{% set fiscal_year_start_month = 2 %}

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('date_spine_start') ~ "' as date)",
        end_date="cast('" ~ var('date_spine_end') ~ "' as date)"
    ) }}

),

holidays as (

    select
        holiday_date,
        holiday_name,
        affects_volume
    from {{ ref('seed_holidays') }}
    where country_code = 'GLOBAL'

),

enriched as (

    select
        cast(date_day as date)                        as date_day,
        {{ date_key('date_day') }}                      as date_key,

        extract(year from date_day)                   as calendar_year,
        extract(quarter from date_day)                as calendar_quarter,
        extract(month from date_day)                  as calendar_month,
        extract(day from date_day)                    as day_of_month,
        extract(dayofweek from date_day)              as day_of_week,
        extract(doy from date_day)                    as day_of_year,
        extract(week from date_day)                   as iso_week,

        strftime(date_day, '%Y-%m')                   as year_month,
        strftime(date_day, '%A')                      as day_name,
        strftime(date_day, '%B')                      as month_name,

        cast(date_trunc('month', date_day) as date)   as month_start_date,
        cast(date_trunc('quarter', date_day) as date) as quarter_start_date,
        cast(date_trunc('year', date_day) as date)    as year_start_date,
        cast(date_trunc('week', date_day) as date)    as week_start_date,

        -- Needed by the MRR proration: a customer who upgrades on 15 February
        -- contributes 14/28 of February, not 14/30.
        {{ days_in_month('date_day') }}               as days_in_month,

        (extract(dayofweek from date_day) in (0, 6))  as is_weekend,

        -- Fiscal calendar, February start.
        case
            when extract(month from date_day) >= {{ fiscal_year_start_month }}
                then extract(year from date_day)
            else extract(year from date_day) - 1
        end                                           as fiscal_year,
        -- floor division, not `/`. In DuckDB and Snowflake `/` on integers is
        -- FLOAT division, which silently produced 1.0, 1.333, 1.667, 2.0 ...
        -- i.e. twelve distinct "quarters". The accepted_values test caught it.
        cast(floor(
            (
                (
                    cast(extract(month from date_day) as integer)
                    - {{ fiscal_year_start_month }} + 12
                ) % 12
            ) / 3
        ) as integer) + 1                             as fiscal_quarter,
        cast(
            (
                (
                    cast(extract(month from date_day) as integer)
                    - {{ fiscal_year_start_month }} + 12
                ) % 12
            ) + 1
            as integer
        )                                             as fiscal_month

    from spine

)

select
    enriched.*,
    (holidays.holiday_date is not null)                        as is_holiday,
    holidays.holiday_name,
    coalesce(holidays.affects_volume, false)                   as holiday_affects_volume,
    -- Rolling-window models join on this to know whether a day is in the past.
    (enriched.date_day <= cast({{ warehouse_now() }} as date)) as is_past
from enriched
left join holidays on enriched.date_day = holidays.holiday_date
