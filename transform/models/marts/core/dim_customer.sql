{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension', 'scd2']
    )
}}

/*
    Customer dimension, SCD Type 2. One row per customer per VERSION.

    Reads the dbt snapshot rather than `stg_customers`, because the snapshot is
    the thing that has history; staging only ever has the current state.

    ---------------------------------------------------------------------------
    HOW TO USE THIS CORRECTLY -- the part that gets misused.

    Joining a fact to this dimension on `customer_id` alone MULTIPLIES the fact
    by the number of versions. Every join must be one of:

      * "as-of" join, for historical attribution:
            on  f.customer_id = d.customer_id
            and f.placed_at_utc >= d.valid_from
            and f.placed_at_utc <  d.valid_to_or_infinity

      * "current" join, for "who is this customer NOW":
            on f.customer_id = d.customer_id and d.is_current

    `fct_orders` resolves `customer_key` as-of `placed_at_utc`, so downstream
    consumers can join on the surrogate key alone and get the historically
    correct version without having to know any of this.
    ---------------------------------------------------------------------------
*/

with snapshotted as (

    select * from {{ ref('customer_snapshot') }}

),

versioned as (

    select
        {{ ledger_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_key,
        customer_id,

        email,
        customer_name,
        country_code,
        timezone,
        has_explicit_timezone,

        created_at,
        updated_at,
        deleted_at,
        is_deleted,

        -- ------------------------------------------------------------------
        -- THE FIRST-VERSION PROBLEM.
        --
        -- `dbt_valid_from` on version 1 is the moment the SNAPSHOT first saw
        -- the row -- which is when the snapshot was first run, not when the
        -- customer came into existence. Every order placed before that instant
        -- falls outside all versions, the as-of join in `fct_orders` yields
        -- NULL, and those orders silently vanish from every dimensional
        -- aggregate while remaining in the fact table.
        --
        -- It is a large effect, not an edge case: 4,550 of 20,247 orders --
        -- 22% of revenue -- were orphaned this way before this fix, because
        -- the entire 18-month backfill predates the first snapshot run.
        --
        -- The fix is to back-date version 1 to the customer's own `created_at`.
        -- A customer cannot have placed an order before they existed, so this
        -- is exactly the right lower bound -- not an approximation.
        -- ------------------------------------------------------------------
        dbt_valid_from                                                   as valid_from,
        dbt_valid_to                                                     as valid_to,
        -- An explicit far-future sentinel makes the as-of BETWEEN join work
        -- without a `coalesce` at every call site -- and a forgotten coalesce
        -- silently drops every fact that falls in a row's current version.
        coalesce(dbt_valid_to, cast('9999-12-31 23:59:59' as timestamp))
            as valid_to_or_infinity,
        (dbt_valid_to is null)                                           as is_current,

        row_number() over (
            partition by customer_id order by dbt_valid_from
        )                                                                as version_number,
        count(*) over (partition by customer_id)                         as total_versions

    from snapshotted

),

final as (

    select
        versioned.*,

        -- True only for customers who have actually changed country. Small in
        -- number, disproportionate in impact -- these are the rows that make
        -- revenue-by-country stable, and the ones to look at first when
        -- somebody says "last year's numbers moved".
        (
            count(distinct versioned.country_code) over (partition by versioned.customer_id) > 1
        )
            as has_relocated,

        date_diff(
            'day', versioned.valid_from,
            coalesce(versioned.valid_to, cast({{ warehouse_now() }} as timestamp)))
            as version_duration_days

    from versioned

)

select * from final
