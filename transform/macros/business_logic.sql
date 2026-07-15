{#
    Business transformations shared across layers.

    Everything here is a rule the BUSINESS owns, not a SQL convenience. Each one
    is defined exactly once so that "what counts as a completed order" has a
    single answer, and changing that answer is a one-line diff with a blast
    radius the lineage graph can show you.
#}

{#
    Normalise `orders.status`.

    The source table carries 'paid', 'PAID', 'complete', 'Completed' and
    'completed' -- the residue of a data migration that was never finished. All
    five mean the same thing. This is the ONLY place that knowledge lives; every
    downstream model reads the normalised column.

    An unrecognised value maps to 'unknown' rather than passing through. Passing
    it through would let a new legacy spelling silently create a sixth status
    that every `group by status` splits on. `unknown` is loud: the
    accepted_values test on the normalised column fails immediately.
#}
{% macro normalise_order_status(column) %}
    case lower(trim({{ column }}))
        when 'paid'       then 'completed'
        when 'complete'   then 'completed'
        when 'completed'  then 'completed'
        when 'pending'    then 'pending'
        when 'cancelled'  then 'cancelled'
        when 'canceled'   then 'cancelled'
        when 'refunded'   then 'refunded'
        else 'unknown'
    end
{% endmacro %}


{#
    Resolve an order's placed-at into UTC.

    15% of orders arrive from a legacy mobile client that writes naive local
    wall-clock into `placed_at_local` and leaves `placed_at` null. Resolving it
    requires the customer's IANA timezone, which is why this cannot happen in
    staging -- the layer contract forbids joins there. It runs in
    `int_orders__resolved`.

    Two edge cases that a naive implementation gets wrong:

      * A local time inside a DST spring-forward gap does not exist. DuckDB and
        Snowflake both resolve it forward rather than erroring, which is the
        behaviour we want, but it must be a deliberate choice rather than an
        accident.
      * Falling back to UTC when the timezone is unknown is WRONG by up to 14
        hours. We fall back to UTC anyway because the alternative is dropping
        the order, but the `is_timestamp_inferred` flag marks every such row so
        an analyst can exclude them and `assert_no_future_dated_orders` can
        catch the worst of it.
#}
{% macro resolve_placed_at_utc(placed_at, placed_at_local, timezone_col) %}
    coalesce(
        {{ placed_at }},
        case
            when {{ placed_at_local }} is null then null
            when {{ timezone_col }} is null
                then cast({{ placed_at_local }} as timestamp) at time zone 'UTC'
            else cast({{ placed_at_local }} as timestamp) at time zone {{ timezone_col }}
        end
    )
{% endmacro %}


{#
    Convert an amount to USD cents using the rate in force ON THE ORDER DATE.

    Using today's rate to restate historical revenue means last quarter's
    numbers change every morning. Finance notices. This is why `seed_fx_rates`
    is a dated table and why the join is on the date, not a scalar lookup.
#}
{% macro to_usd_cents(amount_column, currency_column, rate_column) %}
    cast(
        round(
            case
                when {{ currency_column }} = 'USD' then {{ amount_column }}
                else {{ amount_column }} / nullif({{ rate_column }}, 0)
            end
        ) as bigint
    )
{% endmacro %}


{#
    Days in the month containing a date. Used by the MRR proration.

    Calendar-day proration, not 1/12-of-annual: a customer who upgrades on
    15 February contributes 14/28 of February, not 14/30. Finance defines this;
    changing it silently moves reported MRR by up to 3%.
#}
{% macro days_in_month(date_column) %}
    {%- if target.type == 'duckdb' -%}
        date_diff('day',
            date_trunc('month', {{ date_column }}),
            date_trunc('month', {{ date_column }}) + interval '1 month'
        )
    {%- else -%}
        datediff('day',
            date_trunc('month', {{ date_column }}),
            dateadd('month', 1, date_trunc('month', {{ date_column }}))
        )
    {%- endif -%}
{% endmacro %}


{#
    Surrogate key. Thin wrapper over dbt_utils so the hashing strategy is one
    edit away from being changed everywhere.
#}
{% macro ledger_surrogate_key(field_list) %}
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}


{#
    The date surrogate key. ALWAYS use this, never hash a date by hand.

    ---------------------------------------------------------------------------
    A BUG THIS MACRO EXISTS TO PREVENT, because it already happened once.

    `dim_date` hashed its spine column directly; `fct_orders` hashed
    `cast(placed_at_utc as date)`. Both "obviously" produce the key for the same
    day. They did not: the spine column was a TIMESTAMP, so dbt_utils stringified
    it as '2026-05-11 00:00:00' while the fact stringified '2026-05-11'. Every
    single one of the 20,247 date_key foreign keys was orphaned.

    Nothing errored. The join simply matched nothing, and every metric joined
    through `dim_date` would have silently returned empty.

    The lesson is not "remember to cast". It is that a key defined in two places
    is a key with two definitions. One macro, one cast, one answer.
    ---------------------------------------------------------------------------
#}
{% macro date_key(date_expression) %}
    {{ dbt_utils.generate_surrogate_key(['cast(' ~ date_expression ~ ' as date)']) }}
{% endmacro %}


{#
    "Now" for the warehouse.

    Never `current_timestamp` directly in a model. A backfill re-running
    2025-03-01 must produce what the original run produced, and a model that
    reads the wall clock cannot. `DBT_RUN_AS_OF` pins it; the checksum proof in
    `make backfill-proof` depends on this being honoured everywhere.
#}
{% macro warehouse_now() %}
    {%- if var('run_as_of') -%}
        cast('{{ var("run_as_of") }}' as timestamp)
    {%- else -%}
        current_timestamp
    {%- endif -%}
{% endmacro %}
