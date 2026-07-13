{#
    Helpers for reading the raw CDC layer.

    The raw layer is an append-only log of what the source said. Every table
    contains multiple rows per primary key -- one per change -- plus the
    original bulk-export snapshot. Turning that back into "the current state of
    each row" is the single most repeated operation in staging, so it lives
    here rather than being copy-pasted seven times.
#}

{#
    Order CDC versions of the same key, newest last.

    The ordering has to cope with two record sources in one stream:

      * bulk export rows  -- `_lsn` is NULL, `_kafka_partition` = -1, and their
        `_kafka_offset` is a synthetic row number in its own space.
      * CDC rows          -- real `_lsn`, real Kafka coordinates.

    `coalesce(_lsn, 0)` puts every bulk row below every CDC row, which is
    correct: the snapshot is by construction the oldest state we have. Within
    CDC, `_lsn` is the authoritative Postgres commit order -- NOT `_source_ts`,
    which has millisecond resolution and ties on high-throughput tables, and
    NOT `_kafka_offset`, which is only ordered within one Kafka partition.
#}
{% macro cdc_version_order() %}
    coalesce({{ adapter.quote('_lsn') }}, 0) asc,
    {{ adapter.quote('_source_ts') }} asc nulls first,
    {{ adapter.quote('_kafka_partition') }} asc,
    {{ adapter.quote('_kafka_offset') }} asc
{% endmacro %}


{#
    Keep only the latest version of each key.

    `include_deletes` controls whether a key whose final state is a delete
    survives. Default is false -- most staging models want live rows -- but
    `stg_customers` passes true, because a soft-deleted customer is still a
    customer and their order history must remain resolvable.
#}
{% macro cdc_latest(relation_alias, key_column, include_deletes=false) %}
    select *
    from (
        select
            {{ relation_alias }}.*,
            row_number() over (
                partition by {{ relation_alias }}.{{ key_column }}
                order by {{ cdc_version_order() }} desc
            ) as _version_rank
        from {{ relation_alias }}
    )
    where _version_rank = 1
    {%- if not include_deletes %}
      and {{ adapter.quote('_op') }} != 'd'
    {%- endif %}
{% endmacro %}


{#
    The incremental predicate. THE most important macro in this project.

    Filtering on the business timestamp (`placed_at`, `issued_at`) is the
    intuitive thing to write and it is wrong. A refund issued today against an
    order placed twelve days ago has a business timestamp far outside any
    sensible window, so it never enters the incremental run -- and the affected
    fact row stays wrong forever. Nothing errors. No test that looks only at
    today's data catches it.

    Filtering on `_ingested_at` -- when the record entered the PIPELINE, not
    when the event happened in the business -- fixes it, because a late-arriving
    fact is by definition recently ingested however old it is.

    The lookback must exceed the maximum arrival delay
    (`max_refund_delay_days`). `assert_no_late_arrival_outside_lookback.sql`
    fails the build if reality ever exceeds the configured window.
#}
{% macro incremental_lookback(column='_ingested_at', this_column=none) %}
    {%- set this_col = this_column or column -%}
    {%- if is_incremental() %}
        {{ column }} > (
            select coalesce(max({{ this_col }}), '1970-01-01'::timestamp)
                   - interval '{{ var("incremental_lookback_days") }} days'
            from {{ this }}
        )
    {%- else %}
        1 = 1
    {%- endif %}
{% endmacro %}


{#
    Read a raw CDC table.

    Indirection so that swapping DuckDB-over-Parquet for a Snowflake external
    table is a change in ONE place rather than in seven staging models.
#}
{% macro raw_source(table_name) %}
    {%- if target.type == 'duckdb' -%}
        read_parquet('{{ var("raw_path") }}/{{ table_name }}/**/*.parquet',
                     hive_partitioning = true,
                     union_by_name = true)
    {%- else -%}
        {{ source('ledger_raw', table_name) }}
    {%- endif -%}
{% endmacro %}


{#
    Introspect the columns actually present in a raw Parquet prefix.

    Cached per compile in `graph`-independent module state, because
    `DESCRIBE SELECT *` over a whole prefix is a real query and seven staging
    models asking the same question seven times is seven scans.
#}
{% macro raw_columns(table_name) %}
    {%- if not execute -%}
        {{ return([]) }}
    {%- endif -%}

    {%- set cache_key = 'raw_columns_' ~ table_name -%}
    {%- if cache_key in graph.get('_ledger_cache', {}) -%}
        {{ return(graph['_ledger_cache'][cache_key]) }}
    {%- endif -%}

    {%- set query -%}
        describe select * from {{ raw_source(table_name) }} limit 0
    {%- endset -%}

    {%- set results = [] -%}
    {%- if target.type == 'duckdb' -%}
        {%- set rows = run_query(query) -%}
        {%- if rows is not none -%}
            {%- set results = rows.columns[0].values() | list -%}
        {%- endif -%}
    {%- else -%}
        {%- set relation = source('ledger_raw', table_name) -%}
        {%- set results = adapter.get_columns_in_relation(relation) | map(attribute='name') | list -%}
    {%- endif -%}

    {%- if '_ledger_cache' not in graph -%}
        {%- do graph.update({'_ledger_cache': {}}) -%}
    {%- endif -%}
    {%- do graph['_ledger_cache'].update({cache_key: results}) -%}
    {{ return(results) }}
{% endmacro %}


{#
    Select a raw column that MAY NOT EXIST YET, falling back to a literal.

    This is what lets a staging model survive an additive migration in both
    directions. `orders.channel` is added by migration 0003 mid-project; before
    it is applied, no Parquet file has the column, and referencing it is a
    BINDER error -- `try_cast` does not help, because the failure is name
    resolution, not casting.

    Reading the Parquet schema at compile time and substituting `null` when the
    column is absent means:

      * the model builds today, against history that predates the column;
      * it builds tomorrow, against files that have it;
      * and it builds during the transition, when the prefix contains both --
        which is the case that actually breaks naive implementations, because
        `union_by_name` resolves it at read time only if at least one file has
        the column.

    The alternative -- gating the model behind a var an operator flips on
    migration day -- works right up until somebody forgets to flip it.
#}
{% macro raw_column(table_name, column_name, fallback='null') %}
    {%- if column_name in raw_columns(table_name) -%}
        {{ adapter.quote(column_name) }}
    {%- else -%}
        {{ fallback }}
    {%- endif -%}
{% endmacro %}
