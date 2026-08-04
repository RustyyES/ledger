-- noqa: disable=LT02
--
-- Indentation rules are disabled for THIS FILE ONLY. It is the one model whose
-- body sits inside a Jinja conditional, and sqlfluff's dbt templater renders
-- the inactive branch away before linting -- so the surviving lines look
-- over-indented relative to a block that is no longer there. Re-indenting to
-- satisfy the linter would make the Jinja structure unreadable, which is the
-- wrong trade. Every other rule still applies here.
--
-- (Note the wording above avoids writing Jinja delimiters literally: a comment
-- is not a comment to the templater, and an unclosed tag inside one is a
-- compilation error before any SQL is parsed.)

{{
    config(
        materialized='table',
        schema='ops',
        alias='schema_changes',
        tags=['ops']
    )
}}

/*
    Schema change audit, surfaced from the sink's JSON log into a queryable table.

    The CDC sink writes one JSON file per batch that contained a schema change,
    under `_schema_changes/_ingested_date=.../`. That is the right place for the
    sink to write it -- the sink has an object store and no warehouse -- but it
    is the wrong place for anyone to READ it, which is what this model fixes.

    Materialised as a table rather than a view because it is small, it is read by
    a dashboard on a refresh timer, and a view would re-glob the whole prefix on
    every page load.

    The model tolerates the prefix being empty: on a fresh install no schema
    change has happened yet, and a dashboard panel that errors because nothing
    has gone wrong is worse than one that says "nothing yet".
*/

{% set changes_glob = var('raw_path') ~ '/_schema_changes/**/*.json' %}

{% if target.type == 'duckdb' %}

    {#- Probe first. read_json over a non-existent prefix is an error, not an
        empty result, and this model must build on a fresh warehouse. -#}
    {% set probe %}
        select count(*) as n from glob('{{ changes_glob }}')
    {% endset %}

    {% if execute %}
        {% set found = run_query(probe) %}
        {% set file_count = found.columns[0].values()[0] if found else 0 %}
    {% else %}
        {% set file_count = 0 %}
    {% endif %}

    {% if file_count > 0 %}

        select
            cast(detected_at as timestamp)  as detected_at,
            cast("table" as varchar)        as "table",
            cast("column" as varchar)       as column_name,
            cast(change as varchar)         as change,
            cast(from_type as varchar)      as from_type,
            cast(to_type as varchar)        as to_type,
            -- Surfaced so the dashboard can colour-code without re-deriving the
            -- policy. Kept in sync with schema_guard.ChangeClass by
            -- `assert_schema_change_classes_are_known`.
            case cast(change as varchar)
                when 'incompatible' then 'halted this table'
                when 'dropped'      then 'accepted, backfilled null'
                when 'widening'     then 'accepted'
                when 'additive'     then 'accepted'
                else 'unknown'
            end                             as outcome
        from read_json_auto('{{ changes_glob }}', union_by_name = true)

    {% else %}

        {#- Empty, but correctly typed, so downstream and the dashboard bind. -#}
        select
            cast(null as timestamp) as detected_at,
            cast(null as varchar)   as table,
            cast(null as varchar)   as column_name,
            cast(null as varchar)   as change,
            cast(null as varchar)   as from_type,
            cast(null as varchar)   as to_type,
            cast(null as varchar)   as outcome
        where false

    {% endif %}

{% else %}

    select
        detected_at, "table", "column" as column_name, change, from_type, to_type,
        case change
            when 'incompatible' then 'halted this table'
            when 'dropped'      then 'accepted, backfilled null'
            when 'widening'     then 'accepted'
            when 'additive'     then 'accepted'
            else 'unknown'
        end as outcome
    from {{ source('ledger_raw', 'schema_changes') }}

{% endif %}
