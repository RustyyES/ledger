{#
    Custom schema naming.

    dbt's default prepends the target schema to every custom schema, producing
    `analytics_staging`, `analytics_marts` and so on. That is right for a shared
    warehouse where several developers build into one database and need
    isolation. It is wrong here: the mart names ARE the public contract, and a
    consumer querying `marts.fct_orders` should not have to know which target
    built it.

    So: in dev and ci, use the custom schema verbatim. In prod, keep dbt's
    prefixing behaviour, because prod builds into a database where the target
    schema is the isolation boundary between the deployed project and anything
    a human is experimenting with.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name in ('dev', 'ci') -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
