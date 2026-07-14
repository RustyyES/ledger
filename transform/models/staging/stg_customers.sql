{{
    config(
        materialized='view',
        tags=['staging', 'core']
    )
}}

/*
    Customer master, reduced to one row per customer.

    `include_deletes=true` is deliberate. A soft-deleted customer is still the
    customer who placed every order in their history; dropping them here would
    orphan those orders and silently shrink historical revenue. The delete is
    surfaced as a flag instead and the WAREHOUSE decides what it means --
    which, per DESIGN.md, is: keep their orders, exclude them from active-
    customer counts, retain them in dim_customer.

    Note there is no `where _op != 'd'` here. That is the exception the layer
    contract asks to be justified explicitly, and this comment is the
    justification.
*/

with source as (

    select * from {{ raw_source('customers') }}

),

latest as (

    {{ cdc_latest('source', 'id', include_deletes=true) }}

),

renamed as (

    select
        cast(id as varchar)                         as customer_id,
        lower(trim(email))                          as email,
        trim(name)                                  as customer_name,
        upper(trim(country_code))                   as country_code,

        -- Timezone is used downstream to resolve naive local order timestamps.
        -- Rows created before the column existed carry 'UTC' from the server
        -- default; `has_explicit_timezone` lets an analyst tell "actually UTC"
        -- from "we do not know".
        coalesce(nullif(trim(timezone), ''), 'UTC') as timezone,
        (
            nullif(trim(timezone), '') is not null
            and trim(timezone) != 'UTC'
        )                                           as has_explicit_timezone,

        cast(created_at as timestamp)               as created_at,
        cast(updated_at as timestamp)               as updated_at,
        cast(deleted_at as timestamp)               as deleted_at,
        (deleted_at is not null)                    as is_deleted,

        -- A hard delete at source would arrive as _op='d'. It should never
        -- happen -- the application only soft-deletes -- so surfacing it
        -- separately from `is_deleted` makes an unexpected one visible rather
        -- than conflating it with the expected case.
        ({{ adapter.quote('_op') }} = 'd')          as is_hard_deleted,

        {{ adapter.quote('_op') }}                  as _op,
        {{ adapter.quote('_lsn') }}                 as _lsn,
        {{ adapter.quote('_ingested_at') }}         as _ingested_at,
        {{ adapter.quote('_source_ts') }}           as _source_ts

    from latest

)

select * from renamed
