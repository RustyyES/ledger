{% snapshot customer_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True,
        tags=['snapshot']
    )
}}

/*
    SCD Type 2 history for customers.

    ---------------------------------------------------------------------------
    WHY THIS IS THE MOST VALUABLE MODEL IN THE PROJECT.

    A customer in Egypt moves to Germany. Their `country_code` changes in place
    in the source -- Postgres keeps no history. Without a snapshot, every
    historical order they ever placed is now attributed to Germany, and last
    year's revenue-by-country report silently changes. Run the same report twice
    six months apart and the numbers differ, with no code change and no bug to
    point at.

    That is not a hypothetical: `services/loadgen/profiles.py` relocates ~3% of
    customers a year specifically so this model has work to do and the tests
    can prove it does it.
    ---------------------------------------------------------------------------

    STRATEGY: `timestamp`, not `check`.

    `check` diffs a column list every run. It is the right choice when the
    source has no reliable modification timestamp. Here it would be strictly
    worse: it costs a full-table comparison on every snapshot run, and it
    cannot see a change that was reverted between two runs.

    `timestamp` relies on `updated_at` moving on EVERY mutation. That is an
    assumption about the application, and it is load-bearing -- a route that
    forgets to touch `updated_at` produces a version that is never recorded,
    silently. `commerce-api/tests/test_customers.py::test_patch_moves_updated_at`
    is the test that holds up this end of the contract, and this comment is here
    so that whoever breaks it can find out why it mattered.

    `invalidate_hard_deletes=True` closes the version of a row that disappears
    from the source entirely. The application only soft-deletes, so this should
    never fire -- but if a DBA ever runs a real DELETE, the alternative is a
    version that stays `is_current` forever.
*/

select
    customer_id,
    email,
    customer_name,
    country_code,
    timezone,
    has_explicit_timezone,
    created_at,
    updated_at,
    deleted_at,
    is_deleted
from {{ ref('stg_customers') }}

{% endsnapshot %}
