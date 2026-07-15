{#
    Reusable generic tests.

    These are the checks that are business rules rather than schema facts, and
    that apply to more than one model.
#}

{#
    Assert a column is non-null only for rows matching a condition.

    The blanket `not_null` test on `payments.processed_at` is WRONG: a pending
    payment legitimately has none. Scoping the test is the difference between a
    suite that catches real problems and one that is permanently red and
    therefore ignored.
#}
{% test not_null_where(model, column_name, condition) %}
    select *
    from {{ model }}
    where ({{ condition }})
      and {{ column_name }} is null
{% endtest %}


{#
    Assert a numeric column stays within bounds.
#}
{% test between(model, column_name, min_value, max_value, where_clause='1=1') %}
    select *
    from {{ model }}
    where ({{ where_clause }})
      and (
            {{ column_name }} < {{ min_value }}
         or {{ column_name }} > {{ max_value }}
      )
{% endtest %}


{#
    Assert an SCD2 dimension has no overlapping validity windows for a key.

    Generic rather than singular because the moment there is a second SCD2
    dimension, the copy-paste starts.
#}
{% test scd2_no_overlaps(model, natural_key, valid_from='valid_from', valid_to='valid_to') %}
    with versions as (
        select
            {{ natural_key }} as natural_key,
            {{ valid_from }}  as valid_from,
            coalesce({{ valid_to }}, cast('9999-12-31' as timestamp)) as valid_to,
            lead({{ valid_from }}) over (
                partition by {{ natural_key }} order by {{ valid_from }}
            ) as next_valid_from
        from {{ model }}
    )
    select *
    from versions
    where next_valid_from is not null
      and next_valid_from < valid_to
{% endtest %}


{#
    Assert exactly one current version per key in an SCD2 dimension.
#}
{% test scd2_exactly_one_current(model, natural_key, flag_column='is_current') %}
    select
        {{ natural_key }} as natural_key,
        count(*) as current_versions
    from {{ model }}
    where {{ flag_column }}
    group by 1
    having count(*) != 1
{% endtest %}


{#
    Assert a fact table's row count matches its staging source within tolerance.

    Tolerance rather than equality because staging is a view over a raw layer
    that the sink is still appending to; an exact match would be flaky by
    construction. Zero tolerance is available by passing 0.
#}
{% test rowcount_matches(model, compare_model, tolerance=0.0, where_clause='1=1') %}
    with this_count as (
        select count(*) as n from {{ model }} where {{ where_clause }}
    ),
    that_count as (
        select count(*) as n from {{ compare_model }} where {{ where_clause }}
    )
    select
        this_count.n as model_rows,
        that_count.n as source_rows,
        abs(this_count.n - that_count.n) as delta
    from this_count, that_count
    where abs(this_count.n - that_count.n)
          > greatest({{ tolerance }} * that_count.n, 0)
{% endtest %}
