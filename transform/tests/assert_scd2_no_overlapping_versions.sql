/*
    No customer may have two versions valid at the same instant.

    Stronger than the generic `scd2_no_overlaps` applied in the schema file: it
    also catches GAPS and non-contiguity, not just overlap. Both failures break
    the as-of join in `fct_orders`, but they break it differently and the
    distinction tells you where to look:

      * OVERLAP  -> the as-of join matches TWO versions and fans the fact out.
                    Revenue doubles for the affected customers.
      * GAP      -> the as-of join matches NO version and `customer_key` is
                    null. Revenue silently disappears from every dimensional
                    aggregate while still being present in fct_orders.

    A gap is the more dangerous of the two precisely because the row count does
    not change.
*/

with versions as (

    select
        customer_id,
        version_number,
        valid_from,
        valid_to_or_infinity,
        lead(valid_from) over (
            partition by customer_id order by valid_from
        ) as next_valid_from
    from {{ ref('dim_customer') }}

)

select
    customer_id,
    version_number,
    valid_from,
    valid_to_or_infinity,
    next_valid_from,
    case
        when next_valid_from < valid_to_or_infinity then 'overlap'
        when next_valid_from > valid_to_or_infinity then 'gap'
    end as defect
from versions
where next_valid_from is not null
  and next_valid_from != valid_to_or_infinity
