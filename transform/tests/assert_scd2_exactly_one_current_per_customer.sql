/*
    Every customer has exactly one current version -- not zero, not two.

    Both failure directions are covered on purpose, because they are opposite
    bugs with opposite symptoms:

      * TWO current versions  -> `where is_current` fans out. Customer counts
        inflate and any per-customer aggregate double-counts.
      * ZERO current versions -> `where is_current` drops the customer entirely.
        They vanish from the active-customer list while their orders remain in
        the fact table, so the two disagree and neither looks obviously wrong.

    Zero is the one a naive test misses: counting rows `where is_current` and
    checking for duplicates only ever finds the first case.
*/

select
    customer_id,
    count(*) filter (where is_current)  as current_versions,
    count(*)                            as total_versions,
    case
        when count(*) filter (where is_current) = 0 then 'no current version'
        else 'multiple current versions'
    end                                 as defect
from {{ ref('dim_customer') }}
group by customer_id
having count(*) filter (where is_current) != 1
