-- Verify that all six deliberate messiness patterns are present in the source.
--
--     make messiness
--
-- These are REQUIREMENTS, not accidents. Each one forces a real modelling
-- decision downstream, and if the generator ever stops producing one, the
-- warehouse test that covers it becomes vacuous -- it passes by testing
-- nothing, which is the worst state for a test to be in.
--
-- Every count below must be > 0.

\pset border 2
\pset title 'Deliberate messiness -- every count must be > 0'

with checks as (

    select 1 as ord,
           'legacy status spellings' as pattern,
           'orders.status carries paid/PAID/complete from an unfinished migration' as forces,
           (select count(*) from orders
            where status in ('paid','PAID','complete','Completed')) as n

    union all select 2,
           'soft deletes',
           'customers.deleted_at set, row never removed',
           (select count(*) from customers where deleted_at is not null)

    union all select 3,
           'soft-deleted WITH order history',
           'the case that forces a decision: keep their revenue or not',
           (select count(distinct c.id) from customers c
            join orders o on o.customer_id = c.id
            where c.deleted_at is not null)

    union all select 4,
           'naive local timestamps',
           'placed_at NULL, placed_at_local set -- needs timezone resolution',
           (select count(*) from orders
            where placed_at is null and placed_at_local is not null)

    union all select 5,
           'late refunds (> 7 days)',
           'the constraint that breaks a business-timestamp incremental',
           (select count(*) from refunds r join payments p on p.id = r.payment_id
            where r.issued_at - p.created_at > interval '7 days')

    union all select 6,
           'refunds at/near the lookback edge (> 14 days)',
           'proves the lookback window is actually exercised',
           (select count(*) from refunds r join payments p on p.id = r.payment_id
            where r.issued_at - p.created_at > interval '14 days')

    union all select 7,
           'multi-currency orders',
           'needs a dated FX conversion layer, not a scalar rate',
           (select count(*) from orders where currency <> 'USD')

    union all select 8,
           'pending payments with NULL processed_at',
           'why a blanket not_null test downstream is wrong',
           (select count(*) from payments
            where status = 'pending' and processed_at is null)

    union all select 9,
           'plan change events',
           'gives SCD2 and the MRR proration something to do',
           (select count(*) from subscription_events
            where event_type in ('upgraded','downgraded'))

    union all select 10,
           'customers who changed country',
           'the mutation that makes SCD2 on dim_customer matter',
           (select count(*) from (
                select customer_id from (
                    select c.id as customer_id, c.country_code from customers c
                ) t group by customer_id having count(distinct country_code) > 1
            ) x)
)

select
    ord                                   as "#",
    pattern                               as "pattern",
    n                                     as "count",
    case when n > 0 then 'ok' else 'MISSING' end as "status",
    forces                                as "what it forces downstream"
from checks
order by ord;

-- A non-zero exit would be better, but psql cannot fail a script on a query
-- result without \gset gymnastics that obscure the output. `make messiness`
-- greps for MISSING instead.
