-- One day's delivery, typed to the contract and deduplicated. Selecting the columns by
-- name is the schema check: a renamed or missing column fails the build here.
-- Delivery is at least once, so redeliveries are normal: within the day the first
-- delivery wins, and events accepted on an earlier day are dropped. A unique test on raw
-- deliveries would fail every day; the tests below check what is left after this step.
with delivered as (
    select
        event_id,
        user_id,
        account_id,
        event_type,
        platform,
        event_at,
        received_at,
        file_name,
        row_number() over (partition by event_id order by received_at, file_name) as delivery
    from {{ source('delivery', 'events') }}
)

select
    event_id,
    user_id,
    account_id,
    event_type,
    platform,
    event_at,
    received_at,
    file_name
from delivered
where
    delivery = 1
    and event_id not in (select a.event_id from {{ source('accepted', 'events') }} as a)
