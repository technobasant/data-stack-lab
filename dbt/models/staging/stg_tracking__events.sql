-- Events are delivered at least once, so an event_id can arrive twice: keep the first
-- delivery. A filtered subquery instead of QUALIFY, which BigQuery only accepts
-- alongside a WHERE, GROUP BY or HAVING.
with ranked as (
    select
        event_id,
        user_id,
        account_id,
        event_type,
        platform,
        event_at,
        received_at,
        row_number() over (partition by event_id order by received_at) as delivery
    from {{ source('tracking', 'events') }}
)

select
    event_id,
    user_id,
    account_id,
    event_type,
    platform,
    event_at,
    received_at
from ranked
where delivery = 1
