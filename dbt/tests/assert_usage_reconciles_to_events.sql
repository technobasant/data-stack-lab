-- Every deduplicated event is counted once in daily usage.
with source_events as (
    select count(*) as source_events from {{ ref('stg_tracking__events') }}
),

counted as (
    select sum(events) as counted_events from {{ ref('fct_daily_usage') }}
)

select *
from source_events cross join counted
where source_events <> counted_events
