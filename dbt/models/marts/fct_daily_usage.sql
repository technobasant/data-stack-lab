-- One row per account version and day: usage counted against the version of the
-- account that was in effect when each event happened.
with resolved as (
    select
        e.event_id,
        e.user_id,
        e.account_id,
        e.event_type,
        e.event_at,
        a.account_key
    from {{ ref('stg_tracking__events') }} as e
    left join {{ ref('dim_account') }} as a
        on
            e.account_id = a.account_id
            and e.event_at >= a.valid_from
            and e.event_at < a.valid_to
)

select
    account_key,
    account_id,
    {{ date_key('event_at') }} as date_key,
    cast(event_at as date) as activity_date,
    cast(count(*) as bigint) as events,
    cast(count(distinct user_id) as bigint) as active_users,
    cast(sum(case when event_type = 'create_report' then 1 else 0 end) as bigint)
        as reports_created,
    cast(sum(case when event_type = 'export_csv' then 1 else 0 end) as bigint) as exports
from resolved
group by 1, 2, 3, 4
