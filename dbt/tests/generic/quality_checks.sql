{# Checks used by the data-quality project (models/quality). Row-level checks return the
   offending rows; batch-level checks return one row when the day as a whole is off. #}

{% test happened_before_arrival(model, column_name) %}
-- An event cannot arrive before it happened; five minutes of clock skew are allowed.
-- Events "from the future" are the usual sign of local time written as UTC.
select {{ column_name }}
from {{ model }}
where {{ column_name }} > received_at + interval '5 minutes'
{% endtest %}

{% test volume_near_baseline(model, tolerance, days) %}
-- The day's row count against the median of the last `days` accepted days.
with today as (select count(*) as n from {{ model }}),

recent as (
    select rows from {{ source('accepted', 'daily') }}
    where platform = 'all'
    order by day desc
    limit {{ days }}
),

baseline as (select median(rows) as m from recent)

select today.n, baseline.m
from today cross join baseline
where abs(today.n - baseline.m) > {{ tolerance }} * baseline.m
{% endtest %}

{% test platform_share_near_baseline(model, tolerance, days) %}
-- Each platform's share of the day against its share over the last `days` accepted days.
-- A platform that goes silent barely moves the total; it moves its own share.
with today as (
    select platform, count(*) * 1.0 / sum(count(*)) over () as share
    from {{ model }}
    group by platform
),

recent_days as (
    select distinct day from {{ source('accepted', 'daily') }}
    order by day desc
    limit {{ days }}
),

baseline as (
    select d.platform, sum(d.rows) * 1.0 / sum(sum(d.rows)) over () as share
    from {{ source('accepted', 'daily') }} as d
    where d.platform <> 'all' and d.day in (select day from recent_days)
    group by d.platform
)

select b.platform, b.share as baseline_share, coalesce(t.share, 0) as today_share
from baseline as b
left join today as t on b.platform = t.platform
where abs(coalesce(t.share, 0) - b.share) > {{ tolerance }}
{% endtest %}

{% test matches_manifest(model) %}
-- Control totals: every file holds the number of rows its producer said it sent. Counted
-- on the raw delivery, before deduplication, because that is what the manifest describes.
with loaded as (
    select file_name, count(*) as n
    from {{ source('delivery', 'events') }}
    group by file_name
)

select m.file_name, m.row_count, coalesce(l.n, 0) as loaded
from {{ source('delivery', 'manifests') }} as m
left join loaded as l on m.file_name = l.file_name
where coalesce(l.n, 0) <> m.row_count
{% endtest %}

{% test redeliveries_below(model, max_share) %}
-- Share of delivered rows that were redeliveries (dropped by deduplication).
with counts as (
    select
        (select count(*) from {{ source('delivery', 'events') }}) as delivered,
        (select count(*) from {{ model }}) as kept
)
select delivered, kept
from counts
where delivered > 0 and (delivered - kept) * 1.0 / delivered > {{ max_share }}
{% endtest %}
