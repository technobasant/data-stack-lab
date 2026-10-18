-- Take what happened after the newest event already loaded.
with batch as (
    select * from {{ source('landing', 'events') }}
    {% if is_incremental() %}
        where event_at > (select max(event_at) from {{ this }})
    {% endif %}
)

{{ first_delivery('batch') }}
