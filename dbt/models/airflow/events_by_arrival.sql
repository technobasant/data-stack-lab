-- Take what arrived after the newest arrival already loaded.
with batch as (
    select * from {{ source('landing', 'events') }}
    {% if is_incremental() %}
        where received_at > (select max(received_at) from {{ this }})
    {% endif %}
)

{{ first_delivery('batch') }}
