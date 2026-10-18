-- Take exactly what arrived inside this run's data interval, as Airflow passes it in.
-- A re-run of a day reloads that day, whatever ran before or after it.
with batch as (
    select * from {{ source('landing', 'events') }}
    {% if is_incremental() %}
        where
            received_at >= cast('{{ var("interval_start", "1970-01-01 00:00:00") }}' as timestamp)
            and received_at < cast('{{ var("interval_end", "1970-01-01 00:00:00") }}' as timestamp)
    {% endif %}
)

{{ first_delivery('batch') }}
