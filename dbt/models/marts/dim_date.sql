with days as (
    {{ dbt.date_spine("day", "cast('2023-01-01' as date)", "cast('2027-01-01' as date)") }}
)

select
    {{ date_key('date_day') }} as date_key,
    cast(date_day as date) as date_day,
    cast({{ dbt.date_trunc('month', 'date_day') }} as date) as month_start,
    cast(extract(year from date_day) as integer) as calendar_year,
    cast(extract(quarter from date_day) as integer) as calendar_quarter,
    cast(extract(month from date_day) as integer) as calendar_month,
    cast(extract(day from date_day) as integer) as day_of_month
from days
