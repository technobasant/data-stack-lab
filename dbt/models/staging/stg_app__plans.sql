select
    plan_id,
    plan_name,
    cast(monthly_price_usd as decimal(10, 2)) as monthly_price_usd
from {{ source('app', 'plans') }}
