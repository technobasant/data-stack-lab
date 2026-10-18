select
    plan_id,
    plan_name,
    monthly_price_usd
from {{ ref('stg_app__plans') }}
