-- The as-of join may neither drop an invoice nor bill one twice: the fact table must
-- hold exactly the billing export's invoices and money.
with billing as (
    select count(*) as billing_invoices, sum(amount_usd) as billing_usd
    from {{ ref('stg_billing__invoices') }}
),

modelled as (
    select count(*) as fact_invoices, sum(amount_usd) as fact_usd
    from {{ ref('fct_invoices') }}
)

select *
from billing cross join modelled
where billing_invoices <> fact_invoices or billing_usd <> fact_usd
