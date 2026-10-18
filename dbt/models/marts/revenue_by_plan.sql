-- Monthly revenue with both attributions side by side:
--   billed_plan   the plan on the invoice's own account version (Type 2)
--   current_plan  the plan the account is on today (what a Type 1 dimension reports)
-- Summed by billed_plan this is the revenue each plan earned. Summed by current_plan
-- it is history rewritten by every later upgrade, downgrade and churn.
with current_versions as (
    select account_id, plan_name as current_plan
    from {{ ref('dim_account') }}
    where is_current
)

select
    cast({{ dbt.date_trunc('month', 'f.invoiced_at') }} as date) as revenue_month,
    billed.plan_name as billed_plan,
    c.current_plan,
    cast(count(*) as bigint) as invoices,
    cast(sum(f.amount_usd) as decimal(12, 2)) as revenue_usd
from {{ ref('fct_invoices') }} as f
inner join {{ ref('dim_account') }} as billed on f.account_key = billed.account_key
inner join current_versions as c on f.account_id = c.account_id
group by 1, 2, 3
