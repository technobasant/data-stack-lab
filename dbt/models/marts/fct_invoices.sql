-- One row per invoice, attached to the version of the account that was in effect when
-- it was billed. A left join, so an invoice with no matching version shows up as a
-- NULL key and fails the not_null test instead of silently disappearing.
select
    i.invoice_id,
    a.account_key,
    i.account_id,
    {{ date_key('i.invoiced_at') }} as invoice_date_key,
    i.invoiced_at,
    i.amount_usd
from {{ ref('stg_billing__invoices') }} as i
left join {{ ref('dim_account') }} as a
    on
        i.account_id = a.account_id
        and i.invoiced_at >= a.valid_from
        and i.invoiced_at < a.valid_to
