select
    invoice_id,
    account_id,
    invoiced_at,
    cast(amount_usd as decimal(10, 2)) as amount_usd,
    currency
from {{ source('billing', 'invoices') }}
