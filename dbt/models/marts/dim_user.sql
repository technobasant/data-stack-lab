-- Type 1: users are overwritten in place. Name and full e-mail address stay in
-- staging; the mart carries the e-mail domain, which is what analysis needs.
select
    user_id,
    account_id,
    {{ dbt.split_part('email', "'@'", 2) }} as email_domain,
    role,
    created_at
from {{ ref('stg_app__users') }}
