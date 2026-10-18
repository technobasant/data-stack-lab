select
    user_id,
    account_id,
    lower(trim(email)) as email,
    full_name,
    role,
    created_at
from {{ source('app', 'users') }}
