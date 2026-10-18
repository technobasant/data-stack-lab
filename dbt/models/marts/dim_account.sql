-- Type 2: one row per version of an account, valid over [valid_from, valid_to).
--
-- Two adjustments to the snapshot's own dates:
--   * the first version is stretched back to signup. The snapshot dates it from the
--     last change it saw, which for an account that changed before its first extract
--     is later than signup, and activity in that gap would find no version at all.
--     What the account looked like in the gap is unknown; its first known state is
--     the best available answer, and it is labelled as such by version_number = 1.
--   * the open version ends at 9999-12-31 instead of NULL, so as-of joins are a plain
--     range predicate on every warehouse.
with versions as (
    select
        dbt_scd_id,
        account_id,
        account_name,
        industry,
        country,
        plan_id,
        status,
        signup_at,
        dbt_valid_from,
        dbt_valid_to,
        row_number() over (partition by account_id order by dbt_valid_from) as version_number
    from {{ ref('snap_accounts') }}
)

select
    v.dbt_scd_id as account_key,
    v.account_id,
    cast(v.version_number as integer) as version_number,
    v.account_name,
    v.industry,
    v.country,
    v.plan_id,
    p.plan_name,
    v.status,
    v.signup_at,
    case when v.version_number = 1 then v.signup_at else v.dbt_valid_from end as valid_from,
    coalesce(v.dbt_valid_to, cast('9999-12-31 00:00:00' as timestamp)) as valid_to,
    v.dbt_valid_to is null as is_current
from versions as v
inner join {{ ref('dim_plan') }} as p on v.plan_id = p.plan_id
