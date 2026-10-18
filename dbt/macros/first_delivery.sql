{# One row per event_id within a batch: the first delivery. Across batches the model's
   unique_key does the same job, so a redelivery in a later file replaces, never adds. #}
{% macro first_delivery(batch) -%}
select event_id, user_id, account_id, event_type, platform, event_at, received_at
from (
    select
        *,
        row_number() over (partition by event_id order by received_at) as delivery
    from {{ batch }}
) as deliveries
where delivery = 1
{%- endmacro %}
