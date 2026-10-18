{#
    Integrity of a Type 2 dimension: an entity's versions must tile time. No two
    versions may overlap (an as-of join would return both), and each version must end
    exactly where the next begins (an as-of join would find none in a gap).
#}
{% test valid_windows_do_not_overlap(model, entity, valid_from, valid_to) %}
select a.{{ entity }}
from {{ model }} as a
inner join {{ model }} as b
    on
        a.{{ entity }} = b.{{ entity }}
        and a.{{ valid_from }} < b.{{ valid_from }}
        and b.{{ valid_from }} < a.{{ valid_to }}
{% endtest %}

{% test valid_windows_are_contiguous(model, entity, valid_from, valid_to) %}
with ordered as (
    select
        {{ entity }} as entity,
        {{ valid_to }} as ends_at,
        lead({{ valid_from }}) over (
            partition by {{ entity }} order by {{ valid_from }}
        ) as next_starts_at
    from {{ model }}
)

select entity
from ordered
where next_starts_at is not null and next_starts_at <> ends_at
{% endtest %}
