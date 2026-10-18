{# The grain test: no two rows share these columns. (dbt_utils has one too; this keeps
   the project free of packages so it runs offline and on dbt v2 unchanged.) #}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
