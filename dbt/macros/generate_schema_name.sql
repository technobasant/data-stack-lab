{#
    dbt's default prefixes a custom schema with the target schema (`main_marts`). Here
    the layer names are the contract — `raw`, `staging`, `snapshots`, `marts` — so a
    bare `+schema` means exactly what it says, on every warehouse.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
