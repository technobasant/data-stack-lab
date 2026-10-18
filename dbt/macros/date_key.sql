{# yyyymmdd as an integer, the conventional date-dimension key. Portable: extract()
   behaves the same on DuckDB, Snowflake, BigQuery and Databricks. #}
{% macro date_key(ts) -%}
    cast(
        extract(year from {{ ts }}) * 10000
        + extract(month from {{ ts }}) * 100
        + extract(day from {{ ts }})
        as integer
    )
{%- endmacro %}
