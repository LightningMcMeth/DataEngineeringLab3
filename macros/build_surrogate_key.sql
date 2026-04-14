{% macro build_surrogate_key(columns) %}
md5(
    {%- for column in columns -%}
        coalesce(cast({{ column }} as varchar), '_dbt_null_')
        {%- if not loop.last %} || '|' || {% endif -%}
    {%- endfor -%}
)
{% endmacro %}
