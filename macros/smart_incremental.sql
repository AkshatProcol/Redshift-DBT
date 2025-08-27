{% macro smart_incremental_filter() %}
  {%- if is_incremental() -%}
    {# Check if table exists and has data #}
    {% set table_exists_query %}
      SELECT COUNT(*) as count FROM {{ this }}
    {% endset %}
    
    {% if execute %}
      {% set results = run_query(table_exists_query) %}
      {% set table_count = results.columns[0].values()[0] %}
      
      {% if table_count > 0 %}
        {# Table has data, use normal incremental filter #}
        WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL '30 minutes'
           OR created_at >= (SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL '30 minutes'
      {% else %}
        {# Table is empty, process all data #}
        {# No WHERE clause needed - process everything #}
      {% endif %}
    {% else %}
      {# During parsing, assume normal incremental behavior #}
      WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL '30 minutes'
         OR created_at >= (SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL '30 minutes'
    {% endif %}
  {%- endif -%}
{% endmacro %}