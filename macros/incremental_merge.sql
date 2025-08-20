{% macro incremental_merge(
    target_relation,
    source_relation,
    unique_key,
    dest_columns,
    merge_update_columns
) %}

    {% set dest_columns_list = dest_columns | join(', ') %}
    {% set merge_update_columns_list = merge_update_columns | join(', ') %}
    
    {% if target_relation.is_table %}
        {% set merge_sql %}
            MERGE INTO {{ target_relation }} AS target
            USING {{ source_relation }} AS source
            ON target.{{ unique_key }} = source.{{ unique_key }}
            WHEN MATCHED THEN
                UPDATE SET
                {% for column in merge_update_columns %}
                    {{ column }} = source.{{ column }}{% if not loop.last %},{% endif %}
                {% endfor %}
            WHEN NOT MATCHED THEN
                INSERT ({{ dest_columns_list }})
                VALUES ({{ dest_columns_list }})
        {% endset %}
        
        {{ return(merge_sql) }}
    {% else %}
        -- For views, just return the source query
        SELECT * FROM {{ source_relation }}
    {% endif %}

{% endmacro %}
