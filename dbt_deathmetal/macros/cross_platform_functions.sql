{% macro split_part(column, delimiter, part_number) %}
    {% if target.type == 'bigquery' %}
        CASE
            WHEN ARRAY_LENGTH(SPLIT({{ column }}, '{{ delimiter }}')) >= {{ part_number }}
            THEN SPLIT({{ column }}, '{{ delimiter }}')[OFFSET({{ part_number - 1 }})]
            ELSE NULL
        END
    {% else %}
        split_part({{ column }}, '{{ delimiter }}', {{ part_number }})
    {% endif %}
{% endmacro %}

{% macro safe_cast_integer(column) %}
    {% if target.type == 'bigquery' %}
        SAFE_CAST({{ column }} AS INT64)
    {% else %}
        {{ column }}::integer
    {% endif %}
{% endmacro %}

{% macro safe_cast_decimal(column) %}
    {% if target.type == 'bigquery' %}
        SAFE_CAST({{ column }} AS NUMERIC)
    {% else %}
        {{ column }}::decimal
    {% endif %}
{% endmacro %}

{% macro mode_function(column) %}
    {% if target.type == 'bigquery' %}
        (
            SELECT value
            FROM UNNEST(APPROX_TOP_COUNT({{ column }}, 1))
            ORDER BY count DESC
            LIMIT 1
        )
    {% else %}
        mode() within group (order by {{ column }})
    {% endif %}
{% endmacro %}

{% macro percentile_cont(column, percentile) %}
    {% if target.type == 'bigquery' %}
        PERCENTILE_CONT({{ column }}, {{ percentile }}) OVER()
    {% else %}
        percentile_cont({{ percentile }}) within group (order by {{ column }})
    {% endif %}
{% endmacro %}

{% macro current_timestamp_func() %}
    {% if target.type == 'bigquery' %}
        CURRENT_TIMESTAMP()
    {% else %}
        current_timestamp
    {% endif %}
{% endmacro %}

{% macro stddev_function(column) %}
    {% if target.type == 'bigquery' %}
        STDDEV({{ column }})
    {% else %}
        stddev({{ column }})
    {% endif %}
{% endmacro %}

{% macro get_row_number(order_by, partition_by=None) %}
    row_number() over (
        {% if partition_by is not none %}
            partition by {{ partition_by }}
        {% endif %}
        order by {{ order_by }}
    )
{% endmacro %}