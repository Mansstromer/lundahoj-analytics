{% macro truncate_to_hour(timestamp_column) %}
    date_trunc('hour', {{ timestamp_column }})
{% endmacro %}

{% macro calculate_percentage(numerator, denominator, decimal_places=1) %}
    round(
        ({{ numerator }}::float / nullif({{ denominator }}, 0) * 100)::numeric,
        {{ decimal_places }}
    )
{% endmacro %}

{% macro safe_divide(numerator, denominator, default=0) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null then {{ default }}
        else {{ numerator }}::float / {{ denominator }}
    end
{% endmacro %}