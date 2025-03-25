{% macro as_sql_list(hashed_fields_list) -%}

    {{ hashed_fields_list | join(", ") }}

{%- endmacro %}

{% test no_hash_collisions(model, column_name, hashed_fields) %}
    with
        all_tuples as (
            select distinct {{ column_name }} as hash, {{ hashed_fields }}
            from {{ model }}
        ),
        validation_errors as (
            select hash, count(*) from all_tuples group by hash having count(*) > 1

        )
    select *
    from validation_errors
{% endtest %}
