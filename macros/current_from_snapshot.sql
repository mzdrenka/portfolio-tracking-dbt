{% macro current_from_snapshot(snsh_ref, output_load_ts = true) %}

    select
        * exclude(dbt_scd_id, dbt_valid_from, dbt_valid_to)
        {% if output_load_ts %}
        , dbt_updated_at as snsh_updated_at
        {% endif %}
    from {{ snsh_ref }}
    where dbt_valid_to is null

{% endmacro %}
