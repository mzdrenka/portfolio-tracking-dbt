with
    current_from_snapshot as (
        select * exclude (dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to)
        from {{ ref("SNSH_CURRENCY") }}
        where dbt_valid_to is null
    )

select *
from current_from_snapshot
