with
    current_from_snapshot as (

        select * from {{ ref("SNSH_ABC_BANK_POSITION") }} where dbt_valid_to is null
    )

select
    *,
    position_value - cost_base as unrealized_profit,
    round(unrealized_profit / cost_base, 5) as unrealized_profit_pct
from current_from_snapshot
