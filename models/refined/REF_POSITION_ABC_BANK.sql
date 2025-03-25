with
    current_from_snapshot as (

        {{
            current_from_snapshot(
                snsh_ref=ref("SNSH_ABC_BANK_POSITION"), output_load_ts=false
            )
        }}

    )

select
    *,
    position_value - cost_base as unrealized_profit,
    round(unrealized_profit / cost_base, 5) as unrealized_profit_pct
from current_from_snapshot
