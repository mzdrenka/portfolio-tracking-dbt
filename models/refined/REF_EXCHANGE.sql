with
    current_from_snapshot as (

        {{ current_from_snapshot(snsh_ref=ref("SNSH_EXCHANGE")) }}
    )

select *
from current_from_snapshot
