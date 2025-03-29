{{ config(materialized="ephemeral") }}

with
    src_data as (
        select
            name as name,  -- TEXT
            id as id,  -- TEXT
            country as country,  -- TEXT
            city as city,  -- TEXT
            zone as zone,  -- TEXT
            delta as delta,  -- FLOAT
            dst_period as dst_period,  -- TEXT
            open as open,  -- TEXT
            close as close,  -- TEXT
            lunch as lunch,  -- TEXT
            open_utc as open_utc,  -- TEXT
            close_utc as close_utc,  -- TEXT
            lunch_utc as lunch_utc,  -- TEXT
            load_ts as load_ts,  -- TIMESTAMP_NTZ
            'SEED.EXCHANGE' as record_source
        from {{ source("seeds", "EXCHANGE") }}
    ),
    default_record as (
        select
            'Missing' as name,
            'Missing' as id,
            'Missing' as country,
            'Missing' as city,
            'Missing' as zone,
            '-1' as delta,
            'Missing' as dst_period,
            'Missing' as open,
            'Missing' as close,
            'Missing' as lunch,
            'Missing' as open_utc,
            'Missing' as close_utc,
            'Missing' as lunch_utc,
            '2020-01-01' as load_ts,
            'System.DefaultKey' as record_source
    ),
    src_data_and_default_record as (

        select *
        from src_data
        union
        select *
        from default_record

    ),
    hashed as (
        select
            {{ dbt_utils.surrogate_key(["id"]) }} as exchange_hkey,
            {{
                dbt_utils.surrogate_key(
                    [
                        "name",
                        "id",
                        "country",
                        "city",
                        "zone",
                        "delta",
                        "dst_period",
                        "open",
                        "close",
                        "lunch",
                        "open_utc",
                        "close_utc",
                        "lunch_utc",
                    ]
                )
            }} as exchange_hdiff,
            * exclude load_ts,
            load_ts as load_ts_utc
        from src_data_and_default_record
    )

select *
from hashed
