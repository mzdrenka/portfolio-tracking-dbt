{{ config(materialized="ephemeral") }}

with
    src_data as (
        select
            alphabeticcode as alphabeticcode,  -- TEXT
            numericcode as numericcode,  -- NUMBER
            decimaldigits as decimaldigits,  -- NUMBER
            currencyname as currencyname,  -- TEXT
            locations as locations,  -- TEXT
            load_ts as load_ts,  -- TIMESTAMP_NTZ
            'SEED.CURRENCY' as record_source
        from {{ source("seeds", "CURRENCY") }}
    ),
    default_record as (
        select
            'Missing' as alphabeticcode,
            '-1' as numericcode,
            '-1' as decimaldigits,
            'Missing' as currencyname,
            'Missing' as locations,
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
            {{ dbt_utils.surrogate_key(["alphabeticcode"]) }} as currency_hkey,
            {{
                dbt_utils.surrogate_key(
                    [
                        "alphabeticcode",
                        "numericcode",
                        "decimaldigits",
                        "currencyname",
                        "locations",
                    ]
                )
            }} as currency_hdiff,
            * exclude load_ts,
            load_ts as load_ts_utc
        from src_data_and_default_record
    )

select *
from hashed
