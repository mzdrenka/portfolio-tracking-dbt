{{ config(materialized="ephemeral") }}

with
    src_data as (

        select
            country_name as country_name,  -- TEXT
            country_code_2_letter as country_code_2_letter,  -- TEXT
            country_code_3_letter as country_code_3_letter,  -- TEXT
            country_code_numeric as country_code_numeric,  -- NUMBER
            iso_3166_2 as iso_3166_2,  -- TEXT
            region as region,  -- TEXT
            sub_region as sub_region,  -- TEXT
            intermediate_region as intermediate_region,  -- TEXT
            region_code as region_code,  -- NUMBER
            sub_region_code as sub_region_code,  -- NUMBER
            intermediate_region_code as intermediate_region_code,  -- NUMBER
            load_ts as load_ts,  -- TIMESTAMP_NTZ
            'SEED.COUNTRY' as record_source
        from {{ source("seeds", "COUNTRY") }}

    ),

    default_record as (
        select
            'Missing' as country_name,
            'Missing' as country_code_2_letter,
            'Missing' as country_code_3_letter,
            '-1' as country_code_numeric,
            'Missing' as iso_3166_2,
            'Missing' as region,
            'Missing' as sub_region,
            'Missing' as intermediate_region,
            '-1' as region_code,
            '-1' as sub_region_code,
            '-1' as intermediate_region_code,
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
            {{ dbt_utils.surrogate_key(["country_name"]) }} as country_hkey,
            {{
                dbt_utils.surrogate_key(
                    [
                        "country_name",
                        "country_code_2_letter",
                        "country_code_3_letter",
                        "country_code_numeric",
                        "iso_3166_2",
                        "region",
                        "sub_region",
                        "intermediate_region",
                        "region_code",
                        "sub_region_code",
                        "intermediate_region_code",
                    ]
                )
            }} as country_hdiff,
            * exclude load_ts,
            load_ts as load_ts_utc
        from src_data_and_default_record
    )

select *
from hashed
