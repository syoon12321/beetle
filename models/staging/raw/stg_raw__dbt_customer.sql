{{
    config(
        pre_hook=before_begin("alter table dbt_customer refresh")
    )
}}

with 

source as (

    select * from {{ source('raw', 'dbt_customer') }}

),

renamed as (

    select
        c1 as c_custkey,
        c2 as c_name,
        c3 as c_address,
        c4 as c_nationkey,
        c5 as c_phone,
        c6 as c_acctbal,
        c7 as c_mktsegment,
        c8 as c_comment

    from source

)

select * from renamed
