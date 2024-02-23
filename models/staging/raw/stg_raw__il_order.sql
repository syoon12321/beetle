{{
    config(
        materialized='table',
        pre_hook=before_begin("alter external table il_order refresh")
    )
}}

with 

source as (

    select * from {{ source('raw', 'il_order') }}

),

renamed as (

    select
        c1 as o_orderkey,
        c2 as o_custkey,
        c3 as o_orderstatus,
        c4 as o_totalprice,
        c5 as o_orderdate,
        c6 as o_orderpriority,
        c7 as o_clerk,
        c8 as o_shippriority,
        c9 as o_comment,
        c10 as o_updatedate

    from source

)

select * from renamed
