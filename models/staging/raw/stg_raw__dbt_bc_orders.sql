{{
    config(
        materialized='table',
        pre_hook=before_begin("alter table {{ source('raw', 'dbt_bc_orders') }} refresh")

    )
}}

with 

source as (

    select * from {{ source('raw', 'dbt_bc_orders') }}

),

renamed as (

    select
        c1 as order_key,
        c2 as cust_key,
        c3 as order_status,
        c4 as total_price,
        c5 as order_date,
        c6 as order_priority,
        c7 as order_clerk,
        c8 as ship_priority,
        c9 as comment

    from source

)

select * from renamed
