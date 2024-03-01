{{
    config(
        materialized='table'
    )
}}

with other_t as (
    select * from {{ ref('stg_raw__dbt_orders2') }}
),

price_t as (
    select * from {{ ref('stg_raw__dbt_orders2_1') }}
),

final as (
    select

        other_t.order_key,
        other_t.cust_key,
        other_t.order_status,
        price_t.total_price,
        other_t.ordr_date,
        other_t.order_priority,
        other_t.clerk,
        other_t.ship_priority,
        other_t.comment

    from other_t
    left join price_t on other_t.order_key=price_t.order_key
)

select * from final