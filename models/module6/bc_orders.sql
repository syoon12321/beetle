{{
    config(
        materialized='incremental',
        unique_key='order_key'
    )
}}

with final as (
    select
        *,
        current_timestamp() as audit_insert_dttm
    from {{ ref('stg_raw__dbt_bc_orders') }}

    {% if is_incremental() %}

    where order_date > (select max(order_date) from {{this}})

    {% endif %}
)

select * from final