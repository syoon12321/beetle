{{
    config(
        materialized='incremental',
        unique_key=['l_orderkey', 'l_linenumber']

    )
}}

with final as (
    select
        *,
        concat(l_orderkey, '-', l_linenumber) as l_surrogatekey,
        null::timestamp as o_orderdate,
        current_timestamp() as audit_insert_dttm_lif,
        current_timestamp() as audit_update_dttm_lif
    from {{ ref('stg_raw__dbt_bc_lineitem') }}

    left join {{ ref('bc_orders') }} on l_orderkey=bc_orders.order_key

    {% if is_incremental() %}

    where o_orderdate > (select max(o_orderdate) from {{this}})

    {% endif %}
    and not exists (
        select 
            bc_orders.order_key 
        from bc_orders
        where l_orderkey = bc_orders.order_key)
)

select * from final