{{
    config(
        materialized='incremental',
        unique_key=['l_orderkey', 'l_linenumber'],
        pre_hook=before_begin("alter table {{ source('raw', 'dbt_bc_lineitem') }} refresh"),
        post_hook="update bc_lineitem_fails set o_orderdate = (select o_orderdate from bc_lineitems where bc_lineitems.o_orderdate = bc_lineitem_fails.o_orderdate)"
    )
}}

with final as (
    select
        *,
        concat(l_orderkey, '-', l_linenumber) as l_surrogatekey,
        {{ ref('bc_orders') }}.order_date as o_orderdate,
        current_timestamp() as audit_insert_dttm_lo,
        current_timestamp() as audit_update_dttm_lo
    from {{ ref('stg_raw__dbt_bc_lineitem') }}
    inner join {{ ref('bc_orders') }} on l_orderkey=bc_orders.order_key

    {% if is_incremental() %}

    where o_orderdate > (select max(o_orderdate) from {{this}})

    {% endif %}
)

select * from final

union 

select 
    *
from {{ ref('bc_lineitem_fails') }}
where bc_lineitem_fails.o_orderdate is null
and not exists (
    select 
        bc_orders.order_key 
    from bc_orders
    where l_orderkey = bc_orders.order_key)