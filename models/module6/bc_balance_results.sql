with 

orders_source as (
    select * from {{ ref('stg_raw__dbt_bc_orders') }}
),

orders as (
    select * from {{ ref('bc_orders') }}
),

lineitem_source as (
    select 
        *, 
        concat(l_orderkey, '-', l_linenumber) as l_surrogatekey 
    from {{ ref('stg_raw__dbt_bc_lineitem') }}
),

lineitem as (
    select * from {{ ref('bc_lineitems') }}
),

bc_lineitem_fails as (
    select * from {{ ref('bc_lineitem_fails')}}
),

orders_results as (
    select 
        'order' as balance_name,
        count(orders_source.order_key) as source_record_count,
        count(orders.order_key) as target_record_count,
        current_timestamp() as audit_insert_dttm
    from orders
    join orders_source on orders.order_key=orders_source.order_key
),

lineitem_results as (
    select 
        'lineitem' as balance_name,
        count(*) as source_record_count,
        count(lineitem.l_orderkey) as target_record_count,
        current_timestamp() as audit_insert_dttm
    from lineitem
    join lineitem_source on lineitem.l_surrogatekey=lineitem_source.l_surrogatekey    
),

union_results as (
    select * from orders_results
    union all
    select * from lineitem_results
),

final as (

    select * from union_results

)

select * from final
