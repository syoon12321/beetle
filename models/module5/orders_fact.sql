{{
    config(
        materialized='incremental'
    )
}}

with orders5 as (
    select 
        o_clerk,
        o_comment,
        o_custkey,
        o_orderdate,
        o_orderkey,
        o_orderpriority,
        o_shippriority,
        o_totalprice,
        o_updatedate
    from {{ ref('orders5') }}
),

clerks as (
    {{ ref('clerks') }}
),

customers as (
    {{ ref('customers') }}
),

nations as (
    {{ ref('nations') }}
),

priority as (
    {{ ref('priority') }}
),

regions as (
    {{ref('regions')}}
)

select
    nations.n_name as country,
    regions.r_regionname as region,
    regions.r_shippingcompany as shipping_company
    case 
        when regions.r_shippingcompany=='FedEx' then 1
        else 0
    end as fedex_ind,
    case 
        when regions.r_shippingcompany=='DHL' then 1
        else 0
    end as dhl_ind,
    
from orders5
join nations on