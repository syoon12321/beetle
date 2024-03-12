with 

source as (

    select * from {{ source('raw', 'dbt_bc_lineitem') }}

),

renamed as (

    select
        c1 as l_orderkey,
        c2 as l_partkey,
        c3 as l_suppkey,
        c4 as l_linenumber,
        c5 as l_quantity,
        c6 as l_extendedprice,
        c7 as l_discount,
        c8 as l_tax,
        c9 as l_returnflag,
        c10 as l_linestatus,
        c11 as l_shipdate,
        c12 as l_commitdate,
        c13 as l_receiptdate,
        c14 as l_shipinstruct,
        c15 as l_shipmode,
        c16 as l_comment

    from source

)

select * from renamed
