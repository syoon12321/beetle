with 

source as (

    select * from {{ source('raw', 'dbt_orders2') }}

),

renamed as (

    select
        cast(c1 as number(38, 0)) as order_key
        , cast(c2 as number(38, 0)) as cust_key
        , cast(c3 as varchar(1)) as order_status
        --, cast(replace(concat(c4, c5), '$', '') as number(12, 2)) as total_price
        , to_date(c6, 'YYYY-DD-MM') as ordr_date
        , cast(c7 as varchar(15)) as order_priority
        , cast(c8 as varchar(15)) as clerk
        , cast(c9 as number(38, 0)) as ship_priority
        , cast(concat(coalesce(c10, ''), coalesce(c11, ''), coalesce(c12, '')) as varchar(79)) as comment

    from source

)

select * from renamed
