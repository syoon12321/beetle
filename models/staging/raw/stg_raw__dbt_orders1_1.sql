with 

source as (

    select * from {{ source('raw', 'dbt_orders1_1') }}

),

order_key as (

    select
        value,
        left(c1, 7) as initial_key,
        case         
            when right(initial_key, 1)=',' then left(initial_key,6)
            else null end
        as six_digits,
        case         
            when left(right(initial_key, 2), 1)=',' then left(initial_key,5)
            else null end
        as five_digits,
        case         
            when left(right(initial_key, 3), 1)=',' then left(initial_key,4)
            else null end
        as four_digits,
        coalesce(six_digits, five_digits, four_digits, initial_key) as order_key
    from source
),

cents as (

    select
        
        value,
        concat('.', left(c2, 2)) as extracted_cents

    from source

),

dollars as (
    select

        value,
        right(c1,7) as six_figures,
        case 
            when left(six_figures, 1)=',' then right(c1, 5) --four figures
            when left(six_figures, 1)='$' then right(c1, 6) --five figures
            else six_figures end
        as extracted_dollars

    from source
),

dollars_and_cents as (
    select

        order_key.order_key,
        cents.value,
        concat(dollars.extracted_dollars, cents.extracted_cents) as price_with_comma,
        replace(price_with_comma, ',', '') as final_price
        
    from cents
    join dollars on cents.value=dollars.value
    join order_key on cents.value=order_key.value
),

renamed as (

    select 
        order_key,
        cast(final_price as number(12,2)) as total_price
    from dollars_and_cents 
)

select * from renamed
