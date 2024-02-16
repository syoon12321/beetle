with orders as (
    select * from {{ref('stg_orders')}}
),

payments as (
    select * from {{ref('stg_payments')}}
),

order_payments as (
    select
        order_id,
        sum(case when status='success' then amount end) as amount
    from payments
    group by 1
)

final as (
    select 
        order_id
        , customer_id
        , payments.amount as payment_amount
        , coalesce(order_payments.amount, 0) as amount
    from orders
    left join payments on payments.orderid=orders.order_id
    left join order_payments using (order_id)
)

select * from final