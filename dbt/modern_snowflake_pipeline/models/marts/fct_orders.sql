with orders as (

    select *
    from {{ ref('stg_tpch__orders') }}

),

customers as (

    select *
    from {{ ref('stg_tpch__customers') }}

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        customers.customer_name,
        customers.market_segment,
        orders.order_status,
        orders.total_price,
        orders.order_date,
        orders.order_priority,
        orders.shipping_priority
    from orders
    left join customers
        on orders.customer_id = customers.customer_id

)

select *
from final