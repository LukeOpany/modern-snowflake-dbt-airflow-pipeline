with orders as (

    select *
    from {{ ref('stg_jaffle__orders') }}

),

customers as (

    select *
    from {{ ref('stg_jaffle__customers') }}

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        customers.customer_name,
        orders.ordered_at,
        cast(orders.ordered_at as date) as order_date,
        orders.store_id,
        orders.subtotal_amount,
        orders.tax_amount,
        orders.order_total_amount
    from orders
    left join customers
        on orders.customer_id = customers.customer_id

)

select *
from final
