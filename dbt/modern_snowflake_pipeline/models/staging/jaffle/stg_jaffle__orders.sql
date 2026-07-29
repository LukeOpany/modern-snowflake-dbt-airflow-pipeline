with source as (

    select *
    from {{ source('jaffle', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        ordered_at,
        store_id,
        subtotal / 100.0 as subtotal_amount,
        tax_paid / 100.0 as tax_amount,
        order_total / 100.0 as order_total_amount
    from source

)

select *
from renamed
