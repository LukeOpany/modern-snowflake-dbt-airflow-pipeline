with source as (

    select *
    from {{ source('tpch', 'orders') }}

),

renamed as (

    select
        o_orderkey as order_id,
        o_custkey as customer_id,
        o_orderstatus as order_status,
        o_totalprice as total_price,
        o_orderdate as order_date,
        o_orderpriority as order_priority,
        o_clerk as clerk_name,
        o_shippriority as shipping_priority,
        o_comment as order_comment
    from source

)

select *
from renamed