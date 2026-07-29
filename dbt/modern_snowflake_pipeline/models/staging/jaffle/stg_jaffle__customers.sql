with source as (

    select *
    from {{ source('jaffle', 'customers') }}

),

renamed as (

    select
        customer_id,
        customer_name
    from source

)

select *
from renamed
