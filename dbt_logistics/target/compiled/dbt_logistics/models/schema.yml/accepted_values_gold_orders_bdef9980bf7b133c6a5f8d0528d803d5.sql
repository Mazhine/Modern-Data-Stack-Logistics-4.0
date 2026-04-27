
    
    

with all_values as (

    select
        delivery_status as value_field,
        count(*) as n_records

    from "logistics_db"."public"."gold_orders"
    group by delivery_status

)

select *
from all_values
where value_field not in (
    'Advance shipping','Late delivery','Shipping on time','Shipping canceled'
)


