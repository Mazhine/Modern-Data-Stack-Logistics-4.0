
    
    

with all_values as (

    select
        is_delayed_prediction as value_field,
        count(*) as n_records

    from "logistics_db"."public"."gold_orders"
    group by is_delayed_prediction

)

select *
from all_values
where value_field not in (
    '0','1'
)


