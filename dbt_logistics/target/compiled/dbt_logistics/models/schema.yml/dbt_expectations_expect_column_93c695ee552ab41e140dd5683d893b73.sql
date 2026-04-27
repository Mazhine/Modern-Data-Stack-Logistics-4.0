






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and delivery_delay_risk >= -100 and delivery_delay_risk <= 360
)
 as expression


    from "logistics_db"."public"."gold_orders"
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







