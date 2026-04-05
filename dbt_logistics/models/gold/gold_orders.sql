{{ config(materialized='table') }}

WITH silver AS (
    SELECT *
    FROM silver_orders
)
SELECT
    order_id,
    customer_id,
    order_date,
    shipping_date,
    order_item_total,
    delivery_status,
    shipping_mode,
    delivery_delay_risk,
    delay_probability,
    is_delayed_prediction,
    CASE 
        WHEN delivery_delay_risk > 0 THEN 'En retard'
        WHEN delivery_delay_risk = 0 THEN 'Dans les temps'
        ELSE 'En avance'
    END as delay_category
FROM silver
