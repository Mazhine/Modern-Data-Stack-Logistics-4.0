{{ config(materialized='table') }}

WITH silver AS (
    SELECT *
    FROM silver_orders
)
SELECT
    order_id,
    customer_id,
    product_card_id,
    order_date,
    shipping_date,
    days_for_shipping_real,
    days_for_shipment_scheduled,
    order_item_total,
    order_item_discount,
    benefit_per_order,
    sales_per_customer,
    order_status,
    delivery_status,
    shipping_mode,
    type,
    order_country,
    order_region,
    order_city,
    is_delayed_actual,
    is_delayed_prediction,
    is_prediction_correct
FROM silver
