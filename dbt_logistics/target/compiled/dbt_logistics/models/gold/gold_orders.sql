

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
    days_for_shipping_real,
    days_for_shipment_scheduled,
    is_delayed_actual,
    is_delayed_prediction,
    is_prediction_correct,
    CASE 
        WHEN is_delayed_prediction = 1 THEN 'Risque Elevé'
        ELSE 'Risque Faible'
    END as prediction_category
FROM silver