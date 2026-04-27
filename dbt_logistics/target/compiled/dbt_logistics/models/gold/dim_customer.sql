

WITH silver AS (
    SELECT DISTINCT
        customer_id,
        customer_segment,
        customer_country,
        customer_city
    FROM silver_orders
    WHERE customer_id IS NOT NULL
)
SELECT * FROM silver