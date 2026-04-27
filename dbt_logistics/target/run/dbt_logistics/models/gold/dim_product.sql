
  
    

  create  table "logistics_db"."public"."dim_product__dbt_tmp"
  
  
    as
  
  (
    

WITH silver AS (
    SELECT DISTINCT
        product_card_id,
        category_name,
        department_name,
        product_price
    FROM silver_orders
    WHERE product_card_id IS NOT NULL
)
SELECT * FROM silver
  );
  