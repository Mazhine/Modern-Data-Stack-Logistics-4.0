-- Cration du Schma en toile (Star Schema) pour la base Logistics 4.0

-- DIMENSIONS
CREATE TABLE IF NOT EXISTS Dim_Customer (
    customer_id INT PRIMARY KEY,
    customer_segment VARCHAR(255),
    customer_country VARCHAR(255),
    customer_city VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Dim_Product (
    product_card_id INT PRIMARY KEY,
    category_name VARCHAR(255),
    department_name VARCHAR(255),
    product_price DOUBLE PRECISION
);

-- La gographie de commande est optionnellement place en dimension
CREATE TABLE IF NOT EXISTS Dim_Geography (
    geography_id SERIAL PRIMARY KEY,
    order_country VARCHAR(255),
    order_region VARCHAR(255),
    order_city VARCHAR(255)
);

-- FAITS (Fact_Orders : Les 25 Colonnes Logistiques dfinies dans le PRD)
CREATE TABLE IF NOT EXISTS Fact_Orders (
    order_id INT PRIMARY KEY,
    customer_id INT REFERENCES Dim_Customer(customer_id),
    product_card_id INT REFERENCES Dim_Product(product_card_id),
    
    order_date TIMESTAMP,
    shipping_date TIMESTAMP,
    days_for_shipping_real INT,
    days_for_shipment_scheduled INT,
    
    order_item_total DOUBLE PRECISION,
    order_item_discount DOUBLE PRECISION,
    benefit_per_order DOUBLE PRECISION,
    sales_per_customer DOUBLE PRECISION,
    
    order_status VARCHAR(100),
    delivery_status VARCHAR(100),
    shipping_mode VARCHAR(100),
    type VARCHAR(100),
    
    order_country VARCHAR(255),
    order_region VARCHAR(255),
    order_city VARCHAR(255),
    
    late_delivery_risk INT
);

-- INDEXATION CRITIQUE
-- Index Block Range Index (BRIN) sur la srie temporelle pour optimiser dmesurment Power BI
CREATE INDEX idx_fact_orders_date_brin ON Fact_Orders USING BRIN (order_date);

-- Index B-Tree standards sur les cls trangres
CREATE INDEX idx_fact_customer ON Fact_Orders(customer_id);
CREATE INDEX idx_fact_product ON Fact_Orders(product_card_id);
