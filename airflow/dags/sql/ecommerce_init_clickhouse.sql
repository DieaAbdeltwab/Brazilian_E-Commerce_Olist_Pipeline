

CREATE DATABASE IF NOT EXISTS ecommerce;

USE ecommerce;

-- ===================================================
-- DIM TABLES
-- ===================================================

CREATE TABLE IF NOT EXISTS ecommerce.dim_customers (
    customer_id String,
    customer_unique_id String,
    customer_zip_code_prefix Nullable(Int64),
    customer_city Nullable(String),
    customer_state Nullable(String)
) ENGINE = MergeTree
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS ecommerce.dim_sellers (
    seller_id String,
    seller_zip_code_prefix Nullable(Int64),
    seller_city Nullable(String),
    seller_state Nullable(String)
) ENGINE = MergeTree
ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS ecommerce.dim_products (
    product_id String,
    product_category_name Nullable(String),
    product_category_name_english Nullable(String),
    product_name_lenght Nullable(Int32),
    product_description_lenght Nullable(Int32),
    product_photos_qty Nullable(Int32),
    product_weight_g Nullable(Int32),
    product_length_cm Nullable(Int32),
    product_height_cm Nullable(Int32),
    product_width_cm Nullable(Int32),
    product_volume_cm3 Nullable(Int32)
) ENGINE = MergeTree
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS ecommerce.dim_date (
    date_key Date,
    day Int32,
    month Int32,
    year Int32,
    day_of_week String,
    week_of_year Int32,
    day_name String,
    month_name String
) ENGINE = MergeTree
ORDER BY date_key;

-- ===================================================
-- FACT TABLE
-- ===================================================

CREATE TABLE IF NOT EXISTS ecommerce.fact_orders (
    order_id String,
    customer_id Nullable(String),
    seller_id Nullable(String),
    product_id Nullable(String),

    order_purchase_timestamp Nullable(DateTime),
    shipping_limit_date Nullable(DateTime),
    order_delivered_carrier_date Nullable(DateTime),
    order_delivered_customer_date Nullable(DateTime),
    order_estimated_delivery_date Nullable(DateTime),

    shipping_days Nullable(Int32),
    delivery_delay Nullable(Int32),
    price Nullable(Float64),
    freight_value Nullable(Float64),
    total_cost Nullable(Float64),
    total_payment Nullable(Float64),
    total_installments Nullable(Int64),
    payment_methods_count Nullable(Int64),
    main_payment_type Nullable(String),
    review_score Nullable(Int64),
    review_sentiment Nullable(String),

    purchase_date_key Nullable(Date),
    delivered_date_key Nullable(Date),
    estimated_date_key Nullable(Date)
) ENGINE = MergeTree
ORDER BY order_id;



