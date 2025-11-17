-- ============= BUSINESS DEPT =============
CREATE TABLE IF NOT EXISTS stg_product_list (
    index_raw INT,
    product_id TEXT,
    product_name TEXT,
    product_type TEXT,
    price NUMERIC,
    raw_data JSONB
);

-- ============= CUSTOMER MGMT =============
CREATE TABLE IF NOT EXISTS stg_user_credit_card (
    index_raw INT,
    user_id TEXT,
    name TEXT,
    card_provider TEXT,
    card_last4 TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS stg_user_data (
    index_raw INT,
    user_id TEXT,
    name TEXT,
    gender TEXT,
    birthdate TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    creation_date TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS stg_user_job (
    index_raw INT,
    user_id TEXT,
    job_title TEXT,
    job_level TEXT,
    raw_data JSONB
);

-- ============= ENTERPRISE DEPT =============
CREATE TABLE IF NOT EXISTS stg_order_with_merchant (
    index_raw INT,
    order_id TEXT,
    merchant_id TEXT,
    staff_id TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS stg_merchant_data (
    index_raw INT,
    merchant_id TEXT,
    merchant_name TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    creation_date TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS stg_staff_data (
    index_raw INT,
    staff_id TEXT,
    name TEXT,
    job_level TEXT,
    department TEXT,
    region TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    raw_data JSONB
);

-- ============= MARKETING =============
CREATE TABLE IF NOT EXISTS stg_campaign_data (
    index_raw INT,
    campaign_id TEXT,
    campaign_name TEXT,
    campaign_description TEXT,
    discount NUMERIC,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS stg_transactional_campaign (
    index_raw INT,
    transaction_id TEXT,
    user_id TEXT,
    order_id TEXT,
    product_id TEXT,
    campaign_id TEXT,
    transaction_date TEXT,
    estimated_arrival TEXT,
    raw_data JSONB
);

-- ============= OPERATIONS =============
CREATE TABLE IF NOT EXISTS stg_line_item_prices (
    index_raw INT,
    order_id TEXT,
    product_id TEXT,
    quantity TEXT,
    unit_price NUMERIC,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS stg_line_item_products (
    index_raw INT,
    order_id TEXT,
    product_id TEXT,
    product_name TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS stg_order_data (
    index_raw INT,
    order_id TEXT,
    user_id TEXT,
    transaction_date TEXT,
    estimated_arrival TEXT,
    order_total NUMERIC,
    shipping_cost NUMERIC,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS stg_order_delays (
    index_raw INT,
    order_id TEXT,
    days_delayed INT,
    reason_code TEXT,
    raw_data JSONB
);
