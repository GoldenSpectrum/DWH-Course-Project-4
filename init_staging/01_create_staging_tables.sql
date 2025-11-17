-- =========================================
-- 🛍️ SHOPZADA STAGING TABLE SCHEMA (FINAL)
-- =========================================

-- ==============================
-- BUSINESS DEPARTMENT
-- ==============================

CREATE TABLE IF NOT EXISTS stg_product_list (
    index_raw INT,
    product_id TEXT,
    product_name TEXT,
    product_type TEXT,
    price NUMERIC,
    raw_data JSONB
);

-- ==============================
-- CUSTOMER MANAGEMENT
-- ==============================

CREATE TABLE IF NOT EXISTS stg_user_credit_card (
    index_raw INT,
    user_id TEXT,
    name TEXT,
    credit_card_number TEXT,
    issuing_bank TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS stg_user_data (
    index_raw INT,
    user_id TEXT,
    creation_date TEXT,
    name TEXT,
    street TEXT,
    state TEXT,
    city TEXT,
    country TEXT,
    birthdate TEXT,
    gender TEXT,
    device_address TEXT,
    user_type TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS stg_user_job (
    index_raw INT,
    user_id TEXT,
    name TEXT,
    job_title TEXT,
    job_level TEXT,
    raw_data JSONB
);

-- ==============================
-- ENTERPRISE (MERCHANT + STAFF)
-- ==============================

CREATE TABLE IF NOT EXISTS stg_merchant_data (
    index_raw INT,
    merchant_id TEXT,
    creation_date TEXT,
    name TEXT,
    street TEXT,
    state TEXT,
    city TEXT,
    country TEXT,
    contact_number TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS stg_staff_data (
    index_raw INT,
    staff_id TEXT,
    name TEXT,
    job_level TEXT,
    street TEXT,
    state TEXT,
    city TEXT,
    country TEXT,
    contact_number TEXT,
    creation_date TEXT,
    raw_data JSONB
);

-- ==============================
-- ORDERS WITH MERCHANT (3 FILES)
-- ==============================

CREATE TABLE IF NOT EXISTS stg_order_with_merchant (
    index_raw INT,
    order_id TEXT,
    merchant_id TEXT,
    staff_id TEXT,
    raw_data JSONB
);

-- ==============================
-- MARKETING (CAMPAIGNS)
-- ==============================

CREATE TABLE IF NOT EXISTS stg_campaign_data (
    index_raw INT,
    campaign_id TEXT,
    campaign_name TEXT,
    campaign_description TEXT,
    discount TEXT,
    raw_data JSONB
);


-- NOTE:
-- campaign_data.csv is one (1) column containing
-- a giant TSV string. No clean split in staging.
-- Transform layer will parse this.

CREATE TABLE IF NOT EXISTS stg_transactional_campaign (
    index_raw INT,
    transaction_date TEXT,
    campaign_id TEXT,
    order_id TEXT,
    "estimated arrival" TEXT,
    availed TEXT,
    raw_data JSONB
);

-- ==============================
-- OPERATIONS (LINE ITEMS)
-- ==============================

CREATE TABLE IF NOT EXISTS stg_line_item_prices (
    index_raw INT,
    order_id TEXT,
    price NUMERIC,
    quantity TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS stg_line_item_products (
    index_raw INT,
    order_id TEXT,
    product_name TEXT,
    product_id TEXT,
    raw_data JSONB
);

-- ==============================
-- ORDER DATA (MULTI-SOURCE)
-- ==============================

CREATE TABLE IF NOT EXISTS stg_order_data (
    index_raw INT,
    order_id TEXT,
    user_id TEXT,
    "estimated arrival" TEXT,
    transaction_date TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS stg_order_delays (
    index_raw INT,
    order_id TEXT,
    "delay in days" INT,
    raw_data JSONB
);
