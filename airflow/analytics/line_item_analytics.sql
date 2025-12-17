-- =====================================================
-- LINE-ITEM LEVEL ANALYTICS
-- Uses: fact_line_items + fact_orders + dim_product + dim_date
-- =====================================================

-- -----------------------------------------------------
-- 2.1 Products sold most frequently
-- -----------------------------------------------------
CREATE OR REPLACE VIEW product_sales_frequency AS
SELECT
    p.product_name,
    SUM(li.line_item_quantity) AS total_qty_sold
FROM fact_line_items li
JOIN dim_product p
    ON li.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_qty_sold DESC;


-- -----------------------------------------------------
-- 2.2 Total revenue per product and category
-- -----------------------------------------------------
CREATE OR REPLACE VIEW product_revenue AS
SELECT
    p.product_name,
    p.product_type,
    SUM(li.line_item_price * li.line_item_quantity) AS total_revenue
FROM fact_line_items li
JOIN dim_product p
    ON li.product_id = p.product_id
GROUP BY p.product_name, p.product_type
ORDER BY total_revenue DESC;


-- -----------------------------------------------------
-- 2.3 Frequently purchased product combinations
-- -----------------------------------------------------
CREATE OR REPLACE VIEW frequent_product_pairs AS
SELECT
    p1.product_name AS product_a,
    p2.product_name AS product_b,
    COUNT(*) AS times_bought_together
FROM fact_line_items li1
JOIN fact_line_items li2
    ON li1.order_id = li2.order_id
   AND li1.product_id < li2.product_id
JOIN dim_product p1 ON li1.product_id = p1.product_id
JOIN dim_product p2 ON li2.product_id = p2.product_id
GROUP BY product_a, product_b
ORDER BY times_bought_together DESC;


-- -----------------------------------------------------
-- 2.4 Average quantity ordered per product
-- -----------------------------------------------------
CREATE OR REPLACE VIEW avg_quantity_per_product AS
SELECT
    p.product_name,
    AVG(li.line_item_quantity) AS avg_quantity_per_order
FROM fact_line_items li
JOIN dim_product p
    ON li.product_id = p.product_id
GROUP BY p.product_name
ORDER BY avg_quantity_per_order DESC;


-- -----------------------------------------------------
-- 2.5 Monthly product sales trends
-- -----------------------------------------------------
CREATE OR REPLACE VIEW monthly_product_sales AS
SELECT
    p.product_name,
    d.date_year,
    d.date_month,
    SUM(li.line_item_quantity) AS qty_sold
FROM fact_line_items li
JOIN fact_orders o
    ON li.order_id = o.order_id
JOIN dim_date d
    ON o.order_transaction_date = d.date_value
JOIN dim_product p
    ON li.product_id = p.product_id
GROUP BY p.product_name, d.date_year, d.date_month
ORDER BY p.product_name, d.date_year, d.date_month;
