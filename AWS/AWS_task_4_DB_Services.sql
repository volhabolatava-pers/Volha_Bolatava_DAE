-- 1. Creating and selecting the working schema
CREATE SCHEMA IF NOT EXISTS dilab_dev;
USE dilab_dev;

-- 2. Creating the staging table for sales data
CREATE TABLE IF NOT EXISTS sales_staging (
    sale_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    sale_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Inserting initial records
INSERT INTO sales_staging (product_name, category, amount, sale_date)
VALUES 
    ('Product A', 'Electronics', 150.00, CURDATE()),
    ('Product B', 'Electronics', 200.50, CURDATE()),
    ('Product C', 'Home & Kitchen', 45.00, CURDATE())
ON DUPLICATE KEY UPDATE amount = VALUES(amount);

-- Verifying raw staging data
SELECT * FROM sales_staging;

-- 4. Creating a view for daily category summaries
CREATE OR REPLACE VIEW v_daily_sales_summary AS
SELECT 
    category,
    COUNT(sale_id) AS total_transactions,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_check
FROM sales_staging
GROUP BY category;

-- Checking aggregated view results
SELECT * FROM v_daily_sales_summary;

-- 5. Creating a stored procedure for new sales entry
DROP PROCEDURE IF EXISTS sp_add_new_sale;

CREATE PROCEDURE sp_add_new_sale(
    IN p_product_name VARCHAR(100),
    IN p_category VARCHAR(50),
    IN p_amount DECIMAL(10, 2),
    IN p_sale_date DATE
)
BEGIN
    INSERT INTO sales_staging (product_name, category, amount, sale_date)
    VALUES (p_product_name, p_category, p_amount, p_sale_date);
END;

-- 6. Executing the procedure and viewing updated metrics
CALL sp_add_new_sale('Product D', 'Home & Kitchen', 89.99, CURDATE());
SELECT * FROM v_daily_sales_summary;