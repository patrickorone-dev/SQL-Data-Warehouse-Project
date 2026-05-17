-- ==============================================================
-- PRE-LOAD QUALITY CHECKS
-- Source: bronze.crm_sales_details
-- Purpose: confirm that the bronze data is ready for the silver load.
-- Expectation: Queries below shouldn't return problematic rows.
-- ==============================================================

SELECT * FROM bronze.crm_sales_details;


-- Check for NULL values in the sls_ord_num key
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num IS NULL;

-- Check for Unwanted Spaces in sls_ord_num
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);


-- Data Quality check for sls_prd_key and prd_key
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (select prd_key from silver.crm_prd_info);



-- Data Quality check for sls_cust_id and cst_id
SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (select cst_id from silver.crm_cust_info);

-- Invalid date Check for sls_order_dt
SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0
OR LENGTH(sls_order_dt::TEXT) != 8 -- checks if date has length not equal to 8
OR sls_order_dt > 20500101 -- checks if date is outside the range below
OR sls_order_dt < 19000101;

-- Invalid date Check for sls_ship_dt
SELECT 
NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <=0
OR LENGTH(sls_ship_dt::TEXT) != 8 -- checks if date has length not equal to 8
OR sls_ship_dt > 20500101 -- checks if date is outside the range below
OR sls_ship_dt < 19000101;


-- Invalid date Check for sls_due_dt
SELECT 
NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <=0
OR LENGTH(sls_due_dt::TEXT) != 8 -- checks if date has length not equal to 8
OR sls_due_dt > 20500101 -- checks if date is outside the range below
OR sls_due_dt < 19000101;



-- =============================================================
-- Check NUll values
--  Check if sls_sales = sls_quantity*sls_price
-- Check for negative, zero values in sls_sales, sls_quantity and sls_price
-- =============================================================

SELECT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;