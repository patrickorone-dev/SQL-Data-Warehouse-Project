-- ============================================================
-- POST-LOAD QUALITY CHECKS
-- Target: silver.crm_sales_details
-- Purpose: confirm cleaning and loading worked correctly
-- Expectation: these queries should return no problematic rows
-- ============================================================



-- Data Quality check for sls_prd_key and prd_key
SELECT *
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (select prd_key from silver.crm_prd_info);



-- Data Quality check for sls_cust_id and cst_id
SELECT *
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (select cst_id from silver.crm_cust_info);


-- =============================================================
-- Check NUll values
--  Check if sls_sales = sls_quantity*sls_price
-- Check for negative, zero values in sls_sales, sls_quantity and sls_price
-- =============================================================

SELECT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


select * from silver.crm_sales_details;