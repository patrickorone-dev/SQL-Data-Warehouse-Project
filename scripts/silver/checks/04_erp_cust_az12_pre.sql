-- ==============================================================
-- PRE-LOAD QUALITY CHECKS
-- Source: bronze.erp_cust_az12
-- Purpose: confirm that the bronze data is ready for the silver load.
-- Expectation: Queries below shouldn't return problematic rows.
-- ==============================================================

select * from bronze.erp_cust_az12;

-- Check duplicates AND null Values in cid 
SELECT 
cid, COUNT(*) as row_count
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1 OR cid is NULL;

-- Check data Quality for cid and cst_key
SELECT
cid
FROM bronze.erp_cust_az12
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- check for invalid dates. checks for the range. 
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate IS NULL OR bdate < '1920-01-01' OR bdate > CURRENT_DATE;

-- Check categories of gen
SELECT distinct
gen
FROM bronze.erp_cust_az12;



