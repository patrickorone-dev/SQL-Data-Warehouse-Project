-- ============================================================
-- POST-LOAD QUALITY CHECKS
-- Target: silver.erp_cust_az12
-- Purpose: confirm cleaning and loading worked correctly
-- Expectation: these queries should return no problematic rows
-- ============================================================

-- Check data Quality for cid and cst_key
SELECT
cid
FROM silver.erp_cust_az12
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);


-- check for invalid dates. checks for the range. 
SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate IS NULL OR bdate < '1920-01-01' OR bdate > CURRENT_DATE;


-- Check categories of gen
SELECT distinct
gen
FROM silver.erp_cust_az12;

select * from silver.erp_cust_az12;
