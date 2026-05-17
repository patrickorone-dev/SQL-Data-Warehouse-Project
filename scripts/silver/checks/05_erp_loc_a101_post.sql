-- ============================================================
-- POST-LOAD QUALITY CHECKS
-- Target: silver.erp_loc_a101
-- Purpose: confirm cleaning and loading worked correctly
-- Expectation: these queries should return no problematic rows
-- ============================================================

select * from silver.erp_loc_a101;

-- Check for data quality in cid and cst_key
SELECT distinct
cid 
FROM silver.erp_loc_a101
where cid NOT IN (select cst_key from silver.crm_cust_info);


-- Check data standardization for country column.
SELECT DISTINCT
country
FROM silver.erp_loc_a101;
