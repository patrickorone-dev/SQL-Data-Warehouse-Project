-- ==============================================================
-- PRE-LOAD QUALITY CHECKS
-- Source: bronze.erp_loc_a101
-- Purpose: confirm that the bronze data is ready for the silver load.
-- Expectation: Queries below shouldn't return problematic rows.
-- ==============================================================

select * from bronze.erp_loc_a101;

-- Check for data quality in cid and cst_key
SELECT distinct
cid 
FROM bronze.erp_loc_a101;
where cid NOT IN (select cst_key from silver.crm_cust_info);


-- Check data standardization for country column.
SELECT DISTINCT
country
FROM bronze.erp_loc_a101;

select cst_key from silver.crm_cust_info;