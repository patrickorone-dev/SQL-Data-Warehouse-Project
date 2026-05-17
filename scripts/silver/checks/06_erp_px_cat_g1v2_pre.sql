-- ==============================================================
-- PRE-LOAD QUALITY CHECKS
-- Source: bronze.erp_px_cat_g1v2
-- Purpose: confirm that the bronze data is ready for the silver load.
-- Expectation: Queries below shouldn't return problematic rows.
-- ==============================================================

select * from bronze.erp_px_cat_g1v2;

select cat_id from silver.crm_prd_info;
-- Data quality check in id and cat_id from silver.crm_prd_info 
SELECT
id
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (select cat_id from silver.crm_prd_info);

-- Check for unwanted spaces from cat subcat and maintence.
select
cat
FROM bronze.erp_px_cat_g1v2
WHERE cat <> TRIM(cat);

select
subcat
FROM bronze.erp_px_cat_g1v2
WHERE subcat <> TRIM(subcat);

select
maintenance
FROM bronze.erp_px_cat_g1v2
WHERE maintenance <> TRIM(maintenance);

-- Check categories of maintenance
SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2;

-- Check categories of cat
SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2;
