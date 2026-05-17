-- ==============================================================
-- PRE-LOAD QUALITY CHECKS
-- Source: bronze.crm_prd_info
-- Purpose: confirm that the bronze data is ready for the silver load.
-- Expectation: Queries below shouldn't return problematic rows.
-- ==============================================================


-- PRINT ENTIRE TABLE
SELECT * FROM bronze.crm_prd_info;

-- Check Null primary Keys.
SELECT prd_id
FROM bronze.crm_prd_info
WHERE prd_id IS NULL;

SELECT prd_key
FROM bronze.crm_prd_Info
WHERE prd_key IS NULL;


-- 2. Duplicate primary keys
SELECT prd_id, COUNT(*) AS row_count
FROM bronze.crm_prd_info
WHERE prd_id IS NOT NULL
GROUP BY prd_id
HAVING COUNT(*) > 1;

SELECT prd_key, COUNT(*) as row_count
FROM bronze.crm_prd_info
WHERE prd_Key IS NOT NULL
GROUP BY prd_key
HAVING COUNT(*) > 1;

-- 3. Leading/trailing spaces in prd_nm
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-- 4. Checking Invalid dates for prd_start_dt & prd_end_dt
SELECT 
functions.fn_safe_to_date(prd_start_dt::TEXT) AS prd_start_dt
FROM bronze.crm_prd_info
WHERE prd_start_dt IS NULL;

SELECT 
functions.fn_safe_to_date(prd_end_dt::TEXT) AS prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_end_dt IS NULL;
