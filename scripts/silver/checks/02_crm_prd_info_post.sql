-- ============================================================
-- POST-LOAD QUALITY CHECKS
-- Target: silver.crm_prd_info
-- Purpose: confirm cleaning and loading worked correctly
-- Expectation: these queries should return no problematic rows
-- ============================================================

-- 1. Check for Null in primary keys
SELECT prd_id
FROM silver.crm_prd_info
WHERE prd_id IS NULL;

SELECT prd_key
FROM silver.crm_prd_info
WHERE prd_key IS NULL;

-- 2. Duplicate primary keys
SELECT prd_id, COUNT(*) AS row_count
FROM silver.crm_prd_info
WHERE prd_id IS NOT NULL
GROUP BY prd_id
HAVING COUNT(*) > 1;

SELECT prd_key, COUNT(*) as row_count
FROM silver.crm_prd_info
WHERE prd_Key IS NOT NULL
GROUP BY prd_key
HAVING COUNT(*) > 1;


-- 3. Leading/trailing spaces in prd_nm
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-- 4. Checking Invalid dates for prd_start_dt & prd_end_dt
SELECT 
functions.fn_safe_to_date(prd_start_dt::TEXT) AS prd_start_dt
FROM silver.crm_prd_info
WHERE prd_start_dt IS NULL;

SELECT 
functions.fn_safe_to_date(prd_end_dt::TEXT) AS prd_end_dt
FROM silver.crm_prd_info
WHERE prd_end_dt IS NULL;

-- 5. Invalid gender after standardization
SELECT DISTINCT prd_line
FROM silver.crm_prd_info
WHERE prd_line NOT IN ('Mountain', 'Road', 'Other Sales', 'Touring', 'n/a');

-- 6. Compare silver vs silver row counts
SELECT
    (SELECT COUNT(*) FROM silver.crm_prd_info WHERE prd_id IS NOT NULL) AS silver_rows_with_id,
    (SELECT COUNT(*) FROM silver.crm_prd_info) AS silver_rows;


SELECT * FROM silver.crm_prd_info;