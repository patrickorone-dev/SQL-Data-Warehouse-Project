-- =====================================================
-- PRE-LOAD QUALITY CHECKS
-- Source: bronze.crm_cust_info
-- Purpose: confirm bronze data is ready for silver load
-- Expectation: these queries should return no problematic rows
-- =====================================================

-- 1. Null primary keys
SELECT cst_id
FROM bronze.crm_cust_info
WHERE cst_id IS NULL;

-- 2. Duplicate primary keys
SELECT cst_id, COUNT(*) AS row_count
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- 3. Leading/trailing spaces in first or last name
SELECT cst_id, cst_firstname, cst_lastname
FROM bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)
   OR cst_lastname  <> TRIM(cst_lastname);

-- 4. Unexpected marital status codes
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status IS NOT NULL
  AND UPPER(TRIM(cst_marital_status)) NOT IN ('S', 'M');

-- 5. Unexpected gender codes
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr IS NOT NULL
  AND UPPER(TRIM(cst_gndr)) NOT IN ('F', 'M');

-- 6. Invalid create_date values
SELECT cst_id, cst_create_date
FROM bronze.crm_cust_info
WHERE cst_create_date IS NOT NULL
  AND functions.fn_safe_to_date(cst_create_date::TEXT) IS NULL;