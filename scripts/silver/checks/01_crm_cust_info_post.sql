-- =====================================================
-- POST-LOAD QUALITY CHECKS
-- Target: silver.crm_cust_info
-- Purpose: confirm cleaning and loading worked correctly
-- Expectation: these queries should return no problematic rows
-- =====================================================

-- 1. Null primary keys
SELECT cst_id
FROM silver.crm_cust_info
WHERE cst_id IS NULL;

-- 2. Duplicate primary keys
SELECT cst_id, COUNT(*) AS row_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- 3. Untrimmed names
SELECT cst_id, cst_firstname, cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)
   OR cst_lastname  <> TRIM(cst_lastname);

-- 4. Invalid marital status after standardization
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status NOT IN ('Single', 'Married', 'n/a');

-- 5. Invalid gender after standardization
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr NOT IN ('Female', 'Male', 'n/a');

-- 6. Compare bronze vs silver row counts
SELECT
    (SELECT COUNT(*) FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL) AS bronze_rows_with_id,
    (SELECT COUNT(*) FROM silver.crm_cust_info) AS silver_rows;
