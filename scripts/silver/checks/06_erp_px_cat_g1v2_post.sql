-- ============================================================
-- POST-LOAD QUALITY CHECKS
-- Target: silver.erp_px_cat_g1v2
-- Purpose: confirm cleaning and loading worked correctly
-- Expectation: these queries should return no problematic rows
-- ============================================================

-- ALL DATA IS UPTO DATE

select * from silver.erp_px_cat_g1v2;