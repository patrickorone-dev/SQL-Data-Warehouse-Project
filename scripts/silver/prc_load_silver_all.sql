/*
===============================================================================
PROCEDURE: silver.prc_load_silver_all()

PURPOSE:
This procedure orchestrates all silver-layer transformations.

ROLE IN PIPELINE:
Second stage of ETL after bronze.load_bronze().

WHAT IT DOES:
- Calls all individual silver ETL procedures
- Cleans and standardizes data
- Removes duplicates
- Applies business rules

DEPENDENCIES:
- bronze.load_bronze()
- silver individual ETL procedures
- helper functions

OUTPUT:
Populates all silver tables with cleaned data.

EXECUTION:
CALL silver.prc_load_silver_all();
===============================================================================
*/
CREATE OR REPLACE PROCEDURE silver.prc_load_silver_all()
LANGUAGE plpgsql
AS $$
DECLARE
v_silver_start TIMESTAMP;
v_silver_end TIMESTAMP;

v_error_message TEXT;
v_error_state TEXT;

BEGIN
	v_silver_start := clock_timestamp();
	RAISE NOTICE '===============================';
	RAISE NOTICE '>>LOADING SILVER LAYER';
	RAISE NOTICE '===============================';

	RAISE NOTICE '###############################';
	RAISE NOTICE '>>LOADING CRM TABLES';
	RAISE NOTICE '###############################';

	CALL silver.prc_load_crm_cust_info();
	call silver.prc_load_crm_prd_info();	
	call silver.prc_load_crm_sales_details();

	
	RAISE NOTICE'###############################';
	RAISE NOTICE '>>LOADING ERP TABLES';
	RAISE NOTICE '###############################';

	
	call silver.prc_load_erp_cust_az12();
	CALL silver.prc_load_erp_loc_a101();
	call silver.prc_load_erp_px_cat_g1v2();
	RAISE NOTICE '##########LOADED SUCCESSFULLY###########';
	v_silver_end := clock_timestamp();
	RAISE NOTICE '>>SILVER LAYER Loading duration: %seconds',
	EXTRACT(EPOCH FROM (v_silver_end - v_silver_start));
	
	EXCEPTION
		WHEN OTHERS THEN
			v_error_message := SQLERRM;
			v_error_state := SQLSTATE;
			RAISE NOTICE 'FULL SILVER LOAD FAILED...............';
			RAISE NOTICE 'Error Message: %', v_error_message;
			RAISE NOTICE 'Error State: %', v_error_state;
END;
$$;
