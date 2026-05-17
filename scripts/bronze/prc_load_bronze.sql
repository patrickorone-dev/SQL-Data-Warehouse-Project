/*
=======================================================================================
Stored Procedure: Load bronze Layer (Source --> Bronze)
=======================================================================================
SCRIPT PURPOSE:
  This stored procedure loads data into the 'bronze' schema from external csv files.
  It performs the following actions:
    Truncate the bronze tables before loading data.
    Uses the 'COPY' command to load data from csv files to bronze tables. 

PARAMETERS:
  None:
  This stored procedure doesnot accept any parameters or return any values.

USAGE EXAMPLE:
  CALL bronze.load_bronze();
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
v_start_time TIMESTAMP;
v_end_time TIMESTAMP;
v_rows INT;

v_bronze_start TIMESTAMP;
v_bronze_end TIMESTAMP;

v_error_message TEXT;
v_error_state TEXT;

BEGIN

	v_bronze_start := clock_timestamp();
	RAISE NOTICE '===============================';
	RAISE NOTICE 'LOADING BRONZE LAYER';
	RAISE NOTICE '===============================';

	RAISE NOTICE '###############################';
	RAISE NOTICE 'LOADING CRM TABLES';
	RAISE NOTICE '###############################';

	v_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	RAISE NOTICE '>> Inserting Data into: bronze.crm_cust_info';
	COPY bronze.crm_cust_info
	FROM 'C:\sql\dwh_project\datasets\source_crm\cust_info.csv'
	WITH (
	FORMAT CSV, 
	HEADER TRUE
	);
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	v_end_time := clock_timestamp();
	RAISE NOTICE 'Rows Loaded: %', v_rows;
	RAISE NOTICE 'crm_cust_info Loading duration: %seconds',
	EXTRACT(EPOCH FROM (v_end_time-v_start_time));
	RAISE NOTICE '';

	---===============================
	--CRM PRODUCT INFO
	---===============================
	v_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
	RAISE NOTICE '>> Inserting Data into: bronze.crm_prd_info';
	COPY bronze.crm_prd_info
	FROM 'C:\sql\dwh_project\datasets\source_crm\prd_info.csv'
	WITH (
	FORMAT CSV, 
	HEADER TRUE
	);
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	v_end_time := clock_timestamp();
	RAISE NOTICE 'Rows Loaded: %', v_rows;
	RAISE NOTICE 'crm_prd_info Loading duration: %seconds',
	EXTRACT(EPOCH FROM (v_end_time-v_start_time));
	RAISE NOTICE '';

	---===============================
	--CRM SALES DETAILS
	---===============================
	v_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;
	RAISE NOTICE '>> Inserting Data into: bronze.crm_sales_details';
	COPY bronze.crm_sales_details
	FROM 'C:\sql\dwh_project\datasets\source_crm\sales_details.csv'
	WITH (
	FORMAT CSV, 
	HEADER TRUE
	);
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	v_end_time := clock_timestamp();
	RAISE NOTICE 'Rows Loaded: %', v_rows;
	RAISE NOTICE 'crm_sales_details Loading duration: %seconds',
	EXTRACT(EPOCH FROM (v_end_time-v_start_time));
	RAISE NOTICE '##########LOADED SUCCESSFULLY##########';
	RAISE NOTICE '';
	
	---===============================
	--ERP CUSTOMER
	---===============================
	RAISE NOTICE'###############################';
	RAISE NOTICE 'LOADING ERP TABLES';
	RAISE NOTICE '###############################';

	v_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	RAISE NOTICE '>> Inserting Data into: bronze.erp_cust_az12';
	COPY bronze.erp_cust_az12
	FROM 'C:\sql\dwh_project\datasets\source_erp\cust_az12.csv'
	WITH (
	FORMAT CSV, 
	HEADER TRUE
	);
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	v_end_time := clock_timestamp();
	RAISE NOTICE 'Rows Loaded: %', v_rows;
	RAISE NOTICE 'erp_cust_az12 Loading duration: %seconds',
	EXTRACT(EPOCH FROM (v_end_time-v_start_time));
	RAISE NOTICE '';

	---===============================
	--LOADING ERP TABLES
	---===============================
	v_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;
	RAISE NOTICE '>> Inserting Data into: bronze.erp_loc_a101';
	COPY bronze.erp_loc_a101
	FROM 'C:\sql\dwh_project\datasets\source_erp\loc_a101.csv'
	WITH (
	FORMAT CSV, 
	HEADER TRUE
	);
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	v_end_time := clock_timestamp();
	RAISE NOTICE 'Rows Loaded: %', v_rows;
	RAISE NOTICE 'erp_loc_a101 Loading duration: %seconds',
	EXTRACT(EPOCH FROM (v_end_time-v_start_time));
	RAISE NOTICE '';
	
	---===============================
	--ERP CATEGORY
	---===============================
	v_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	RAISE NOTICE '>> Inserting Data into: bronze.erp_px_cat_g1v2';
	COPY bronze.erp_px_cat_g1v2
	FROM 'C:\sql\dwh_project\datasets\source_erp\px_cat_g1v2.csv'
	WITH (
	FORMAT CSV, 
	HEADER TRUE
	);
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	v_end_time := clock_timestamp();
	RAISE NOTICE 'Rows Loaded: %', v_rows;
	RAISE NOTICE 'erp_px_cat_g1v2 Loading duration: %seconds',
	EXTRACT(EPOCH FROM (v_end_time-v_start_time));
	RAISE NOTICE '';

	v_bronze_end := clock_timestamp();
	RAISE NOTICE 'BRONZE LAYER Loading duration: %seconds',
	EXTRACT(EPOCH FROM (v_bronze_end - v_bronze_start));
	RAISE NOTICE '##########LOADED SUCCESSFULLY###########';

	EXCEPTION
		WHEN OTHERS THEN
			v_error_message := SQLERRM;
			v_error_state := SQLSTATE;

			RAISE NOTICE 'Error Message: %', v_error_message;
			RAISE NOTICE 'Error State: %', v_error_state;

END;
$$;



