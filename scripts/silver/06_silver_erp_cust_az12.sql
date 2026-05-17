/*
===============================================================================
FILE NAME:          06_silver_erp_cust_az12.sql
SCHEMA:             silver
PROCEDURE NAME:     silver.prc_load_erp_cust_az12()

PURPOSE:
This procedure loads cleaned customer data from bronze.erp_cust_az12
into silver.erp_cust_az12.

SOURCE TABLE:
bronze.erp_cust_az12

TARGET TABLE:
silver.erp_cust_az12

MAIN TRANSFORMATIONS:
- Standardizes gender values
- Converts dates safely
- Cleans category IDs

BUSINESS RULES:
- One row per category (cid)

DEPENDENCIES:
- functions.fn_map_gender()

EXECUTION:
CALL silver.prc_load_erp_cust_az12();

AUTHOR:             Patrick Orone
LAYER:              Silver Layer
LOAD TYPE:          Full Refresh
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.prc_load_erp_cust_az12()
LANGUAGE plpgsql
AS $$
DECLARE
v_start_time TIMESTAMP;
v_end_time TIMESTAMP;
v_step_ts TIMESTAMP;

v_rows INT;

v_error_message TEXT;
v_error_state TEXT;

BEGIN

	v_start_time := clock_timestamp();
	RAISE NOTICE '==========================================';
	RAISE NOTICE '>>Starting load for silver.erp_cust_az12: ';
	RAISE NOTICE '>>Start Time: % seconds', v_start_time;
	RAISE NOTICE '===========================================';
	
	v_step_ts := clock_timestamp();
	RAISE NOTICE '>>TRUNCATING TABLE: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	RAISE NOTICE '>>TRUNCATING COMPLETED IN: %', clock_timestamp() - v_step_ts;
	RAISE NOTICE '########################################';
	RAISE NOTICE '>>Loading clean data: silver.erp_cust_az12';
	RAISE NOTICE '########################################';
	
	
	
	WITH source_data AS (
		SELECT 
			cid,
			bdate,
			functions.fn_map_gender(gen) AS gen
		FROM bronze.erp_cust_az12
	), transformed_data AS (
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
				ELSE cid
			END as cid,
			CASE WHEN bdate > CURRENT_DATE THEN NULL
				ELSE bdate
			END AS bdate,
			gen
		FROM source_data
	)
	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)
	SELECT
		cid,
		bdate,
		gen
	FROM transformed_data;

	
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	v_end_time := clock_timestamp();
	RAISE NOTICE 'Rows Loaded: %', v_rows;
	RAISE NOTICE 'erp_cust-az12 Loading duration: % seconds',
	EXTRACT(EPOCH FROM (v_end_time-v_start_time));
	RAISE NOTICE '===========================================';
	
	EXCEPTION
		WHEN others THEN
			v_error_message := SQLERRM;
			v_error_state := SQLSTATE;
	
			RAISE NOTICE '==========================================';
			RAISE NOTICE '>>Error Message: %', v_error_message;
			RAISE NOTICE '>>Error State: %', v_error_state;
			RAISE NOTICE '==========================================';

END;
$$;

call silver.prc_load_erp_cust_az12();
