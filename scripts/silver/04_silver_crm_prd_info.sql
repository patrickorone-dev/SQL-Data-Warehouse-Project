/*
===============================================================================
FILE NAME:          03_silver_crm_prd_info.sql
SCHEMA:             silver
PROCEDURE NAME:     silver.prc_load_crm_prd_info()

PURPOSE:
This procedure loads cleaned customer data from bronze.crm_prd_info
into silver.crm_prd_info.

SOURCE TABLE:
bronze.crm_prd_info

TARGET TABLE:
silver.crm_prd_info

MAIN TRANSFORMATIONS:
- Standardizes prd_line values
- Converts dates safely
- Removes NULL product IDs
- Derived New Columns
- Data Transformation.

BUSINESS RULES:
-prd_cost values are 0 if null.

DEPENDENCIES:
- functions.fn_safe_date()
- functions.fn_map_prd_line()

EXECUTION:
CALL silver.prc_load_crm_prd_info();

AUTHOR:             Patrick Orone
LAYER:              Silver Layer
LOAD TYPE:          Full Refresh
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.prc_load_crm_prd_info()
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
	RAISE NOTICE '>>Starting load for silver.crm_prd_info: ';
	RAISE NOTICE '>>Start Time: % seconds', v_start_time;
	RAISE NOTICE '===========================================';
	
	v_step_ts := clock_timestamp();
	RAISE NOTICE '>>TRUNCATING TABLE: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	RAISE NOTICE '>>TRUNCATING COMPLETED IN: %', clock_timestamp() - v_step_ts;
	RAISE NOTICE '########################################';
	RAISE NOTICE '>>Loading clean data: silver.crm_prd_info';
	RAISE NOTICE '########################################';
	
	
	
	WITH source_data AS (
		SELECT
			prd_id,
			prd_key,
			prd_nm,
			COALESCE(prd_cost, 0) AS prd_cost,
			functions.fn_map_prd_line(prd_line) AS prd_line,
			functions.fn_safe_to_date(prd_start_dt::TEXT) AS prd_start_dt,
			functions.fn_safe_to_date(prd_end_dt::TEXT) AS prd_end_dt
		FROM bronze.crm_prd_info
	), enriched_data AS (
		SELECT
			prd_id,
			-- Deriving new columns cat_id from prd_key.
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			-- Deriving a new prd_key from the old prd_key
			SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt
		FROM source_data
	)
	
	
	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT * FROM enriched_data;

	
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	v_end_time := clock_timestamp();
	RAISE NOTICE 'Rows Loaded: %', v_rows;
	RAISE NOTICE 'crm_prd_info Loading duration: % seconds',
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

call silver.prc_load_crm_prd_info();
