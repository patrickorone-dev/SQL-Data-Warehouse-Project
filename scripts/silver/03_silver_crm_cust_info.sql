/*
===============================================================================
FILE NAME:          03_silver_crm_cust_info.sql
SCHEMA:             silver
PROCEDURE NAME:     silver.prc_load_crm_cust_info()

PURPOSE:
This procedure loads cleaned customer data from bronze.crm_cust_info
into silver.crm_cust_info.

SOURCE TABLE:
bronze.crm_cust_info

TARGET TABLE:
silver.crm_cust_info

MAIN TRANSFORMATIONS:
- Removes leading/trailing spaces
- Standardizes gender values
- Standardizes marital status values
- Converts dates safely
- Removes NULL customer IDs
- Removes duplicate customer records
- Retains latest customer record using ROW_NUMBER()

BUSINESS RULES:
- One row per customer (cst_id)
- Latest customer record is retained
- Invalid gender values mapped to 'n/a'
- Invalid marital status mapped to 'n/a'

DEPENDENCIES:
- functions.fn_trim_text()
- functions.fn_map_gender()
- functions.fn_map_marital_status()
- functions.fn_safe_to_date()

EXECUTION:
CALL silver.prc_load_crm_cust_info();

AUTHOR:             Patrick Orone
LAYER:              Silver Layer
LOAD TYPE:          Full Refresh
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.prc_load_crm_cust_info()
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
	RAISE NOTICE '>>Starting load for silver.crm_cust_info: ';
	RAISE NOTICE '>>Start Time: % seconds', v_start_time;
	RAISE NOTICE '===========================================';
	
	v_step_ts := clock_timestamp();
	RAISE NOTICE '>>TRUNCATING TABLE: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	RAISE NOTICE '>>TRUNCATING COMPLETED IN: %', clock_timestamp() - v_step_ts;
	RAISE NOTICE '########################################';
	RAISE NOTICE '>>Loading clean data: silver.crm_cust_info';
	RAISE NOTICE '########################################';
	
	WITH source_data AS (
		SELECT
			cst_id,
			cst_key,
			functions.fn_trim_text(cst_firstname) AS cst_firstname,
			functions.fn_trim_text(cst_lastname) AS cst_lastname,
			functions.fn_map_marital_status(cst_marital_status) AS cst_marital_status,
			functions.fn_map_gender(cst_gndr) AS cst_gndr,
			functions.fn_safe_to_date(cst_create_date::TEXT) AS cst_create_date
		FROM bronze.crm_cust_info
	), filtered_data AS (
	    SELECT *
	    FROM source_data
	    WHERE cst_id IS NOT NULL
	), ranked_data AS (
		SELECT
		*,
		ROW_NUMBER() OVER (
			PARTITION BY cst_id ORDER BY cst_create_date DESC NULLS LAST
		)AS rn
		FROM filtered_data
	)
	
	-- SELECT * FROM ranked_data;
	
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	FROM ranked_data
	WHERE rn = 1;
	
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	v_end_time := clock_timestamp();
	RAISE NOTICE 'Rows Loaded: %', v_rows;
	RAISE NOTICE 'crm_cust_info Loading duration: % seconds',
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

CALL silver.prc_load_crm_cust_info();
