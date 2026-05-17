/*
===============================================================================
FILE NAME:          05_silver_crm_sales_details.sql
SCHEMA:             silver
PROCEDURE NAME:     silver.prc_load_crm_sales_details()

PURPOSE:
This procedure loads cleaned customer data from bronze.crm_sales_details
into silver.crm_sales_details.

SOURCE TABLE:
bronze.crm_sales_details

TARGET TABLE:
silver.crm_sales_details

MAIN TRANSFORMATIONS:
- Handled Inconsistent Data Formats Text to Date
- Identified incorrect dates.
- Handles null values for Dates
- Fixing negative Values
- Handling Zeros
- Retains latest customer record using ROW_NUMBER()

BUSINESS RULES:
- Invalid dates mapped to 'n/a'
Business RULES
sales = Quantity * price
therefore negative numbers, zeros, null are not allowed. 

Rules for cleaning and transforming sls_sales, sls_quantity and sls_price
	if Sales is negative, zero or null derive it using quantity and price
	if price is zero or null, calculate it using sales and quantity
	if price is negative, convert it to a positive value. 

DEPENDENCIES:
- functions.fn_map_sls_date()

EXECUTION:
CALL silver.prc_load_crm_sales_details();

AUTHOR:             Patrick Orone
LAYER:              Silver Layer
LOAD TYPE:          Full Refresh
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.prc_load_crm_sales_details()
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
	RAISE NOTICE '>>Starting load for silver.crm_sales_details: ';
	RAISE NOTICE '>>Start Time: % seconds', v_start_time;
	RAISE NOTICE '===========================================';
	
	v_step_ts := clock_timestamp();
	RAISE NOTICE '>>TRUNCATING TABLE: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	RAISE NOTICE '>>TRUNCATING COMPLETED IN: %', clock_timestamp() - v_step_ts;
	RAISE NOTICE '########################################';
	RAISE NOTICE '>>Loading clean data: silver.crm_prd_info';
	RAISE NOTICE '########################################';
	
	
	
	WITH transformed_source_data AS (
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			functions.fn_map_sls_date(sls_order_dt) as sls_order_dt,
			functions.fn_map_sls_date(sls_ship_dt) as sls_ship_dt,
			functions.fn_map_sls_date(sls_due_dt) as sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		From bronze.crm_sales_details
	), standardized_data AS (
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			/*
				Business RULES
				sales = Quantity * price
				therefore negative numbers, zeros, null are not allowed. 
				
				Rules for cleaning and transforming sls_sales, sls_quantity and sls_price
					if Sales is negative, zero or null derive it using quantity and price
					if price is zero or null, calculate it using sales and quantity
					if price is negative, convert it to a positive value. 
			*/
			CASE WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity*ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price
			
	
		FROM transformed_source_data
	)
	
	INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	FROM standardized_data;

	
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	v_end_time := clock_timestamp();
	RAISE NOTICE 'Rows Loaded: %', v_rows;
	RAISE NOTICE 'crm_sales_details Loading duration: % seconds',
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

call silver.prc_load_crm_sales_details();
