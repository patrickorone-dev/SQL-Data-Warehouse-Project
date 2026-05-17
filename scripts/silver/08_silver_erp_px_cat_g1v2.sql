CREATE OR REPLACE PROCEDURE silver.prc_load_erp_px_cat_g1v2()
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
	RAISE NOTICE '>>Starting load for silver.erp_px_cat_g1v2: ';
	RAISE NOTICE '>>Start Time: % seconds', v_start_time;
	RAISE NOTICE '===========================================';
	
	v_step_ts := clock_timestamp();
	RAISE NOTICE '>>TRUNCATING TABLE: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	RAISE NOTICE '>>TRUNCATING COMPLETED IN: %', clock_timestamp() - v_step_ts;
	RAISE NOTICE '########################################';
	RAISE NOTICE '>>Loading clean data: silver.erp_px_cat_g1v2';
	RAISE NOTICE '########################################';
	
	
	WITH source_data AS (
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2
	), transformed_data AS ( -- No transformation occured
		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM source_data
	)
	
	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT
		id,
		cat,
		subcat,
		maintenance
	FROM transformed_data;

	
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	v_end_time := clock_timestamp();
	RAISE NOTICE 'Rows Loaded: %', v_rows;
	RAISE NOTICE 'erp_px_cat_g1v2 Loading duration: % seconds',
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

call silver.prc_load_erp_px_cat_g1v2();