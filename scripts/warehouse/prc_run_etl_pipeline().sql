CREATE OR REPLACE PROCEDURE warehouse.prc_run_etl_pipeline()
LANGUAGE plpgsql
AS $$
DECLARE
v_pipeline_start TIMESTAMP;
v_pipeline_end TIMESTAMP;

v_error_message TEXT;
v_error_state TEXT;
BEGIN
	v_pipeline_start := clock_timestamp();
	RAISE NOTICE '******************************************************';
	RAISE NOTICE 'STARTING FULL ETL PIPELINE: ';
	RAISE NOTICE '>>Start Time: % seconds', v_pipeline_start;
	RAISE NOTICE '******************************************************';

	-- LOAD BRONZE LAYER
	RAISE NOTICE 'Running bronze.load_bronze(): ';
	call bronze.load_bronze();
	RAISE NOTICE 'Bronze Layer: completed: ';

	-- LOAD SILVER LAYER
	RAISE NOTICE 'Running silver.prc_load_silver_all(): ';
	call silver.prc_load_silver_all();
	RAISE NOTICE 'Silver Layer: completed: ';
	
	v_pipeline_end := clock_timestamp();
	RAISE NOTICE '******************************************************';
	RAISE NOTICE '>>END OF FULL PIPELINE: ';
	RAISE NOTICE '>>END TIME: % seconds', v_pipeline_end;
	RAISE NOTICE '>>TOTAL DURATION: % seconds',EXTRACT(EPOCH FROM (v_pipeline_end - v_pipeline_start));
	RAISE NOTICE '******************************************************';

	EXCEPTION
		WHEN OTHERS THEN
			v_error_message := SQLERRM;
			v_error_state := SQLSTATE;
			RAISE NOTICE 'FULL ETL PIPELINE FAILED...............';
			RAISE NOTICE 'Error Message: %', v_error_message;
			RAISE NOTICE 'Error State: %', v_error_state;

END;
$$;
