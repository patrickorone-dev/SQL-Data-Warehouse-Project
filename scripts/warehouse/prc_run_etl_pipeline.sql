/*
===============================================================================
PROCEDURE: warehouse.prc_run_etl_pipeline()

PURPOSE:
Master orchestration procedure that runs the entire data warehouse ETL pipeline.

ROLE IN ARCHITECTURE:
This is the CONTROL TOWER of the system.

PIPELINE FLOW:
1. bronze.load_bronze()  --> Load raw data
2. silver.prc_load_silver_all() --> Clean and transform data

FEATURES:
- End-to-end pipeline execution
- Logging of pipeline start/end
- Error handling across layers
- Execution timing tracking

DEPENDENCIES:
- bronze.load_bronze()
- silver.prc_load_silver_all()

EXECUTION:
CALL warehouse.prc_run_etl_pipeline();

USAGE:
This is the ONLY procedure needed to run full ETL pipeline.
===============================================================================
*/
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
