/*
========================================================================================
Create Database and Schemas
========================================================================================

This script creates a new database named "DataWarehouse", after checking it already exists
If the database exists, it is dropped and recreated. Additionally the script creates 3 
schemas within the database: bronze, silver and gold.

WARNING
  Running this script will drop the entire "DataWarehouse" database if it exists. 
  All data in the database will be deleted, PROCEED WITH CAUTION.
  and esure you HAVE PROPER BACKUPS before running this script. 

*/


--connect to the default postgres database first
--Terminate active connections to the database.
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'DataWarehouse'
AND pid <> pg_backend_pid();

--Drop the database if it exists.
DROP DATABASE IF EXISTS "DataWarehouse";

--Recreate the database
CREATE DATABASE "DateWarehouse";

--Connect to the new database

\c DataWarehouse

--Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

















