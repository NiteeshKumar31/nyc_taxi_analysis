--- Create exteranal silver trip type table

IF OBJECT_ID('SILVER.trip_type') IS NOT NULL
    DROP EXTERNAL TABLE SILVER.trip_type
CREATE EXTERNAL TABLE SILVER.trip_type
WITH
(
        LOCATION = '/silver/trip_type',
        DATA_SOURCE = NYC_TAXI_SRC,
        FILE_FORMAT = parquet_file_format
)
AS
SELECT 
* 
FROM 
BRONZE.trip_type

--- 

select * from BRONZE.trip_type
MINUS
select * from SILVER.trip_type
