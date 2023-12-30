--- Create exteranal silver calendar table

IF OBJECT_ID('SILVER.calendar_info') IS NOT NULL
    DROP EXTERNAL TABLE SILVER.calendar_info
CREATE EXTERNAL TABLE SILVER.calendar_info
WITH
(
        LOCATION = '/silver/calendar',
        DATA_SOURCE = NYC_TAXI_SRC,
        FILE_FORMAT = parquet_file_format
)
AS
SELECT 
* 
FROM 
BRONZE.calendar_info

---
SELECT COUNT(1) FROM BRONZE.calendar_info
MINUS
SELECT COUNT(1) FROM SILVER.calendar_info

----
SELECT * FROM BRONZE.calendar_info
MINUS 
SELECT * FROM SILVER.calendar_info