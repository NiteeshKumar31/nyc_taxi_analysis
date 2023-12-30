--- Create exteranal silver vendor table
CREATE OR ALTER PROCEDURE SILVER.usp_silver_vendor_info
AS
BEGIN
IF OBJECT_ID('SILVER.vendor_info') IS NOT NULL
    DROP EXTERNAL TABLE SILVER.vendor_info;
CREATE EXTERNAL TABLE SILVER.vendor_info
WITH
(
        LOCATION = '/silver/vendor',
        DATA_SOURCE = NYC_TAXI_SRC,
        FILE_FORMAT = parquet_file_format
)
AS
SELECT 
* 
FROM 
BRONZE.vendor_info
END
---
-- SELECT COUNT(1) FROM BRONZE.vendor_info
-- MINUS
-- SELECT COUNT(1) FROM SILVER.vendor_info

-- ----
-- SELECT * FROM BRONZE.vendor_info
-- MINUS 
-- SELECT * FROM SILVER.vendor_info