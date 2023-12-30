CREATE OR ALTER PROCEDURE SILVER.usp_silver_rate_code
AS
BEGIN
IF OBJECT_ID('SILVER.rate_code') IS NOT NULL
    DROP EXTERNAL TABLE SILVER.rate_code;
CREATE EXTERNAL TABLE SILVER.rate_code
WITH
(
        LOCATION = '/silver/rate_code',
        DATA_SOURCE = NYC_TAXI_SRC,
        FILE_FORMAT = parquet_file_format
)
AS
SELECT 
* 
FROM 
BRONZE.rate_code_vw
END
--- 

-- select count(1) from BRONZE.rate_code_vw
-- MINUS
-- select * from SILVER.rate_code