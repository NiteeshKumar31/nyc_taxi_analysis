CREATE OR ALTER PROCEDURE SILVER.usp_silver_payment_type
AS
BEGIN
IF OBJECT_ID('SILVER.payment_type') IS NOT NULL
    DROP EXTERNAL TABLE SILVER.payment_type;
CREATE EXTERNAL TABLE SILVER.payment_type
WITH
(
        LOCATION = '/silver/payment_type',
        DATA_SOURCE = NYC_TAXI_SRC,
        FILE_FORMAT = parquet_file_format
)
AS
SELECT 
* 
FROM 
BRONZE.payment_type_vw
END
--- 

-- select count(1) from BRONZE.payment_type_vw
-- MINUS
-- select * from SILVER.payment_type