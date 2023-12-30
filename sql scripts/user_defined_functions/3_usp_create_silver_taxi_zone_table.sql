--- Create exteranal silver taxi_zone table
GO
CREATE OR ALTER PROCEDURE SILVER.usp_silver_taxi_zone
AS
BEGIN
        IF OBJECT_ID('SILVER.taxi_zone') IS NOT NULL
            DROP EXTERNAL TABLE SILVER.taxi_zone;

        CREATE EXTERNAL TABLE SILVER.taxi_zone
            WITH
            (
                    LOCATION = '/silver/taxi_zone',
                    DATA_SOURCE = NYC_TAXI_SRC,
                    FILE_FORMAT = parquet_file_format
            )
        AS
        SELECT * 
        FROM 
        BRONZE.taxi_zone
END