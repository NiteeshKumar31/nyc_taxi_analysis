GO

CREATE OR ALTER PROCEDURE SILVER.usp_silver_trip_data_green
@year VARCHAR(4),
@month VARCHAR(2)
AS
BEGIN

    DECLARE @create_sql_stmt NVARCHAR(MAX),
            @drop_sql_stmt   NVARCHAR(MAX);

    SET @create_sql_stmt = 
        'CREATE EXTERNAL TABLE SILVER.trip_data_green_' + @year + '_' + @month + 
        ' WITH
            (
                DATA_SOURCE = NYC_TAXI_SRC,
                LOCATION = ''silver/trip_data_green/year=' + @year + '/month=' + @month + ''',
                FILE_FORMAT = parquet_file_format
            )
        AS
        SELECT [VendorID] AS vendor_id
                ,[lpep_pickup_datetime]
                ,[lpep_dropoff_datetime]
                ,[store_and_fwd_flag]
                ,[total_amount]
                ,[payment_type]
                ,[trip_type]
                ,[congestion_surcharge]
                ,[extra]
                ,[mta_tax]
                ,[tip_amount]
                ,[tolls_amount]
                ,[ehail_fee]
                ,[improvement_surcharge]
                ,[RatecodeID] AS rate_code_id
                ,[PULocationID] AS pu_location_id
                ,[DOLocationID] AS do_location_id
                ,[passenger_count]
                ,[trip_distance]
                ,[fare_amount]
        FROM BRONZE.trip_data_green_csv_vw
        WHERE year = ''' + @year + '''
          AND month = ''' + @month + '''';    

    print(@create_sql_stmt)

    EXEC sp_executesql @create_sql_stmt;

    SET @drop_sql_stmt = 
        'DROP EXTERNAL TABLE SILVER.trip_data_green_' + @year + '_' + @month;

    print(@drop_sql_stmt)
    EXEC sp_executesql @drop_sql_stmt;

END;




