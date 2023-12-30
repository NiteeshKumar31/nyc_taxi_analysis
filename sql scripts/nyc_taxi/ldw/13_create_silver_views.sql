GO 

-- Create view for trip_data_green
DROP VIEW IF EXISTS SILVER.trip_data_green_csv_vw
GO

CREATE VIEW SILVER.trip_data_green_csv_vw
AS
SELECT
    result.filepath(1) AS year,
    result.filepath(2) AS month,
    result.*
FROM
    OPENROWSET(
        BULK 'silver/trip_data_green/year=*/month=*/*.parquet',
        DATA_SOURCE = 'NYC_TAXI_SRC',
        FORMAT = 'PARQUET'
    )
    WITH (
        vendor_id INT,
        lpep_pickup_datetime datetime2(7),
        lpep_dropoff_datetime datetime2(7),
        store_and_fwd_flag CHAR(1),
        rate_code_id INT,
        pu_location_id INT,
        do_location_id INT,
        passenger_count INT,
        trip_distance FLOAT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        ehail_fee INT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        payment_type INT,
        trip_type INT,
        congestion_surcharge FLOAT
  ) AS [result]
GO

SELECT TOP(100) *
  FROM SILVER.trip_data_green_csv_vw
GO

SELECT count(1) from SILVER.trip_data_green_csv_vw
