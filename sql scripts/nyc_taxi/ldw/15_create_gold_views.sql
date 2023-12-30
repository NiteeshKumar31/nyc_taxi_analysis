USE nyc_taxi_ldw
GO 

-- Create view for trip_data_green
DROP VIEW IF EXISTS GOLD.trip_data_green_vw
GO

CREATE VIEW GOLD.trip_data_green_vw
AS
SELECT
    result.filepath(1) AS year,
    result.filepath(2) AS month,
    result.*
FROM
    OPENROWSET(
        BULK 'gold/trip_data_green/year=*/month=*/*.parquet',
        DATA_SOURCE = 'NYC_TAXI_SRC',
        FORMAT = 'PARQUET'
    )
    WITH (
        Borough         VARCHAR(15),
        trip_date       DATE,
        trip_day        VARCHAR(10),
        trip_day_weekend_ind CHAR(1),
        card_trip_count INT,
        cash_trip_count INT,
        street_hail_trip_count INT,
        dispatch_trip_count    INT,
        trip_distance          FLOAT,
        trip_duration          INT,
        fare_amount            FLOAT
  ) AS [result]
GO

----

select count(1) from GOLD.trip_data_green_vw

