--- Identify number of trips made by each borough
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK '/trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]

--- Check for discrepencies on borough column like whether nulls exists
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'taxi_zone.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) 
    WITH(
        LocationID SMALLINT,
        Borough VARCHAR(20),
        Zone VARCHAR(50),
        service_zone VARCHAR(20)
    )AS [result]
WHERE BOROUGH IS NULL  -- no nulls

--- join trip data and taxi zone data using location id 

SELECT taxi_zone_data.borough, count(1) as count
FROM
    OPENROWSET(
        BULK '/trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data

    JOIN

    OPENROWSET(
        BULK 'taxi_zone.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) 
    WITH(
        LocationID SMALLINT,
        Borough VARCHAR(20),
        Zone VARCHAR(50),
        service_zone VARCHAR(20)
    )AS taxi_zone_data
    ON trip_data.PULocationID = taxi_zone_data.locationID
    GROUP BY borough
    order by count desc

