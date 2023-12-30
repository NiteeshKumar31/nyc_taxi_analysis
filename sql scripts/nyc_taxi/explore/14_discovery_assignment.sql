-- Identify the percentage of cash and credit card trips by borough
WITH v_payment_type AS
(
SELECT
payment_type,
---payment_type_desc,
description as payment_desc
FROM
    OPENROWSET(
        BULK 'payment_type.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b'
    )
    WITH
    (
        jsonDoc NVARCHAR(MAX)
    )
    AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
    WITH
    (
        payment_type SMALLINT,
        --payment_type_desc VARCHAR(20),
        description VARCHAR(20) '$.payment_type_desc' -- if you want to rename column
    )
),

v_taxi_zone AS
(   SELECT * FROM 
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
),

v_trip_data as 
(
SELECT *
FROM
    OPENROWSET(
        BULK '/trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data
)

SELECT 
v_taxi_zone.borough, 
count(1) as total_trips,
SUM(CASE WHEN v_payment_type.payment_desc = 'Cash' THEN 1 ELSE 0 END) AS cash_trips,
SUM(CASE WHEN v_payment_type.payment_desc = 'Credit card' THEN 1 ELSE 0 END) AS card_trips,
CAST(SUM(CASE WHEN v_payment_type.payment_desc = 'Cash' THEN 1 ELSE 0 END)/ CAST(count(1) as DECIMAL)) *100  AS DECIMAL(5,2)) AS cash_trips_percentage,
CAST(SUM(CASE WHEN v_payment_type.payment_desc = 'Credit card' THEN 1 ELSE 0 END)/ CAST(count(1) as DECIMAL))*100  AS DECIMAL(5,2)) AS card_trips_percentage
FROM v_trip_data
LEFT JOIN v_payment_type ON (v_trip_data.payment_type = v_payment_type.payment_type)
LEFT JOIN v_taxi_zone ON (v_trip_data.PUlocationId = v_taxi_zone.location_id)
WHERE v_payment_type.payment_desc IN ('Cash', 'Credi card')
GROUP BY v_taxi_zone.borough
ORDER BY v_taxi_zone.borough


