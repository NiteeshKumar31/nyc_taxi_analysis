SELECT TOP 100 *
FROM 
OPENROWSET
(
    BULK '/trip_data_green_delta/',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'Delta'
) AS trip_data

-- 
SELECT TOP 100 *
FROM 
OPENROWSET
(
    BULK '/trip_data_green_delta/',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'Delta'
) 
WITH 
(
    store_and_fwd_flag char(1),
    trip_type int,
    tip_amount float,
    year VARCHAR(10),
    month VARCHAR(2)
)AS trip_data