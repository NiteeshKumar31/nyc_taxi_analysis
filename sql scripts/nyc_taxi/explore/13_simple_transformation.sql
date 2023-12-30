SELECT
    DATEDIFF(minute,lpep_pickup_datetime, lpep_dropoff_datetime)/60 as from_hour,
    (DATEDIFF(minute,lpep_pickup_datetime, lpep_dropoff_datetime)/60)+1 as to_hour,
    count(1) as no_of_trips

FROM
    OPENROWSET(
        BULK '/trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]
GROUP BY DATEDIFF(minute,lpep_pickup_datetime, lpep_dropoff_datetime)/60,
     (DATEDIFF(minute,lpep_pickup_datetime, lpep_dropoff_datetime)/60)+1
ORDER BY from_hour, to_hour