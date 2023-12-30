SELECT 
   MIN(TOTAL_AMOUNT) AS MIN_TOTAL_AMOUNT,
   MAX(TOTAL_AMOUNT) AS MAX_TOTAL_AMOUNT,
   AVG(TOTAL_AMOUNT) AS AVG_TOTAL_AMOUNT,
   COUNT(1) AS TOTAL_RECORDS,
   COUNT(TOTAL_AMOUNT) AS TOTAL_AMOUNT_RECORDS,
   COUNT(DISTINCT TOTAL_AMOUNT) AS DISTINCT_TOTAL_AMOUNT
FROM
    OPENROWSET(
        BULK '/trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]

--- Find negative total_amount data

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK '/trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE TOTAL_AMOUNT < 0 -- Holds the negative total amount data

--- Find payemnt types of data and it's amount
SELECT
    payment_type,
    count(1) as payment_type_count,
    sum(total_amount) as total_amout_for_payment_type
FROM
    OPENROWSET(
        BULK '/trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]
GROUP BY payment_type