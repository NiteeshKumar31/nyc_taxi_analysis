-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK '/trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]

--- Check datatypes

EXEC sp_describe_first_result_set N'
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''/trip_data_green_parquet/year=2020/month=01/'',
        DATA_SOURCE = ''nyc_taxi_data_raw'',
        FORMAT = ''PARQUET''
    ) AS [result]'

--- Updating datatype lengths

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK '/trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    )
    WITH
    (
       store_and_fwd_flag CHAR(1) 
    )
    AS [result]

/* Assignment
1. query from folders using wild cards
2. use filename function
3. query from subfolders
4. use filepath function to select only from certain partitions
*/

---
SELECT TOP 100
        result.filepath() as file_path,
        result.*
FROM
    OPENROWSET(
        BULK '/trip_data_green_parquet/**',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    )
    AS [result]


---
SELECT
     result.filepath(1) as year,
     result.filepath(2) as month,
    count(1) as count
FROM
    OPENROWSET(
        BULK '/trip_data_green_parquet/year=*/month=*/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    )
    AS [result]
GROUP BY result.filepath(1), result.filepath(2)
ORDER BY result.filepath(1), result.filepath(2)
