-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=2020/month=01/green_tripdata_2020-01.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]

---- select data from a folder

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=2020/month=01/*', -- '*' is a wild character, it will search for all files in the folder. '*.csv' fetches all csv files in folder
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
) AS [result]

----select files from different folder

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ('trip_data_green_csv/year=2020/month=01/*.csv','trip_data_green_csv/year=2020/month=02/*.csv'),
        FORMAT = 'CSV',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
) AS [result]

----select data from subfolders

SELECT
     TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=2020/**', -- '**' fetches all the files in 2020 folder and it's subfolders
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE

---- select data from all folders

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*', -- fetches all the folders starts with 'year=' and then subfolders 'month='
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
) AS [result]

---- Fetch corresponding file name of each record using "filename" fucnction

SELECT
    result.filename() as file_name,
    count(1) as count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*.csv', -- fetches all the folders starts with 'year=' and then subfolders 'month='
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
) AS [result]
-- where result.filename() IN ('green_tripdata_2020-01.csv', 'green_tripdata_2020-02.csv') -- filter only 2 files for group by 
GROUP BY result.filename()
ORDER BY result.filename()

--- Filepath function

SELECT
    result.filename() as file_name,
    result.filepath() as file_path,
    count(1) as count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*.csv', -- fetches all the folders starts with 'year=' and then subfolders 'month='
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
) AS [result]
-- where result.filename() IN ('green_tripdata_2020-01.csv', 'green_tripdata_2020-02.csv') -- filter only 2 files for group by 
GROUP BY result.filename(), result.filepath()
ORDER BY result.filename(), result.filepath()

--- further filtering of filepath

SELECT
    --result.filename() as file_name,
    --result.filepath() as file_path,
    result.filepath(1) as year, -- fetches first object in filepath(here it is year)
    result.filepath(2) as month, -- fetches first object in filepath(here it is month)
    count(1) as count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*.csv', -- fetches all the folders starts with 'year=' and then subfolders 'month='
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
) AS [result]
where result.filepath(1) = 2020 and result.filepath(2) IN ('06', '07','08')
GROUP BY result.filepath(1),result.filepath(2)
ORDER BY result.filepath(1),result.filepath(2)


