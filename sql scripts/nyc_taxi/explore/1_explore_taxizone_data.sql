-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://nyctaxisynapsedl.dfs.core.windows.net/nyc-taxi-data/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) AS [result]

--- Validate datatypes of columns

EXEC sp_describe_first_result_set N'
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''https://nyctaxisynapsedl.dfs.core.windows.net/nyc-taxi-data/raw/taxi_zone.csv'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        HEADER_ROW = TRUE
    ) AS [result]'

--- Change datatype size explictly using WITH clause

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://nyctaxisynapsedl.dfs.core.windows.net/nyc-taxi-data/raw/taxi_zone.csv',
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


------

EXEC sp_describe_first_result_set N'
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''https://nyctaxisynapsedl.dfs.core.windows.net/nyc-taxi-data/raw/taxi_zone.csv'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        HEADER_ROW = TRUE
    ) 
    WITH(
        LocationID SMALLINT,
        Borough VARCHAR(20),
        Zone VARCHAR(50),
        service_zone VARCHAR(20)
    )AS [result]'

---- Creating new database for project

CREATE database nyc_taxi_discovery;

USE nyc_taxi_discovery;

--- Changing data type collation for VARCHAR types

ALTER DATABASE nyc_taxi_discovery COLLATE Latin1_General_100_CI_AI_SC_UTF8;

select name,collation_name from sys.databases;

------
DROP EXTERNAL DATA SOURCE nyc_taxi_data
----

CREATE EXTERNAL DATA SOURCE nyc_taxi_data_raw
WITH
(
    LOCATION = 'https://nyctaxisynapsedl.dfs.core.windows.net/nyc-taxi-data/raw'
)

-----


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
