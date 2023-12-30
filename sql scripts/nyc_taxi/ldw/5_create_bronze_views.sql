GO

DROP VIEW IF EXISTS BRONZE.rate_code_vw

GO

CREATE VIEW BRONZE.rate_code_vw
AS
SELECT rate_code_id, rate_code
FROM
    OPENROWSET(
        BULK 'raw/rate_code.json',
        DATA_SOURCE = 'NYC_TAXI_SRC',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b', -- Field terminator usally comma(',') for csv, if we use the same it will break the key value pairs of json.. hence we will use verticaL tab ('0x0b' is vertical tab)
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0X0b' -- new line character. 0x0b(vertical tab)
    )
    WITH
    (
        jsonDoc NVARCHAR(MAX)
    )
    AS rate_code
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        rate_code_id SMALLINT,
        rate_code VARCHAR(20)
    )
GO

select * from BRONZE.rate_code_vw

GO

------ Create view for payment json file

GO

DROP VIEW IF EXISTS BRONZE.payment_type_vw

GO

CREATE VIEW BRONZE.payment_type_vw
AS
SELECT
payment_type,
description
FROM
    OPENROWSET(
        BULK 'raw/payment_type.json',
        DATA_SOURCE = 'NYC_TAXI_SRC',
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
    WITH(
        payment_type SMALLINT,
        description VARCHAR(20) '$.payment_type_desc' -- if you want to rename column
    )
GO

select * from BRONZE.payment_type_vw

GO

----- Create  view for trip green
GO

DROP VIEW IF EXISTS BRONZE.trip_data_green_csv_vw

GO

CREATE VIEW BRONZE.trip_data_green_csv_vw
AS
SELECT
    result.filepath(1) as year,
    result.filepath(2) as month,
    result.*
FROM
    OPENROWSET(
        BULK 'raw/trip_data_green_csv/year=*/month=*/*.csv', -- fetches all the folders starts with 'year=' and then subfolders 'month='
        DATA_SOURCE = 'NYC_TAXI_SRC',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
)
WITH
(
    VendorID INT,
    lpep_pickup_datetime datetime2(7),
    lpep_dropoff_datetime datetime2(7),
    store_and_fwd_flag CHAR(1),
    RatecodeID INT,	
    PULocationID INT,	
    DOLocationID INT,	
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

)
AS [result]
GO

select COUNT(1) from BRONZE.trip_data_green_csv_vw

GO
