SELECT
CAST(JSON_VALUE(jsonDoc, '$.payment_type') AS SMALLINT) payment_type,
CAST(JSON_VALUE(jsonDoc, '$.payment_type_desc') AS VARCHAR(20))payment_desc
FROM
    OPENROWSET(
        BULK 'payment_type.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b', -- vertical tab
        FIELDQUOTE = '0x0b', -- vertical tab
        ROWTERMINATOR = '0X0a' -- new line(\n)
    )
    WITH
    (
        jsonDoc NVARCHAR(MAX)
    )
    AS payment_type

-------- Above json value function has some drawbacks, we need to cast datatype everytime for every column in dataset.

SELECT
payment_type,
description
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
    WITH(
        payment_type SMALLINT,
        description VARCHAR(20) '$.payment_type_desc' -- if you want to rename column
    )
-- CROSS APPLY works like SQL JOIN
-----------------------------------------------
--Use open json to explore json array
-----------------------------------------------
SELECT
payment_type, payment_type_desc_value
FROM
    OPENROWSET(
        BULK 'payment_type_array.json',
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
    WITH(
        payment_type SMALLINT,
        payment_type_desc NVARCHAR(MAX) AS JSON
    )
    CROSS APPLY OPENJSON(payment_type_desc) -- passing innger json structure payment_type_desc into CROSS APPLY
    WITH
    (
        sub_type SMALLINT, 
        payment_type_desc_value VARCHAR(20) '$.value'
    )
