SELECT *
FROM
    OPENROWSET(
        BULK 'rate_code.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
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
---------------------------------------------
-- process multi line json
---------------------------------------------

SELECT *
FROM
    OPENROWSET(
        BULK 'rate_code_multi_line.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0X0b' -- 0x0b(vertical tab)
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