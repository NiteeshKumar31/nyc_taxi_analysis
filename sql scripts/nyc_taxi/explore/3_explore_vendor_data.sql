SELECT
*
FROM
    OPENROWSET(
        BULK 'vendor.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    )
    AS vendor

----
SELECT
*
FROM
    OPENROWSET(
        BULK 'vendor_escaped.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        ESCAPECHAR = '\\',
        HEADER_ROW = TRUE
    )
    AS vendor
----
EXEC sp_describe_first_result_set N'
SELECT
TOP 100 *
FROM
    OPENROWSET(
        BULK ''vendor.csv'',
        DATA_SOURCE = ''nyc_taxi_data_raw'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        HEADER_ROW = TRUE,
        FIELDQUOTE = ''"''
    )
    AS [vendor]'
