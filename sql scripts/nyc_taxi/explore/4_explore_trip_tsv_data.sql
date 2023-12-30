SELECT
*
FROM
    OPENROWSET(
        BULK 'trip_type.tsv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIELDTERMINATOR = '\t',
        HEADER_ROW = TRUE
    )
    AS vendor