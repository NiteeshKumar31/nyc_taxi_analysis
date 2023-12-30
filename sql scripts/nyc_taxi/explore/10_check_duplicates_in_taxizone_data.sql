-- Check for duplicates
SELECT
    LocationID,
    count(1) as count
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
GROUP BY LocationID
HAVING count(*) > 1
ORDER BY LocationID
