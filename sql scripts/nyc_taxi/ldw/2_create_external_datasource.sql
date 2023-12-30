IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE NAME ='NYC_TAXI_SRC')
CREATE EXTERNAL DATA SOURCE NYC_TAXI_SRC
WITH
(
LOCATION  = 'https://nyctaxisynapsedl.dfs.core.windows.net/nyc-taxi-data'
)