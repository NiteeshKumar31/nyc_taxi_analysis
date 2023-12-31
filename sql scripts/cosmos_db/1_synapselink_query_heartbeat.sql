IF (NOT EXISTS(SELECT * FROM sys.credentials WHERE name = 'nyc-taxi-cosmos-db'))
    CREATE CREDENTIAL [nyc-taxi-cosmos-db]
    WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = 'cosmosdBaccountkey'
GO

SELECT TOP 100 *
FROM OPENROWSET(​PROVIDER = 'CosmosDB',
                CONNECTION = 'Account=nyc-taxi-cosmos-db;Database=nyctaxi',
                OBJECT = 'Heartbeat',
                SERVER_CREDENTIAL = 'nyc-taxi-cosmos-db'
) AS [Heartbeat]
