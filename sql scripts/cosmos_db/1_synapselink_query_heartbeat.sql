IF (NOT EXISTS(SELECT * FROM sys.credentials WHERE name = 'nyc-taxi-cosmos-db'))
    CREATE CREDENTIAL [nyc-taxi-cosmos-db]
    WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = 'ufoc1dpnkO6T1yNLFkYSVl1mpIjWjMx6fdI7sIDmWIImbfTVWyX3tpnynPZhA00O4Dq1fivQL5IRACDbOBEUJA=='
GO

SELECT TOP 100 *
FROM OPENROWSET(​PROVIDER = 'CosmosDB',
                CONNECTION = 'Account=nyc-taxi-cosmos-db;Database=nyctaxi',
                OBJECT = 'Heartbeat',
                SERVER_CREDENTIAL = 'nyc-taxi-cosmos-db'
) AS [Heartbeat]
