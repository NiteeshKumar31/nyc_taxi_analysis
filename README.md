# NYC Taxi Analysis

## Project Overview
In this project we will be analysing NYC taxi data using Azure Synapse Analytics. Synapse Analytics is a Limitless analytics service that brings together data integration, enterprise data warehousing and big data analytics. Azure Synpase Analytics is one such service that solves all data engineering problems of using multiple services on their day to day activities, with the help of this single azure service we can perfom all 3 operations of Integration, transformation, Reporting.

## Tools

- Azure Synapse Analytics
- Azure Cosmos DB
- Azuer Storage service

### Environment Setup

- Create a Azure acccount using Azure portal.
- Create a resource group to have all services at one place.
- Create and configure Storagee Account(Blob Storage and ADLS GEN2).
- Create and configure Azure Synapse Analytics.
- Download and configure Power BI.

### Project Implementation

- Let's divide this project into typical data engineering project
  1. Data Ingestion
  2. Data Transformation
  3. Data Reporting

#### Solution Architecture

-- Insert synapse architecture snip here


##### Data Discovery

- Before diving into data ingestion, let's explore the data we have
  1. Data exploration capability on the raw data
  2. Schema applied to the raw data
  3. Data Discovery using T-SQL
  4. Data Discovery using pay-per-query model
  
- we will be exploring our data in datalake using serverless sql pool, serverless sql pool comes with synapse workspace hence we don't need to configure anything. As the name describes serverless sql pool is serverless and we will be only charged for the data we proceseed.
- serverless sql pool provides OPENROWSET function which will help in querying data in Azure storage. It does support multiple formats to read.
- Below is the synatax for reading `` PARQUET, DELTA, CSV`` files.
  
```
--OPENROWSET syntax for reading Parquet or Delta Lake files
OPENROWSET  
( { BULK 'unstructured_data_path' , [DATA_SOURCE = <data source name>, ]
    FORMAT= ['PARQUET' | 'DELTA'] }  
)  
[WITH ( {'column_name' 'column_type' }) ]
[AS] table_alias(column_alias,...n)

--OPENROWSET syntax for reading delimited text files
OPENROWSET  
( { BULK 'unstructured_data_path' , [DATA_SOURCE = <data source name>, ] 
    FORMAT = 'CSV'
    [ <bulk_options> ]
    [ , <reject_options> ] }  
)  
WITH ( {'column_name' 'column_type' [ 'column_ordinal' | 'json_path'] })  
[AS] table_alias(column_alias,...n)
 
<bulk_options> ::=  
[ , FIELDTERMINATOR = 'char' ]    
[ , ROWTERMINATOR = 'char' ] 
[ , ESCAPECHAR = 'char' ] 
[ , FIRSTROW = 'first_row' ]     
[ , FIELDQUOTE = 'quote_characters' ]
[ , DATA_COMPRESSION = 'data_compression_method' ]
[ , PARSER_VERSION = 'parser_version' ]
[ , HEADER_ROW = { TRUE | FALSE } ]
[ , DATAFILETYPE = { 'char' | 'widechar' } ]
[ , CODEPAGE = { 'ACP' | 'OEM' | 'RAW' | 'code_page' } ]
[ , ROWSET_OPTIONS = '{"READ_OPTIONS":["ALLOW_INCONSISTENT_READS"]}' ]

<reject_options> ::=  
{  
    | MAXERRORS = reject_value,  
    | ERRORFILE_DATA_SOURCE = <data source name>,
    | ERRORFILE_LOCATION = '/REJECT_Directory'
}
```

- The major parameters are bulk and format, for csv we need to mention rowterminator, parser version, firstrow, header_row etc if our file is different from traditional csv.
- Let's query taxi zone data using T-sql
- Below we've created a data source which will holds our file path infomation.
- Through WITH clause we're explictly defining data type length and it's order to save amount of data/memory being processed. but make sure you specify column names correctly as it WITH clause is CASE SENSITIVE.
- You can also check the data type lengths using the storage procedure ``sp_describe_first_result_set``
- Just like above, explore other CSV files CALENDAR, VENDOR. TRIP TYPE is also same but with TAB as Field terminator.
  
``` Refer to taxizone, calendar, vendor and trip tsv scripts in sql script folder ```

###### Reading JSON type files

- Coming to json file types, need to make sure we pass correct ROWTERMINATOR and FIELDTERMINATOR.
- With the help of JSON VALUE and OPEN json functions we can query json files.
-  CROSS APPLY works just like JOIN in SQL.
- If there is nested json structure, just apply OPENJSON on inner json structure.

``` Refer to rate code and payment scripts in sql script folder ```


###### Reading data inside folders

- There are cases where in we need to read all files inside folder, in those cases we need to make use of wildcard characters '*', '**'
- '*.csv will read only CSV files in folder.
- '**' will read all file types present in the folder.
- We can also make use of metadata functions like filepath and filename, these will give you total filepath name in datalake.
  
``` Refer to trip data green script in sql script folder ```

###### Reading PARQUET and DELTA Files

- The syntax is pretty much straight forward here, the only challenge is we can't query subfolder data of DELTA files.
- If we want to check subfolder data, we can use WHERE clause to filter specific subfolder.
- While reading delta files we have to specify the partition columns in with clause without fail.

``` Refer to trip data scripts in sql script folder ```
  
 #### Data Ingestion

 - Key objectives in data ingestion
   1. Ingested data to be stored as Parquet
   2. Ingested data to be stored as tables/ views
   3. Ability to query the ingested data using SQL
   4. Ingestion using pay-per-query model

 #### Data Transformation

  - Key objectives in data ingestion
    1. Join the key information required for reporting to create a new table.
    2. Join the key information required for Analysis to create a new table.
    3. Must be able to analyze the transformed data via T-SQL
    4. Transformed data must be stored in columnar format (i.e., Parquet)

 #### Data Reporting

  - Key objectives in data ingestion
    1. Taxi Demand
    2. Credit Card Campaign
    3. Operational Reporting
   
