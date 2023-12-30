-- Create an external file format for DELIMITED (CSV/TSV) files.

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE NAME ='csv_file_format')
CREATE EXTERNAL FILE FORMAT csv_file_format
WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS 
            ( 
            FIELD_TERMINATOR = ','
            , STRING_DELIMITER = '"'
            , FIRST_ROW = 2 -- Applies to: Azure Synapse Analytics and SQL Server 2022 and later versions
            , USE_TYPE_DEFAULT = FALSE -- If this is true then serverless pool defaultly adds value to the NULL fields in data
            , ENCODING = 'UTF8'
            , PARSER_VERSION = '2.0'
            )        
        );

-- create file format with parser version 1.0
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE NAME ='csv_file_format_pv1')
CREATE EXTERNAL FILE FORMAT csv_file_format_pv1
WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS 
            ( 
            FIELD_TERMINATOR = ','
            , STRING_DELIMITER = '"'
            , FIRST_ROW = 2 -- Applies to: Azure Synapse Analytics and SQL Server 2022 and later versions
            , USE_TYPE_DEFAULT = FALSE 
            , ENCODING = 'UTF8'
            , PARSER_VERSION = '1.0'
            )        
        );

--- Create external tsv file format 2.0
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE NAME ='tsv_file_format')
CREATE EXTERNAL FILE FORMAT tsv_file_format
WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS 
            ( 
            FIELD_TERMINATOR = '\t'
            , STRING_DELIMITER = '"'
            , FIRST_ROW = 2 -- Applies to: Azure Synapse Analytics and SQL Server 2022 and later versions
            , USE_TYPE_DEFAULT = FALSE 
            , ENCODING = 'UTF8'
            , PARSER_VERSION = '2.0'
            )        
        );

-- create tsv file format with parser version 1.0
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE NAME ='tsv_file_format_pv1')
CREATE EXTERNAL FILE FORMAT tsv_file_format_pv1
WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS 
            ( 
            FIELD_TERMINATOR = '\t'
            , STRING_DELIMITER = '"'
            , FIRST_ROW = 2 -- Applies to: Azure Synapse Analytics and SQL Server 2022 and later versions
            , USE_TYPE_DEFAULT = FALSE 
            , ENCODING = 'UTF8'
            , PARSER_VERSION = '1.0'
            )        
        );

--Create an external file format for PARQUET files.
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE NAME ='parquet_file_format')
CREATE EXTERNAL FILE FORMAT parquet_file_format
WITH (
         FORMAT_TYPE = PARQUET,
         DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );

--Create an external file format for DELTA files.
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE NAME ='delta_file_format')
CREATE EXTERNAL FILE FORMAT delta_file_format
WITH (
         FORMAT_TYPE = DELTA,
         DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );
