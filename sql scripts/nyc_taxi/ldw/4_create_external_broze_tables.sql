-- Create a new external table

IF OBJECT_ID('BRONZE.taxi_zone') IS NOT NULL
    DROP EXTERNAL TABLE BRONZE.taxi_zone

CREATE EXTERNAL TABLE BRONZE.taxi_zone
    (   location_id SMALLINT,
        Borough VARCHAR(20),
        Zone VARCHAR(50),
        service_zone VARCHAR(20)
    )
    WITH 
    (
        LOCATION = '/raw/taxi_zone.csv',
        DATA_SOURCE = NYC_TAXI_SRC,
        FILE_FORMAT = csv_file_format_pv1
        -- REJECT_VALUE = 10,    -- rejection value needs to be given only for parse version is 2.0, for version 1.0 we need to define column and data types explicitly
        -- REJECTED_ROW_LOCATION = 'rejections/taxi_zone'
    );

---
SELECT * FROM BRONZE.taxi_zone

-- Create External table for vendor

IF OBJECT_ID('BRONZE.vendor_info') IS NOT NULL
    DROP EXTERNAL TABLE BRONZE.vendor_info

CREATE EXTERNAL TABLE BRONZE.vendor_info
    (   vendor_id SMALLINT,
        vendor_name VARCHAR(100)
    )
    WITH 
    (
        LOCATION = '/raw/vendor.csv',
        DATA_SOURCE = NYC_TAXI_SRC,
        FILE_FORMAT = csv_file_format,
        REJECT_VALUE = 10,
        REJECTED_ROW_LOCATION = 'rejections/vendor'
    );

----
SELECT * FROM BRONZE.vendor_info
----

-- Create External table for calender

IF OBJECT_ID('BRONZE.calendar_info') IS NOT NULL
    DROP EXTERNAL TABLE BRONZE.calendar_info

CREATE EXTERNAL TABLE BRONZE.calendar_info
    (   
        date_key INT,
        date DATE,
        year SMALLINT,
        month TINYINT,
        day TINYINT,
        day_name VARCHAR(10),
        day_of_year SMALLINT,
        week_of_month TINYINT,
        week_of_year TINYINT,
        month_name VARCHAR(10),
        year_month INT,
        year_week INT
    )
    WITH 
    (
        LOCATION = '/raw/calendar.csv',
        DATA_SOURCE = NYC_TAXI_SRC,
        FILE_FORMAT = csv_file_format,
        REJECT_VALUE = 10,
        REJECTED_ROW_LOCATION = 'rejections/calendar'
    );

---
select * from BRONZE.calendar_info
---

-- Create External table for trips tsv file

IF OBJECT_ID('BRONZE.trip_type') IS NOT NULL
    DROP EXTERNAL TABLE BRONZE.trip_type

CREATE EXTERNAL TABLE BRONZE.trip_type
    (   vendor_id SMALLINT,
        vendor_name VARCHAR(100)
    )
    WITH 
    (
        LOCATION = '/raw/trip_type.tsv',
        DATA_SOURCE = NYC_TAXI_SRC,
        FILE_FORMAT = tsv_file_format,
        REJECT_VALUE = 10,
        REJECTED_ROW_LOCATION = 'rejections/trip'
    );

----
SELECT * FROM BRONZE.trip_type
----

---- Create external table for trip data green csv file


IF OBJECT_ID('BRONZE.trip_data_green_csv') IS NOT NULL
    DROP EXTERNAL TABLE BRONZE.trip_data_green_csv;

CREATE EXTERNAL TABLE BRONZE.trip_data_green_csv
    (  
    VendorID INT,
    lpep_pickup_datetime datetime2(7),
    lpep_dropoff_datetime datetime2(7),
    store_and_fwd_flag CHAR(1),
    RatecodeID INT,	
    PULocationID INT,	
    DOLocationID INT,	
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,	
    tip_amount FLOAT,
    tolls_amount FLOAT,	
    ehail_fee INT,	
    improvement_surcharge FLOAT,
    total_amount FLOAT,	
    payment_type INT,
    trip_type INT,	
    congestion_surcharge FLOAT
    )
    WITH 
    (
        LOCATION = '/raw/trip_data_green_csv/**',
        DATA_SOURCE = NYC_TAXI_SRC,
        FILE_FORMAT = csv_file_format
    );

----
SELECT COUNT(1) FROM BRONZE.trip_data_green_csv
----

---- Create external table for trip data green delta file

IF OBJECT_ID('BRONZE.trip_data_green_delta') IS NOT NULL
    DROP EXTERNAL TABLE BRONZE.trip_data_green_delta;

CREATE EXTERNAL TABLE BRONZE.trip_data_green_parquet
    (  
    VendorID INT,
    lpep_pickup_datetime datetime2(7),
    lpep_dropoff_datetime datetime2(7),
    store_and_fwd_flag CHAR(1),
    RatecodeID INT,	
    PULocationID INT,	
    DOLocationID INT,	
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,	
    tip_amount FLOAT,
    tolls_amount FLOAT,	
    ehail_fee INT,	
    improvement_surcharge FLOAT,
    total_amount FLOAT,	
    payment_type INT,
    trip_type INT,	
    congestion_surcharge FLOAT
    )
    WITH 
    (
        LOCATION = '/raw/trip_data_green_parquet/**',
        DATA_SOURCE = NYC_TAXI_SRC,
        FILE_FORMAT = parquet_file_format
    );

----
SELECT COUNT(1) FROM BRONZE.trip_data_green_parquet
----

---- Create external table for trip data green delta file

IF OBJECT_ID('BRONZE.trip_data_green_delta') IS NOT NULL
    DROP EXTERNAL TABLE BRONZE.trip_data_green_delta;

CREATE EXTERNAL TABLE BRONZE.trip_data_green_delta
    (  
    VendorID INT,
    lpep_pickup_datetime datetime2(7),
    lpep_dropoff_datetime datetime2(7),
    store_and_fwd_flag CHAR(1),
    RatecodeID INT,	
    PULocationID INT,	
    DOLocationID INT,	
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,	
    tip_amount FLOAT,
    tolls_amount FLOAT,	
    ehail_fee INT,	
    improvement_surcharge FLOAT,
    total_amount FLOAT,	
    payment_type INT,
    trip_type INT,	
    congestion_surcharge FLOAT
    )
    WITH 
    (
        LOCATION = '/raw/trip_data_green_delta/',
        DATA_SOURCE = NYC_TAXI_SRC,
        FILE_FORMAT = delta_file_format
    );

----
SELECT COUNT(1) FROM BRONZE.trip_data_green_delta
----
