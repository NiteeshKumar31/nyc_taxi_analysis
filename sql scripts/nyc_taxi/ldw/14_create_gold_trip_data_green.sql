-- create gold trip data green table for payment data analysis 

SELECT  td.year,
        td.month,
        tz.Borough,
        CONVERT(DATE, td.lpep_pickup_datetime) as trip_date,
        cal.day_name as trip_day,
        CASE WHEN cal.day_name in ('Saturday','Sunday') THEN 'Y' ELSE 'N' END as weeknd_ind,
        SUM(CASE WHEN pt.description = 'Credit card' THEN 1 ELSE 0 END) as card_trip_amount,
        SUM(CASE WHEN pt.description = 'Cash' THEN 1 ELSE 0 END) as cash_trip_amount
    FROM SILVER.trip_data_green_csv_vw td 
    JOIN SILVER.taxi_zone tz ON (td.pu_location_id = tz.location_id)
    JOIN SILVER.calendar_info cal ON (cal.date =CONVERT(DATE, td.lpep_pickup_datetime))
    JOIN SILVER.payment_type pt on (pt.payment_type = td.payment_type)
WHERE td.year = '2020' AND td.month = '01'
GROUP BY 
td.year,
td.month,
tz.Borough,
CONVERT(DATE, td.lpep_pickup_datetime),
cal.day_name



-- SELECT * from SILVER.taxi_zone
-- SELECT TOP 10 * from SILVER.trip_data_green_csv_vw
-- SELECT * from SILVER.calendar_info 
-- SELECT * from SILVER.payment_type

