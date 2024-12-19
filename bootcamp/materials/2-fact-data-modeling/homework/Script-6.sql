WITH yesterday AS (
    SELECT * FROM user_devices_cumulated
--    WHERE date = DATE('2023-01-01')
),
    today AS (
          SELECT 
          		user_id as user_id,
          		jsonb_build_object(
          		user_id, DATE(cast(case when browser_type is null then null else  event_time end as TIMESTAMP))) AS date_active
                  FROM devices d join events e
                  on d.device_id = e.device_id
            WHERE DATE(cast(event_time as TIMESTAMP)) = DATE('2023-01-02')
            AND user_id IS NOT NULL
         GROUP BY user_id,  DATE(cast(case when browser_type is null then null else  event_time end as TIMESTAMP))
    )
INSERT INTO users_cumulated
SELECT
       COALESCE(t.user_id, y.user_id),
       case when y.dates_active is NULL
       then ARRAY[t.date_active]
       when t.date_active is null then y.dates_active
       else ARRAY[t.date_active]|| y.dates_active
       end
       as dates_active
--       COALESCE(t.date_active, y.date + Interval '1 day') as date
FROm yesterday y
    FULL OUTER JOIN
    today t ON t.user_id = y.user_id;
    
   
   select * from devices d 
   
   select * from events e 
   
   
   
   SELECT events->'holiday' AS holiday_dates FROM example_table;

   
   select * from user_devices_cumulated