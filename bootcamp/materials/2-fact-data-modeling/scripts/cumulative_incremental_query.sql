--Creating a  cumulative query to generate device_activity_datelist from events
   WITH yesterday AS (
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-01-30') -- harcoded for now
),
    today AS (
          SELECT 
       				user_id,  
       				browser_type,
                   DATE(cast(event_time as TIMESTAMP)) AS date_active
                  FROM devices d join events e
                  on d.device_id = e.device_id
            WHERE DATE(cast(event_time as TIMESTAMP)) = DATE('2023-01-31') -- this is hardcoded 
            AND user_id IS NOT null
             AND d.browser_type IS NOT null
         GROUP BY user_id, browser_type, DATE(cast(event_time as TIMESTAMP)) 
    )
-- Inserting the cumulative query into the user_devices_cumulated table
INSERT INTO user_devices_cumulated
SELECT
       COALESCE(t.user_id, y.user_id),
       case when y.device_activity is NULL
       then ARRAY[t.date_active]
       when t.date_active is null then y.device_activity
       else ARRAY[t.date_active] || y.device_activity
       end
       as devices_activity,
       COALESCE(t.browser_type, y.browser_type),
       COALESCE(t.date_active, y.date + Interval '1 day') as date 
FROm yesterday y
    FULL OUTER JOIN
    today t ON t.user_id = y.user_id and t.browser_type = y.browser_type ; -- use browser_type and user_id to ensure there's no duplicate rows based on those values 