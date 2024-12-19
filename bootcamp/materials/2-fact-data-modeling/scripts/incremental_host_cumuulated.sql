-- The incremental query to generate host_activity_datelist
   WITH yesterday AS (
    SELECT * FROM hosts_cumulated
    WHERE date = DATE('2023-01-01') -- harcoded for now
),
    today AS (
          SELECT 
       				host,
                   DATE(cast(event_time as TIMESTAMP)) AS date_active
                  FROM events e
            WHERE DATE(cast(event_time as TIMESTAMP)) = DATE('2023-01-02') -- hardcoded for now
             AND host IS NOT null
         GROUP BY host, DATE(cast(event_time as TIMESTAMP)) 
    )
INSERT INTO hosts_cumulated
select 
       COALESCE(t.host, y.host),
       case when y.device_activity is NULL
       then ARRAY[t.date_active]
       when t.date_active is null then y.device_activity
       else ARRAY[t.date_active] || y.device_activity
       end
       as devices_activity,
       COALESCE(t.date_active, y.date + Interval '1 day') as date
FROm yesterday y
    FULL OUTER JOIN
    today t ON  t.host= y.host ; -- joining key on th ehost column