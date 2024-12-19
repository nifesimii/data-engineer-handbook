WITH yesterday AS (
    SELECT * FROM user_devices_cumulated
--    WHERE date = DATE('2023-01-01')
),
    today AS (
          SELECT 
          		user_id as user_id,
          		DATE(cast(case when browser_type is null then null else  event_time end as TIMESTAMP)) AS date_active
                  FROM devices d join events e
                  on d.device_id = e.device_id
            WHERE DATE(cast(event_time as TIMESTAMP)) = DATE('2023-01-02')
            AND user_id IS NOT NULL
         GROUP BY user_id,  DATE(cast(case when browser_type is null then null else  event_time end as TIMESTAMP))
    )
--INSERT INTO users_cumulated
select
COALESCE(t.user_id, cast(y.user_id as bigint)),
              case when y.dates_active->y.user_id is null
              then jsonb_build_object(t.user_id,array_to_json(ARRAY[t.date_active]))
               when t.date_active is null then y.dates_active
                else jsonb_build_object(t.user_id ,array_to_json(ARRAY[t.date_active]))|| y.dates_active->y.user_id
                end as dates_active
              from  yesterday y
    FULL OUTER JOIN
    today t ON  cast(t.user_id as bigint) = cast(y.user_id as bigint);