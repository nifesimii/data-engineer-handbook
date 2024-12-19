-- This is a query for converting the device_activity_datelist column into a datelist_int column
with users as (
select * from user_devices_cumulated uc 
where date = DATE('2023-01-31')
),
	series as (
		select * 
		from generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') -- generate a series for the month of january 
			as series_date
	),
	place_holder_ints as (
select case when 
			device_activity @> ARRAY [DATE(series_date)]
			then  cast(POW(2, 32 - (date - DATE(series_date))) as BIGINT)
					else 0
							end as placeholder_int_value,
 *
from users cross join series
--where user_id = '439578290726747300'
)
select
	user_id,
cast(cast(sum(placeholder_int_value) as BIGINT) as bit(32)) ,
BIT_COUNT(cast(cast(sum(placeholder_int_value) as BIGINT) as bit(32))) > 0 as dim_is_monthly_active, -- this is to say if a user has been active in a recent month
bit_count( CAST('111111100000000000000000000000' as bit(32)) &
cast(CAST(sum(placeholder_int_value) as BIGINT) as bit(32))) > 0 as dim_is_weekly_active, -- this is to say if a user has been active in the last week or so
bit_count( CAST('100000000000000000000000000000' as bit(32)) &
cast(CAST(sum(placeholder_int_value) as BIGINT) as bit(32))) > 0 as dim_is_daily_active -- this is to say if a user has been active in more than one day
from place_holder_ints
group by user_id