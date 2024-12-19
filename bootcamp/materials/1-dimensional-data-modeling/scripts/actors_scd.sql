create table actors_history_scd(
actor text,
actorid text,
quality_class quality_class,
is_active BOOLEAN,
start_date INTEGER,
end_date INTEGER,
current_year INTEGER,
PRIMARY KEY(actorid,start_date)
)


create type actor_scd as (
quality_class quality_class,
is_active boolean,
start_date INTEGER,
end_date INTEGER)





insert into actors_history_scd
with  with_previous as (
SELECT 
actor,
actorid ,
current_year ,
quality_class,
is_active,
LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) as previous_quality_class,
LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) as previous_is_active
FROM actors
where current_year <= 2021
),
with_indicators AS (
SELECT *, 
case 
when quality_class <> previous_quality_class then 1 
when is_active <> previous_is_active then 1 
ELSE 0
END AS change_indicator
from with_previous),
with_streaks AS (
SELECT *,
SUM(change_indicator)
	OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
	FROM with_indicators
)
SELECT actor,
		actorid,
	quality_class,
	is_active,
--	streak_identifier,
	MIN(current_year) AS start_year,
	MAX(current_year) AS end_year,
	2021 as current_year
  FROM with_streaks
 GROUP by actor,actorid,streak_identifier, is_active,quality_class
 order by actor,actorid



 select * from actors_history_scd
 

select  max(current_year) from  actors


with last_year_scd as (
 select * from actors_history_scd ahs
 where current_year = 2020
 and end_date = 2021),
 historical_scd as (
 select actor,actorid,quality_class,is_active,start_date,end_date FROM actors_history_scd ahs
 where current_year = 2020
 and end_date < 2020),
 this_year_data as (
 select * from actors a
 where current_year = 2021
 ),
 unchanged_records AS(
 select ts.actor,
 ts.actorid,
 ts.quality_class, 
 ts.is_active,
 ls.start_date,
 ts.current_year as end_year
 from this_year_data ts
 join last_year_scd ls
 on ls.actorid = ts.actorid
 where ts.quality_class = ls.quality_class
 and ts.is_active = ls.is_active
 ),
 changed_records AS(
  select ts.actor,
  ts.actorid,
 unnest(ARRAY[row(ls.quality_class,
 ls.is_active,
 ls.start_date,
 ls.end_date)::actor_scd,
 row(
 ts.quality_class,
 ts.is_active,
 ts.current_year,
 ts.current_year
 )::actor_scd]) as records
 from this_year_data ts
 left join last_year_scd ls
 on ls.actorid = ts.actorid
 where (ts.quality_class <> ls.quality_class
 OR ts.is_active <> ls.is_active)
 ), unnested_changed_records AS (
 select actor,
 actorid,
 (records::actor_scd).quality_class,
 (records::actor_scd).is_active,
 (records::actor_scd).start_date,
  (records::actor_scd).end_date
 from changed_records),
 new_records as (
 select 
 ts.actor,
 ts.actorid,
 ts.quality_class,
 ts.is_active,
 ts.current_year as start_date,
 ts.current_year as end_date
 from this_year_data ts
 left join last_year_scd ls
 on ts.actorid= ls.actorid
 where ls.actorid is NULL
 )
 select * from historical_scd
 union all
 select * from unchanged_records
 union all
 select * from unnested_changed_records
 union all
 select * from new_records