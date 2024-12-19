--- incremental player_scd table query 
insert into players_scd 
with  with_previous as (
select player_name, 
current_season,
scoring_class, 
is_active,
LAG(scoring_class, 1 ) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
LAG(is_active, 1 ) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
from players
where current_season <= 2021
),
with_indicators AS (
SELECT *, 
case 
when scoring_class <> previous_scoring_class then 1 
when is_active <> previous_is_active then 1 
ELSE 0
END AS change_indicator
from with_previous),
with_streaks AS (
SELECT *,
SUM(change_indicator)
	OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
	FROM with_indicators
)
SELECT player_name,
	scoring_class,
	is_active,
--	streak_identifier,
	MIN(current_season) AS start_season,
	MAX(current_season) AS end_season,
	2021 as current_season
  FROM with_streaks
 GROUP BY player_name,streak_identifier, is_active,scoring_class
 order by player_name


 select * from players_scd 
 
 
 --cumulative players_scd table
 with last_season_scd as (
 select * from players_scd ps
 where current_season = 2021
 and end_season = 2021),
 historical_scd as (
 select player_name,scoring_class,is_active,start_season,end_season from players_scd ps
 where current_season = 2021
 and end_season < 2021),
 this_season_data as (
 select * from players p
 where current_season = 2022
 ),
 unchanged_records AS(
 select ts.player_name,
 ts.scoring_class, 
 ts.is_active,
 ls.start_season,
 ts.current_season as end_season
 from this_season_data ts
 join last_season_scd ls
 on ls.player_name = ts.player_name
 where ts.scoring_class = ls.scoring_class
 and ts.is_active = ls.is_active
 ),
 changed_records AS(
  select ts.player_name,
-- ts.scoring_class, 
-- ts.is_active,
-- ls.start_season,
-- ts.current_season as end_season,
 unnest(ARRAY[row(ls.scoring_class,
 ls.is_active,
 ls.start_season,
 ls.end_season)::scd_type,
 row(
 ts.scoring_class,
 ts.is_active,
 ts.current_season,
 ts.current_season
 )::scd_type]) as records
 from this_season_data ts
 left join last_season_scd ls
 on ls.player_name = ts.player_name
 where (ts.scoring_class <> ls.scoring_class
 OR ts.is_active <> ls.is_active)
 ), unnested_changed_records AS (
 select player_name,
 (records::scd_type).scoring_class,
 (records::scd_type).is_active,
 (records::scd_type).start_season,
  (records::scd_type).end_season
 from changed_records),
 new_records as (
 select 
 ts.player_name,
 ts.scoring_class,
 ts.is_active,
 ts.current_season as start_season,
 ts.current_season as end_season
 from this_season_data ts
 left join last_season_scd ls
 on ts.player_name = ls.player_name
 where ls.player_name is NULL
 )
 select * from historical_scd
 union all
 select * from unchanged_records
 union all
 select * from unnested_changed_records
 union all
 select * from new_records

 
 
-- select ts.player_name,
-- ts.scoring_class , ts.is_active,
-- ls.scoring_class, ls.is_active
-- from this_season_data ls
-- left join last_season_scd ts
-- on ls.player_name = ts.player_name
 
 
 SELECT max(year) FROM actor_films

