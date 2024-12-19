-- This is to repopulate players table after day one during day two

INSERT INTO players
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
), p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.gp,
                            ps.pts,
                            ps.reb,
                            ps.ast
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
    w.season,
    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;
   
   
  select * from players
   
   
   

--During the lesson

insert into players
WITH yesterday AS (
	SELECT * FROM players
	WHERE current_season = 2000
),
	today AS (
		SELECT * FROM player_seasons
		WHERE season = 2001
		)
SELECT
	COALESCE(t.player_name,y.player_name) AS player_name,
	COALESCE(t.height,y.height) AS height,
	COALESCE(t.college,y.college) AS college,
	COALESCE(t.country,y.country) AS country,	
	COALESCE(t.draft_year,y.draft_year) AS draft_year,	
	COALESCE(t.draft_round,y.draft_round) AS draft_round,	
	COALESCE(t.draft_number,y.draft_number) AS draft_number,
	case when y.season_stats is null 
	then ARRAY[row(
	t.season,
	t.gp,
	t.pts,
	t.reb,
	t.ast
	)::season_stats]
	when t.season is not null then y.season_stats || ARRAY[row(t.season,t.gp,t.pts,t.reb,t.ast)::season_stats]
	else y.season_stats
	end as season_stats,
	case when t.season is not null then
	case when t.pts > 20 then 'star'
		when t.pts > 15 then 'good'
		when t.pts > 10 then 'average'
		else 'bad'
		end::scoring_class
	else y.scoring_class
	end as scoring_class,
	case when t.season is not null then 0
	else y.years_since_last_season + 1
	end as years_since_last_season,
   case when y.current_season = t.season then true
   else false end as  is_active,
	coalesce(t.season,y.current_season + 1) as current_season
from today t FULL OUTER JOIN yesterday y
ON t.player_name = y.player_name
--
--
--WITH unnested AS (
--select player_name,
--UNNEST(season_stats)::season_stats AS season_stats 
--from players where current_season = 2001 and player_name = 'Michael Jordan')
--SELECT player_name,
--(season_stats::season_stats).*
--FROM unnested
--
--
--
--
--
--select 
--player_name,
--(season_stats[cardinality(season_stats)]::season_stats).pts/
--case when (season_stats[1]::season_stats).pts = 0 then 1 else (season_stats[1]::season_stats).pts end 
--from players where current_season = 2001
--and scoring_class = 'star'
--order by 2 desc
--
--
--
--select distinct season from player_seasons ps 
