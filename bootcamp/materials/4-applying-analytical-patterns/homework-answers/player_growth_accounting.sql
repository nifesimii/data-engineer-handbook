

create type state_class as enum ('New', 'Retired', 'Continued Playing', 'Returned from Retirement', 'Stayed Retired');

create table players_growth_accounting (
player_name text,
first_active_season INTEGER,
last_active_season INTEGER,
season_state TEXT,
seasons_active INTEGER[],
current_season INTEGER,
PRIMARY KEY(player_name, current_season)
)

--drop table players_growth_accounting


--insert into players_growth_accounting
WITH last_season AS (
    SELECT * FROM players_growth_accounting
    WHERE current_season  = 2024
),
     this_season AS (
    	 select DISTINCT p.player_name,
    	 ps.season as current_season 
    	 from players p join  players_scd pscd
   		 on p.player_name = pscd.player_name
    	 join player_seasons ps 
    	 on p.player_name  = ps.player_name
         WHERE season = 2025
         AND p.player_name IS NOT NULL
     )
         SELECT COALESCE(t.player_name, l.player_name) as player_name,
                COALESCE(l.first_active_season, t.current_season)       AS first_active_season,
                COALESCE(t.current_season, l.last_active_season)        AS last_active_season,
                CASE
                    WHEN l.player_name IS NULL THEN 'New'
                    WHEN l.last_active_season = t.current_season - 1 THEN 'Continued Playing'
                    WHEN l.last_active_season < t.current_season - 1  THEN 'Returned from Retirement'
                    WHEN t.current_season IS NULL AND l.last_active_season = l.current_season THEN 'Retired'
                    ELSE 'Stayed Retired'
                    END                                           as season_state,
                COALESCE(l.seasons_active,
                         ARRAY[]::integer[]) 
                         || CASE
                           WHEN
                               t.player_name IS NOT NULL
                               THEN ARRAY [t.current_season]
                           ELSE ARRAY[]::integer[]
                    END                                           AS season_list,
                COALESCE(t.current_season, l.current_season + 1) as year
         FROM this_season t
                  FULL OUTER JOIN last_season l
                                  ON t.player_name = l.player_name




   SELECT * FROM players_growth_accounting 
    
    
    
    
    select DISTINCT p.player_name,
    ps.season 
    from players p join  players_scd pscd
    on p.player_name = pscd.player_name
    join player_seasons ps 
    on p.player_name  = ps.player_name
    where p.player_name = 'Aaron Brooks'