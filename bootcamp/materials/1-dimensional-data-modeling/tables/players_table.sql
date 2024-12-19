select * from player_seasons ps 

SELECT date_part('year', (SELECT current_timestamp));

create type season_stats as ( 
season INTEGER,
gp INTEGER,
pts real,
reb real,
ast real)



CREATE TABLE players (
player_name TEXT ,
height TEXT,
college TEXT,
country TEXT,
draft_year TEXT,
draft_round TEXT,
draft_number TEXT,
season_stats season_stats[],
scoring_class scoring_class,
years_since_last_season INTEGER,
current_season INTEGER,
is_active BOOLEAN,
PRIMARY KEY(player_name,current_season)
);

create type scoring_class as enum ('star', 'good', 'average', 'bad');