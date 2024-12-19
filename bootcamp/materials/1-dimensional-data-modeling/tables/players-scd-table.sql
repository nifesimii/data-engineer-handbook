create type scd_type as (
scoring_class scoring_class,
is_active boolean,
start_season INTEGER,
end_season INTEGER)

create table players_scd (
player_name text,
scoring_class scoring_class,
is_active BOOLEAN,
start_season INTEGER,
end_season INTEGER,
current_season INTEGER,
PRIMARY KEY(player_name, start_season)
)
