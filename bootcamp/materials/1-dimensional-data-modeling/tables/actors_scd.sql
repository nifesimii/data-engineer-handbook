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