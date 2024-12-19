
---inserting the game vertex 
insert into vertices
select game_id as identifier,
'game'::vertex_type as type,
json_build_object('pts_home', pts_home,
				  'pts_away', pts_away,
				  'winning_team', case when home_team_wins = 1 then home_team_id else visitor_team_id end ) as properties
from games;


---inserting the player vertex 
insert into vertices
with players_agg as (
select
	player_id as identifier,
	MAX(player_name) as player_name,
	count(1) as number_of_games,
	sum(pts) as total_points,
	ARRAY_AGG(distinct team_id) as teams
	from game_details 
	group by player_id
)
select identifier, 'player'::vertex_type,
		json_build_object(
			'player_name', player_name,
			'number_of_games', number_of_games,
			 'total_points', total_points,
			 'teams', teams)
from players_agg

--inserting the teams vertex
insert into vertices
with teams_deduped as (
	select *, row_number() over(partition by team_id) as row_num
	from teams)
select team_id as identifier,
	   'team'::vertex_type as type,
	    json_build_object(
	    'abbreviation', abbreviation,
	    'nickname', nickname,
	    'city', city,
	    'arena', arena,
	    'year_founded', yearfounded 
	    )
from teams_deduped
where row_num = 1



select 
      v.properties->>'player_name',
      max(e.properties->>'pts')
      from vertices v join edges e
on e.subject_identifier = v.identifier 
and e.subject_type = v.type
group by 1
order by 2 desc 






-- inseting the  player edges
insert into edges
with deduped AS(
select *, row_number() over (partition by player_id,game_id) as row_num
from game_details),
filtered as (select * from deduped 
where row_num =1
),aggregated as (
select 
	f1.player_id as subject_player_id,
	MAX(f1.player_name) as subject_player_name,
	f2.player_id as  object_player_id,
	MAX(f2.player_name) as object_player_name,
	case when f1.team_abbreviation = f2.team_abbreviation
	then 'shares_team'::edge_type
	else 'plays_against'::edge_type
	end as edge_type,
	COUNT(1) as num_games,
	SUM(f1.pts) as subject_points,
	SUM(f2.pts) as object_points
	from filtered f1
			join filtered F2
			on f1.game_id = f2.game_id
			and f1.player_name <> f2.player_name
	where f1.player_id > f2.player_id
	group by f1.player_id,
	f2.player_id,
	case when f1.team_abbreviation = f2.team_abbreviation
	then 'shares_team'::edge_type
	else 'plays_against'::edge_type
	end
	)
	select 
	subject_player_id as subject_identifier,
	'player'::vertex_type as subject_type,
	object_player_id as object_identifier,
	'player'::vertex_type as object_type,
	edge_type as edge_type,
	jsonb_build_object(
	'num_games', num_games,
	'subject_points', subject_points,
	'object_points', object_points) 
	from aggregated


-- inserting the game details egses
insert into edges
with deduped as (
select *, row_number() over (partition by player_id,game_id) as row_num
from game_details)
	select 
	player_id as subject_identifier,
	'player'::vertex_type as subject_type,
	game_id as object_identifer,
	'game'::vertex_type as object_type,
	'plays_in'::edge_type as edge_type,
	json_build_object(
		'start_position', start_position,
		'pts', pts,
		'teams_id', team_id,
		'team_abbreviation', team_abbreviation) as properties
 from deduped
where row_num = 1;


select type,COUNT(1)
from vertices v 
group by 1



--- final query
select 
v.properties->>'player_name',
e.object_identifier,
CAST(v.properties->>'number_of_games' as real)/
case when CAST(v.properties->>'total_points' as real) = 0 then 1 else 
CAST(v.properties->>'total_points' as real) end ,
e.properties->>'subject_points',
e.properties ->>'num_games'
from vertices v  join edges e
on v.identifier = e.subject_identifier
and v.type = e.subject_type
where e.object_type = 'player'::vertex_type