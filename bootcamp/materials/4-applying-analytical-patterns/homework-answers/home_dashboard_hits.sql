select * from game_details gd 

drop table teams_hits_dashboard


--QUESTION ONE 

create table teams_hits_dashboard as 
with games_aggregated as (
  select 
      gd.team_abbreviation,
       gd.player_name,
       g.season,
           case when (team_id = home_team_id and home_team_wins = 1)
           or (team_id <> home_team_id and home_team_wins = 0) then 1 else 0 end as game_won,
           gd.game_id,
           gd.pts as player_points
    from game_details gd
    join games g on 
     g.game_id = gd.game_id
     where pts is not null and season is not null
), grouped_data as 
(select 
    	   coalesce(team_abbreviation, '(overall)') as team,
           coalesce(player_name, '(overall)') as player_name,
           coalesce(cast(season as varchar),'(overall)') as season,
       case
           when grouping(player_name,team_abbreviation) = 0
               then 'player_name__team'
           when grouping(player_name,season) = 0 
           	   then 'player_name__season'
           when grouping(team_abbreviation) = 0 then 'team'
       end as aggregation_level,
	   sum(player_points) as points_sum,
	   count(distinct (case when game_won = 1 then game_id end))
from games_aggregated
group by grouping sets (
		(player_name,team_abbreviation),
        (player_name,season),
        (team_abbreviation)
    ))
   select * from grouped_data;
--order by number_of_team_wins desc;

   
   


select player_name,player_name,season,points_sum  from teams_hits_dashboard where aggregation_level = 'player_name__season'
order by points_sum desc
limit 1


select team, player_name,points_sum  from teams_hits_dashboard where aggregation_level = 'player_name__team'
order by points_sum desc
limit 1


select team, aggregation_level,count  from teams_hits_dashboard where aggregation_level = 'team'
order by count desc
limit 1
