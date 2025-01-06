-- Query to perform state change tracking for players:
-- A player entering the league: 'New'
-- A player leaving the league: 'Retired'
-- A player staying in the league: 'Continued Playing'
-- A player coming out of retirement: 'Returned from Retirement'
-- A player staying out of the league: 'Stayed Retired'

-- Selecting all records from game_details (base table for game-related data)
select * from game_details gd;

-- Dropping the `teams_hits_dashboard` table if it exists (clean slate for the new data)
drop table teams_hits_dashboard;


-- Creating a new table `teams_hits_dashboard` to store aggregated player and team statistics
create table teams_hits_dashboard as 
with games_aggregated as (
  -- Aggregating data to calculate individual game-level performance and game wins
  select 
      gd.team_abbreviation, -- Team abbreviation for grouping
      gd.player_name,       -- Player name for grouping
      g.season,             -- Season of the game
      case 
          -- Calculating if a game was won by the player's team
          when (team_id = home_team_id and home_team_wins = 1)
          or (team_id <> home_team_id and home_team_wins = 0) 
          then 1 
          else 0 
      end as game_won,
      gd.game_id,           -- Game ID for uniqueness
      gd.pts as player_points -- Player points scored in the game
  from game_details gd
  join games g on g.game_id = gd.game_id -- Joining to enrich game details with season data
  where pts is not null and season is not null -- Ensuring valid points and season data
), 
grouped_data as (
  -- Aggregating data at multiple levels: player/team, player/season, team
  select 
      coalesce(team_abbreviation, '(overall)') as team, -- Default grouping value for team
      coalesce(player_name, '(overall)') as player_name, -- Default grouping value for player
      coalesce(cast(season as varchar), '(overall)') as season, -- Default grouping value for season
      case 
          -- Determining the aggregation level for easier analysis
          when grouping(player_name, team_abbreviation) = 0 then 'player_name__team'
          when grouping(player_name, season) = 0 then 'player_name__season'
          when grouping(team_abbreviation) = 0 then 'team'
      end as aggregation_level,
      sum(player_points) as points_sum, -- Total points scored
      count(distinct (case when game_won = 1 then game_id end)) as number_of_team_wins -- Total wins
  from games_aggregated
  group by grouping sets (
      -- Grouping sets for multi-level aggregation
      (player_name, team_abbreviation),
      (player_name, season),
      (team_abbreviation)
  )
)
-- Selecting all columns from the grouped data to populate the `teams_hits_dashboard` table
select * from grouped_data;

-- Query 1: Retrieve the player with the highest points in a single season
select player_name, season, points_sum  
from teams_hits_dashboard 
where aggregation_level = 'player_name__season' 
order by points_sum desc 
limit 1;

-- Query 2: Retrieve the player with the highest points for a specific team
select team, player_name, points_sum  
from teams_hits_dashboard 
where aggregation_level = 'player_name__team' 
order by points_sum desc 
limit 1;

-- Query 3: Retrieve the team with the highest number of wins
select team, aggregation_level, count 
from teams_hits_dashboard 
where aggregation_level = 'team' 
order by count desc 
limit 1;




