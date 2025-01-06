

-- Define an ENUM type to represent the possible states of a player's career progression.
CREATE TYPE state_class AS ENUM (
    'New',
    'Retired',
    'Continued Playing',
    'Returned from Retirement',
    'Stayed Retired'
);

-- Create the main table for tracking players' growth and activity over seasons.
CREATE TABLE players_growth_accounting (
    player_name TEXT,                      -- Name of the player.
    first_active_season INTEGER,          -- The first season the player was active.
    last_active_season INTEGER,           -- The last season the player was active.
    season_state state_class,             -- The player's state during the current season (uses state_class ENUM).
    seasons_active INTEGER[],             -- Array of all seasons the player was active.
    current_season INTEGER,               -- The current season being evaluated.
    PRIMARY KEY (player_name, current_season) -- Composite primary key to uniquely identify each player by name and season.
);

-- Uncomment this line to drop the table if needed for re-creation.
-- DROP TABLE players_growth_accounting;

-- Insert or update player data for the new season (2025) based on historical data.
WITH last_season AS (
    -- Fetch player data from the 2024 season.
    SELECT * FROM players_growth_accounting
    WHERE current_season = 2024
),
this_season AS (
    -- Fetch unique players active in the 2025 season.
    SELECT DISTINCT 
        p.player_name,
        ps.season AS current_season
    FROM players p
    JOIN players_scd pscd ON p.player_name = pscd.player_name
    JOIN player_seasons ps ON p.player_name = ps.player_name
    WHERE ps.season = 2025
      AND p.player_name IS NOT NULL
)
-- Combine data from the last season and the current season to determine player state and update the table.
SELECT 
    COALESCE(t.player_name, l.player_name) AS player_name, -- Use current season player name or fallback to last season.
    COALESCE(l.first_active_season, t.current_season) AS first_active_season, -- First active season (fallback to current season if new player).
    COALESCE(t.current_season, l.last_active_season) AS last_active_season, -- Last active season (updated if active this season).
    CASE
        WHEN l.player_name IS NULL THEN 'New' -- Player is new this season.
        WHEN l.last_active_season = t.current_season - 1 THEN 'Continued Playing' -- Player continued without a break.
        WHEN l.last_active_season < t.current_season - 1 THEN 'Returned from Retirement' -- Player returned after a break.
        WHEN t.current_season IS NULL AND l.last_active_season = l.current_season THEN 'Retired' -- Player retired this season.
        ELSE 'Stayed Retired'::state_class -- Player remained retired.
    END AS season_state,
    COALESCE(l.seasons_active, ARRAY[]::INTEGER[]) || 
    CASE
        WHEN t.player_name IS NOT NULL THEN ARRAY[t.current_season] -- Append the current season if player is active.
        ELSE ARRAY[]::INTEGER[] -- Empty array if not active.
    END AS season_list,
    COALESCE(t.current_season, l.current_season + 1) AS year -- Determine the year for insertion.
FROM this_season t
FULL OUTER JOIN last_season l ON t.player_name = l.player_name; -- Join to combine current and historical data.

-- Query all records from the updated table for validation.
SELECT * FROM players_growth_accounting;

-- Fetch all seasons for a specific player (e.g., Aaron Brooks) for analysis.
SELECT DISTINCT 
    p.player_name,
    ps.season
FROM players p
JOIN players_scd pscd ON p.player_name = pscd.player_name
JOIN player_seasons ps ON p.player_name = ps.player_name
WHERE p.player_name = 'Aaron Brooks';




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












-- What is the most games a team has won in a 90-game stretch?

-- Step 1: Rank games by team and game date to prepare for cumulative calculations
WITH ranked_games AS (
    SELECT 
        team_id,  -- Unique identifier for the team
        team_abbreviation,  -- Shortened name of the team
        season,  -- Season the game belongs to
        game_date_est AS game_date,  -- Game date in Eastern Standard Time
        -- Determine if the team won the game:
        -- 1 if the team is the home team and won or the away team and won
        CASE 
            WHEN (team_id = home_team_id AND home_team_wins = 1)
              OR (team_id <> home_team_id AND home_team_wins = 0) 
            THEN 1 ELSE 0 
        END AS win,
        -- Assign a rank to games based on date for each team
        ROW_NUMBER() OVER (PARTITION BY team_id, game_date_est, gd.game_id 
                           ORDER BY game_date_est) AS game_rank
    FROM game_details gd
    JOIN games g ON g.game_id = gd.game_id
),

-- Step 2: Calculate cumulative wins over the last 90 games for each team
cumulative_wins AS (
    SELECT 
        team_id,  -- Team identifier
        season,  -- Season identifier
        game_date,  -- Game date
        team_abbreviation,  -- Team abbreviation
        -- Sum of wins in the current game and the preceding 89 games
        SUM(win) OVER (
            PARTITION BY team_id 
            ORDER BY game_rank 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_in_90_games
    FROM ranked_games
)

-- Step 3: Find the maximum wins in any 90-game stretch for each team and season
SELECT 
    team_id,  -- Team identifier
    team_abbreviation,  -- Team abbreviation
    season,  -- Season identifier
    MAX(wins_in_90_games) AS max_wins_in_90_games  -- Maximum wins in a 90-game stretch
FROM cumulative_wins
GROUP BY team_id, team_abbreviation, season
ORDER BY MAX(wins_in_90_games) DESC  -- Sort by maximum wins in descending order
LIMIT 2;  -- Limit results to the top 2 teams

-- How many games in a row did LeBron James score over 10 points?

-- Step 1: Filter games for LeBron James and calculate whether he scored over 10 points
WITH PlayerGames AS (
    SELECT
        player_name,  -- Player's name
        game_date_est AS game_date,  -- Game date
        season,  -- Season identifier
        g.game_id,  -- Game identifier
        pts AS points_scored,  -- Points scored by the player in the game
        -- Flag if the player scored more than 10 points in the game
        CASE 
            WHEN pts > 10 THEN 1
            ELSE 0
        END AS scored_over_10
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
    WHERE player_name = 'LeBron James'  -- Filter for LeBron James
),

-- Step 2: Identify streak groups by comparing row numbers
GameStreaks AS (
    SELECT
        *,
        -- Calculate streak groups using the difference between row numbers
        ROW_NUMBER() OVER (ORDER BY game_date)
        - ROW_NUMBER() OVER (PARTITION BY scored_over_10 ORDER BY game_date) AS streak_group
    FROM PlayerGames
)

-- Step 3: Calculate streak statistics for games where he scored over 10 points
SELECT
    player_name,  -- Player's name
    streak_group,  -- Group identifier for streaks
    season,  -- Season identifier
    MIN(game_date) AS streak_start,  -- Start date of the streak
    MAX(game_date) AS streak_end,  -- End date of the streak
    COUNT(game_id) AS games_in_streak  -- Total number of games in the streak
FROM GameStreaks
WHERE scored_over_10 = 1  -- Consider only streaks where he scored over 10 points
GROUP BY player_name, season, streak_group
ORDER BY COUNT(game_id) DESC  -- Sort by longest streak in descending order
LIMIT 1;  -- Limit results to the longest streak
