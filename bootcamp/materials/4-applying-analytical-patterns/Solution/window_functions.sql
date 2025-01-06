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
