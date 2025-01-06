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
