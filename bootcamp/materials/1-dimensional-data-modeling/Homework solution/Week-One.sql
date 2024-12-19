-- Task 1: Create a table for actors
-- The table `actors` holds actor-specific information for each year.
CREATE TABLE actors (
    actor TEXT, -- Name of the actor
    actorid TEXT, -- Unique identifier for the actor
    films FILMS[], -- Array of films associated with the actor
    quality_class QUALITY_CLASS, -- Enum representing the actor's quality classification
    current_year INTEGER, -- The year this data pertains to
    is_active BOOLEAN, -- Indicates if the actor is active in the given year
    PRIMARY KEY (actorid, current_year) -- Composite primary key ensures unique records per actor per year
);

-- Create the `FILMS` type to represent individual film details
CREATE TYPE FILMS AS (
    film TEXT, -- Title of the film
    votes INTEGER, -- Number of votes received by the film
    rating INTEGER, -- Average rating of the film
    filmid TEXT -- Unique identifier for the film
);

-- Create the `QUALITY_CLASS` enum to classify actor performance
CREATE TYPE QUALITY_CLASS AS ENUM ('star', 'good', 'average', 'bad');

-- Task 2: Populate the actors table cumulatively
-- This block of PL/pgSQL inserts actor data year by year, aggregating film and rating details
DO $$
DECLARE
    year_start INT := 1970; -- Starting year for the data
    year_end INT := 2021; -- Ending year for the data
    asofyear INT; -- Current iteration year
    asofthisyear INT; -- Next year in the iteration
BEGIN
    -- Loop through each year from the starting year to the ending year
    FOR asofyear IN year_start..year_end LOOP
        asofthisyear := asofyear + 1; -- Calculate the next year
        INSERT INTO actors
        WITH last_year AS (
            -- Retrieve actor data from the previous year
            SELECT * FROM actors
            WHERE current_year = asofyear
        ),
        this_year AS (
            -- Aggregate actor data for the current year
            SELECT
                actor,
                actorid,
                YEAR,
                CASE 
                    WHEN YEAR IS NULL THEN ARRAY[]::FILMS[] -- If no films, create an empty array
                    ELSE ARRAY_AGG(ROW(film, votes, rating, filmid)::FILMS) -- Aggregate films into an array
                END AS films,
                AVG(rating) AS average_rating -- Calculate average rating for the year
            FROM actor_films af 
            WHERE year = asofthisyear
            GROUP BY actor, actorid, year
        )
        SELECT
            COALESCE(ty.actor, ly.actor) AS actor, -- Use actor name from this year or last year
            COALESCE(ty.actorid, ly.actorid) AS actorid, -- Use actor ID from this year or last year
            COALESCE(ly.films, ARRAY[]::FILMS[]) || 
                CASE WHEN ty.YEAR IS NOT NULL THEN ty.films ELSE ARRAY[]::FILMS[] END AS films, -- Merge film arrays
            CASE 
                WHEN ARRAY_LENGTH(ty.films, 1) IS NOT NULL THEN
                    CASE 
                        WHEN ty.average_rating > 8 THEN 'star'
                        WHEN ty.average_rating > 7 AND ty.average_rating <= 8 THEN 'good'
                        WHEN ty.average_rating > 6 AND ty.average_rating <= 7 THEN 'average'
                        WHEN ty.average_rating <= 6 THEN 'bad'
                    END::QUALITY_CLASS 
                ELSE ly.quality_class
            END AS quality_class, -- Determine quality class based on average rating
            COALESCE(ty.YEAR, ly.current_year + 1) AS current_year, -- Set the year
            CASE 
                WHEN ARRAY_LENGTH(ty.films, 1) IS NULL THEN FALSE 
                ELSE TRUE 
            END AS is_active -- Determine if the actor was active this year
        FROM last_year ly
        FULL OUTER JOIN this_year ty ON ly.actorid = ty.actorid; -- Join last year's and this year's data
    END LOOP;
END $$;

-- Task 3: Create a table for tracking actor history with slowly changing dimensions (SCD)
-- The table `actors_history_scd` tracks changes to actor quality and activity over time
CREATE TABLE actors_history_scd (
    actor TEXT, -- Name of the actor
    actorid TEXT, -- Unique identifier for the actor
    quality_class QUALITY_CLASS, -- Actor's quality classification
    is_active BOOLEAN, -- Indicates if the actor was active
    start_date INTEGER, -- Start year of the period
    end_date INTEGER, -- End year of the period
    current_year INTEGER, -- Current year for this record
    PRIMARY KEY (actorid, start_date) -- Composite key ensures unique records per actor and start year
);

-- Define a structured type to represent SCD changes
CREATE TYPE actor_scd AS (
    quality_class QUALITY_CLASS, -- Actor's quality classification
    is_active BOOLEAN, -- Indicates if the actor was active
    start_date INTEGER, -- Start year of the period
    end_date INTEGER -- End year of the period
);

-- Task 4: Backfill the actors_history_scd table
-- This query processes historical data to populate the `actors_history_scd` table
INSERT INTO actors_history_scd
WITH with_previous AS (
    -- Fetch actor data and compare with previous year's data
    SELECT 
        actor,
        actorid,
        current_year,
        quality_class,
        is_active,
        LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
        LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
    FROM actors
    WHERE current_year <= 2021
),
with_indicators AS (
    -- Identify changes in quality or activity status
    SELECT *, 
        CASE 
            WHEN quality_class <> previous_quality_class THEN 1 
            WHEN is_active <> previous_is_active THEN 1 
            ELSE 0
        END AS change_indicator
    FROM with_previous
),
with_streaks AS (
    -- Assign a unique streak identifier for each continuous period
    SELECT *,
        SUM(change_indicator) OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
    FROM with_indicators
)
-- Aggregate streak data into start and end periods
SELECT 
    actor,
    actorid,
    quality_class,
    is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2021 AS current_year
FROM with_streaks
GROUP BY actor, actorid, streak_identifier, is_active, quality_class
ORDER BY actor, actorid;
