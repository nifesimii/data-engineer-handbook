-- Task 1: Create a DDL for the `actors` table with specified fields
CREATE TABLE actors (
    actor TEXT, -- Name of the actor
    actorid TEXT, -- Unique identifier for the actor
    films FILMS[], -- Array of film details for the actor
    quality_class QUALITY_CLASS, -- Enum categorizing the actor's overall quality
    current_year INTEGER, -- The year this record corresponds to
    is_active BOOLEAN, -- Indicates if the actor was active in the current year
    PRIMARY KEY (actorid, current_year) -- Composite primary key ensuring uniqueness for each actor and year
);

-- Define custom structured and enum types
CREATE TYPE films AS (
    film TEXT, -- Name of the film
    votes INTEGER, -- Number of votes for the film
    rating INTEGER, -- Rating of the film
    filmid TEXT -- Unique identifier for the film
);

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad'); -- Quality categories for an actor

-- Task 2: Populate the `actors` table cumulatively, one year at a time
DO $$
DECLARE
    year_start INT := 1970; -- Initial year to start populating data
    year_end INT := 2021; -- Final year to process data
    asofyear INT; -- Loop variable for the previous year
    asofthisyear INT; -- Loop variable for the current year
BEGIN
    -- Loop through each year between `year_start` and `year_end`
    FOR asofyear IN year_start..year_end LOOP
        asofthisyear := asofyear + 1; -- Calculate the current year

        -- Insert cumulative data into `actors`
        INSERT INTO actors
        WITH last_year AS (
            SELECT * FROM actors
            WHERE current_year = asofyear -- Retrieve actor data for the previous year
        ),
        this_year AS (
            SELECT 
                actor,
                actorid,
                year,
                CASE 
                    WHEN year IS NULL THEN ARRAY[]::films[] -- No films if the year is null
                    ELSE ARRAY_AGG(ROW(film, votes, rating, filmid)::films) -- Aggregate film data into an array
                END AS films,
                AVG(rating) AS average_rating -- Calculate average rating of films
            FROM actor_films af
            WHERE year = asofthisyear -- Filter data for the current year
            GROUP BY actor, actorid, year
        )
        SELECT 
            COALESCE(ty.actor, ly.actor) AS actor, -- Use data from the current year or carry over from the previous year
            COALESCE(ty.actorid, ly.actorid) AS actorid,
            COALESCE(ly.films, ARRAY[]::films[]) || 
                CASE 
                    WHEN ty.year IS NOT NULL THEN ty.films 
                    ELSE ARRAY[]::films[] 
                END AS films, -- Merge films from both years
            CASE 
                WHEN ARRAY_LENGTH(ty.films, 1) IS NOT NULL THEN
                    CASE 
                        WHEN ty.average_rating > 8 THEN 'star'
                        WHEN ty.average_rating > 7 THEN 'good'
                        WHEN ty.average_rating > 6 THEN 'average'
                        ELSE 'bad'
                    END::quality_class
                ELSE ly.quality_class
            END AS quality_class, -- Determine quality class based on ratings
            COALESCE(ty.year, ly.current_year + 1) AS current_year, -- Assign the current year
            CASE 
                WHEN ARRAY_LENGTH(ty.films, 1) IS NULL THEN FALSE
                ELSE TRUE
            END AS is_active -- Determine if the actor is active
        FROM last_year ly
        FULL OUTER JOIN this_year ty
            ON ly.actorid = ty.actorid; -- Join previous and current year data
    END LOOP;
END $$;

-- Task 3: Create a DDL for `actors_history_scd` table to manage historical records
CREATE TABLE actors_history_scd (
    actor TEXT, -- Actor name
    actorid TEXT, -- Actor unique identifier
    quality_class QUALITY_CLASS, -- Actor's quality class
    is_active BOOLEAN, -- Whether the actor was active
    start_date INTEGER, -- Start year of the record
    end_date INTEGER, -- End year of the record
    current_year INTEGER, -- Reference year for incremental updates
    PRIMARY KEY (actorid, start_date) -- Unique key for actor and start date
);

-- Define a structured type for SCD (slowly changing dimension)
CREATE TYPE actor_scd AS (
    quality_class QUALITY_CLASS, -- Quality class for the period
    is_active BOOLEAN, -- Active status for the period
    start_date INTEGER, -- Start date of the period
    end_date INTEGER -- End date of the period
);

-- Task 4: Backfill query for `actors_history_scd` table
INSERT INTO actors_history_scd
WITH with_previous AS (
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
    SELECT *, 
        CASE 
            WHEN quality_class <> previous_quality_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
),
with_streaks AS (
    SELECT *,
        SUM(change_indicator) OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
    FROM with_indicators
)
SELECT 
    actor,
    actorid,
    quality_class,
    is_active,
    MIN(current_year) AS start_year,
    MAX(current_year) AS end_year,
    2021 AS current_year
FROM with_streaks
GROUP BY actor, actorid, streak_identifier, quality_class, is_active
ORDER BY actor, actorid;

-- Task 5: Incremental query for `actors_history_scd` table
WITH last_year_scd AS (
    SELECT * 
    FROM actors_history_scd
    WHERE current_year = 2020 AND end_date = 2021
),
historical_scd AS (
    SELECT actor, actorid, quality_class, is_active, start_date, end_date
    FROM actors_history_scd
    WHERE current_year = 2020 AND end_date < 2020
),
this_year_data AS (
    SELECT * 
    FROM actors
    WHERE current_year = 2021
),
unchanged_records AS (
    SELECT 
        ts.actor,
        ts.actorid,
        ts.quality_class,
        ts.is_active,
        ls.start_date,
        ts.current_year AS end_year
    FROM this_year_data ts
    JOIN last_year_scd ls
        ON ts.actorid = ls.actorid
    WHERE ts.quality_class = ls.quality_class
      AND ts.is_active = ls.is_active
),
changed_records AS (
    SELECT 
        ts.actor,
        ts.actorid,
        UNNEST(ARRAY[
            ROW(ls.quality_class, ls.is_active, ls.start_date, ls.end_date)::actor_scd,
            ROW(ts.quality_class, ts.is_active, ts.current_year, ts.current_year)::actor_scd
        ]) AS records
    FROM this_year_data ts
    LEFT JOIN last_year_scd ls
        ON ts.actorid = ls.actorid
    WHERE ts.quality_class <> ls.quality_class OR ts.is_active <> ls.is_active
),
unnested_changed_records AS (
    SELECT 
        actor,
        actorid,
        (records::actor_scd).quality_class,
        (records::actor_scd).is_active,
        (records::actor_scd).start_date,
        (records::actor_scd).end_date
    FROM changed_records
),
new_records AS (
    SELECT 
        ts.actor,
        ts.actorid,
        ts.quality_class,
        ts.is_active,
        ts.current_year AS start_date,
        ts.current_year AS end_date
    FROM this_year_data ts
    LEFT JOIN last_year_scd ls
        ON ts.actorid = ls.actorid
    WHERE ls.actorid IS NULL
)
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;
