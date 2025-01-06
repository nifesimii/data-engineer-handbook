-- Creating the ddl for the session events table in postgressql

CREATE TABLE session_events (
    session_start TIMESTAMP(3),  -- Start time of the session, with millisecond precision
    session_end TIMESTAMP(3),    -- End time of the session, with millisecond precision
    ip VARCHAR,                  -- IP address of the user
    host VARCHAR,                -- Hostname or domain accessed
    num_events BIGINT            -- Total number of events in the session
);




-- This query retrieves session data for the user with IP address '74.96.167.138'
-- and calculates the average number of events per session across different hosts.

SELECT
    ip ,                           -- Alias the IP column for clarity
    host,                                    -- Include the host column
    CAST(AVG(num_events) AS BIGINT) AS avg_num_events  -- Calculate average events and cast to BIGINT
FROM 
    session_events                           -- Table containing session event data
WHERE 
    ip = '74.96.167.138'                     -- Filter by specific IP
    AND host = 'zachwilson.techcreator.io'   -- Filter by specific host
GROUP BY 
    ip,                                      -- Group by IP
    host;                                    -- Group by host for accurate aggregation

