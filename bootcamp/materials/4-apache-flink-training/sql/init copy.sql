

CREATE TABLE session_events (
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),  
    ip VARCHAR,       
    host VARCHAR,
    num_events BIGINT
    )

-- Analyze user '74.96.167.138' on zachwilson.techcreator.io
SELECT
  ip AS user_ip,
  host,
  CAST(AVG(num_events) AS BIGINT) AS Average_num_events
FROM session_events
WHERE ip = '74.96.167.138'
AND host = 'zachwilson.techcreator.io'  -- Filter for specific host
GROUP BY ip, host;

-- Analyze user '74.96.167.138' on www.techcreator.io
SELECT
  ip AS user_ip,
  host,
  CAST(AVG(num_events) AS BIGINT) AS Average_num_events
FROM session_events
WHERE ip = '74.96.167.138'
AND host = 'www.techcreator.io'  -- Filter for specific host
GROUP BY ip, host;

-- Analyze user '74.96.167.138' on bootcamp.techcreator.io
SELECT
  ip AS user_ip,
  host,
  CAST(AVG(num_events) AS BIGINT) AS Average_num_events
FROM session_events
WHERE ip = '74.96.167.138'
AND host = 'bootcamp.techcreator.io'  -- Filter for specific host
GROUP BY ip, host;

-- Analyze user '74.96.167.138' on lulu.techcreator.io 
SELECT
  ip AS user_ip,
  host,
  CAST(AVG(num_events) AS BIGINT) AS Average_num_events
FROM session_events
WHERE ip = '74.96.167.138'
AND host = 'lulu.techcreator.io'  -- Filter for specific host
GROUP BY ip, host;