
--What is the most games a team has won in a 90 game stretch?

WITH ranked_games AS (
    SELECT 
        team_id,
        team_abbreviation,
        season,
        game_date_est as game_date,
        case when (team_id = home_team_id and home_team_wins = 1)
           or (team_id <> home_team_id and home_team_wins = 0) then 1 else 0 end as win,
        ROW_NUMBER() OVER (PARTITION BY team_id,game_date_est,gd.game_id ORDER BY game_date_est) AS game_rank
    from game_details gd
    join games g on 
     g.game_id = gd.game_id
)
,cumulative_wins AS (
    SELECT 
        team_id,
        season,
        game_date,
         team_abbreviation,
        SUM(win) OVER (PARTITION BY team_id ORDER BY game_rank ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS wins_in_90_games
    FROM ranked_games
)
SELECT 
    team_id,
     team_abbreviation,
     season,
    MAX(wins_in_90_games) AS max_wins_in_90_games
FROM cumulative_wins
GROUP BY team_id, team_abbreviation,season
order by MAX(wins_in_90_games) desc 
limit 2
;



--How many games in a row did LeBron James score over 10 points a game?


WITH PlayerGames AS (
    select
        player_name,
        game_date_est as game_date,
        season,
        g.game_id,
        pts as points_scored,
        CASE 
            WHEN pts > 10 THEN 1
            ELSE 0
        END AS scored_over_10
        from game_details gd join games g 
        on gd.game_id = g.game_id
        where 
        player_name = 'LeBron James'
)
,
GameStreaks AS (
    SELECT
        *,
        ROW_NUMBER() OVER ( ORDER by game_date)
        -
ROW_NUMBER() OVER (partition by scored_over_10 ORDER by game_date) AS streak_group
  FROM
      PlayerGames
)
SELECT
    player_name,
    streak_group,
    season,
    MIN(game_date) AS streak_start,
    MAX(game_date) AS streak_end,
    COUNT(game_id) AS games_in_streak
FROM
    GameStreaks
WHERE
    scored_over_10 = 1
GROUP BY
    player_name, season,streak_group
ORDER BY
     COUNT(game_id) desc 
     limit 1