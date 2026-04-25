{{ config(materialized='table') }}

WITH home AS (
    SELECT 
        season, 
        league_name, 
        home_team AS team_name,
        1 AS is_home, 
        home_goals AS goals_scored, 
        away_goals AS goals_conceded,
        result_type
    FROM {{ ref('silver_team_results') }}
),
away AS (
    SELECT 
        season, 
        league_name, 
        away_team AS team_name,
        0 AS is_home, 
        away_goals AS goals_scored, 
        home_goals AS goals_conceded,
        CASE
            WHEN result_type = 'Home Win' THEN 'Away Loss'
            WHEN result_type = 'Away Win' THEN 'Away Win'
            ELSE 'Draw'
        END AS result_type
    FROM {{ ref('silver_team_results') }}
),
combined AS (
    SELECT * FROM home
    UNION ALL
    SELECT * FROM away
)

SELECT
    season,
    league_name,
    team_name,
    COUNT(*)                                                            AS total_matches,
    SUM(CASE WHEN is_home = 1 AND result_type = 'Home Win' THEN 1 ELSE 0 END) AS home_wins,
    SUM(CASE WHEN is_home = 1 AND result_type = 'Draw' THEN 1 ELSE 0 END)     AS home_draws,
    SUM(CASE WHEN is_home = 1 AND result_type = 'Away Win' THEN 1 ELSE 0 END) AS home_losses,
    SUM(CASE WHEN is_home = 0 AND result_type = 'Away Win' THEN 1 ELSE 0 END) AS away_wins,
    SUM(CASE WHEN is_home = 0 AND result_type = 'Draw' THEN 1 ELSE 0 END)     AS away_draws,
    SUM(CASE WHEN is_home = 0 AND result_type = 'Away Loss' THEN 1 ELSE 0 END) AS away_losses,
    SUM(goals_scored)                                                   AS goals_scored,
    SUM(goals_conceded)                                                 AS goals_conceded,
    SUM(goals_scored) - SUM(goals_conceded)                             AS goal_difference,
    ROUND(AVG(goals_scored + goals_conceded), 2)                        AS avg_match_goals
FROM combined
GROUP BY season, league_name, team_name
