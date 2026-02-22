{{ config(materialized='view') }}

SELECT
    season,
    league_name,
    match_date,
    home_team,
    away_team,
    home_goals,
    away_goals,
    home_goals + away_goals AS total_goals,
    CASE
        WHEN home_goals > away_goals THEN home_team
        WHEN away_goals > home_goals THEN away_team
        ELSE 'Draw'
    END AS winner,
    CASE
        WHEN home_goals > away_goals THEN 'Home Win'
        WHEN away_goals > home_goals THEN 'Away Win'
        ELSE 'Draw'
    END AS result_type
FROM {{ source('bronze', 'bronze_team_results') }}
WHERE home_team IS NOT NULL
  AND away_team IS NOT NULL
