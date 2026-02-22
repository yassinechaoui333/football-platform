{{ config(materialized='view') }}

SELECT
    season,
    league_name,
    team_name,
    rank,
    points,
    played,
    won,
    drawn,
    lost,
    goals_for,
    goals_against,
    goals_for - goals_against AS goal_difference,
    ROUND(won::NUMERIC / NULLIF(played, 0) * 100, 2) AS win_percentage
FROM {{ source('bronze', 'bronze_standings') }}
WHERE team_name IS NOT NULL
  AND points IS NOT NULL
