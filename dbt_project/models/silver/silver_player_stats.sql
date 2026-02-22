{{ config(materialized='view') }}

SELECT
    season,
    league_name,
    player_name,
    team_name,
    position,
    appearances,
    goals,
    assists,
    minutes_played,
    goals + assists AS goal_contributions,
    ROUND(goals::NUMERIC / NULLIF(appearances, 0), 2) AS goals_per_game,
    ROUND(assists::NUMERIC / NULLIF(appearances, 0), 2) AS assists_per_game,
    ROUND(minutes_played::NUMERIC / NULLIF(appearances, 0), 2) AS minutes_per_game
FROM {{ source('bronze', 'bronze_player_stats') }}
WHERE player_name IS NOT NULL
  AND appearances > 0
