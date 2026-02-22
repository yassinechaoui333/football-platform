{{ config(materialized='view') }}

SELECT
    season,
    league_name,
    player_name,
    team_name,
    goals,
    assists,
    yellow_cards,
    red_cards,
    goals + assists AS goal_contributions
FROM {{ source('bronze', 'bronze_top_scorers') }}
WHERE player_name IS NOT NULL
  AND goals IS NOT NULL
