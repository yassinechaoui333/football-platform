{{ config(materialized='table') }}

SELECT
    season,
    league_name,
    player_name,
    team_name,
    position,
    appearances,
    goals,
    assists,
    goal_contributions,
    minutes_played,
    goals_per_game,
    assists_per_game,
    minutes_per_game,
    RANK() OVER (PARTITION BY season, league_name, position ORDER BY goal_contributions DESC) AS position_rank,
    RANK() OVER (PARTITION BY season, league_name ORDER BY goals_per_game DESC)               AS efficiency_rank
FROM {{ ref('silver_player_stats') }}
WHERE appearances >= 5
