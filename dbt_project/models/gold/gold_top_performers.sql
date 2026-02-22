{{ config(materialized='table') }}

SELECT
    season,
    league_name,
    player_name,
    team_name,
    goals,
    assists,
    goal_contributions,
    yellow_cards,
    red_cards,
    RANK() OVER (PARTITION BY season, league_name ORDER BY goals DESC)              AS goals_rank,
    RANK() OVER (PARTITION BY season, league_name ORDER BY goal_contributions DESC) AS contributions_rank
FROM {{ ref('silver_top_scorers') }}

