{{ config(materialized='table') }}

SELECT
    season,
    league_name,
    home_team AS team_name,
    COUNT(*)                                                        AS total_matches,
    SUM(CASE WHEN result_type = 'Home Win' THEN 1 ELSE 0 END)      AS home_wins,
    SUM(CASE WHEN result_type = 'Draw' THEN 1 ELSE 0 END)          AS draws,
    SUM(CASE WHEN result_type = 'Away Win' THEN 1 ELSE 0 END)      AS home_losses,
    SUM(home_goals)                                                 AS goals_scored,
    SUM(away_goals)                                                 AS goals_conceded,
    SUM(home_goals) - SUM(away_goals)                              AS goal_difference,
    ROUND(AVG(total_goals), 2)                                      AS avg_match_goals
FROM {{ ref('silver_team_results') }}
GROUP BY season, league_name, home_team
