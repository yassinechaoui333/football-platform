{{ config(materialized='table') }}

SELECT
    season,
    league_name,
    COUNT(DISTINCT team_name)                    AS total_teams,
    SUM(goals_for)                               AS total_goals,
    ROUND(AVG(goals_for), 2)                     AS avg_goals_per_team,
    MAX(points)                                  AS highest_points,
    MIN(points)                                  AS lowest_points,
    ROUND(AVG(win_percentage), 2)                AS avg_win_percentage
FROM {{ ref('silver_standings') }}
GROUP BY season, league_name
