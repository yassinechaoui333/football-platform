CREATE DATABASE airflow_db;

-- Connect to football_db to create tables
\c football_db;

CREATE TABLE IF NOT EXISTS bronze_standings (
    id          SERIAL PRIMARY KEY,
    season      INT NOT NULL,
    league_name VARCHAR(100) NOT NULL,
    team_name   VARCHAR(100) NOT NULL,
    rank        INT,
    points      INT,
    played      INT,
    won         INT,
    drawn       INT,
    lost        INT,
    goals_for   INT,
    goals_against INT,
    loaded_at   TIMESTAMP DEFAULT NOW(),
    UNIQUE(season, league_name, team_name)
);
CREATE INDEX IF NOT EXISTS idx_standings_season ON bronze_standings(season, league_name);

CREATE TABLE IF NOT EXISTS bronze_top_scorers (
    id           SERIAL PRIMARY KEY,
    season       INT NOT NULL,
    league_name  VARCHAR(100) NOT NULL,
    player_name  VARCHAR(100) NOT NULL,
    team_name    VARCHAR(100) NOT NULL,
    goals        INT DEFAULT 0,
    assists      INT DEFAULT 0,
    yellow_cards INT DEFAULT 0,
    red_cards    INT DEFAULT 0,
    loaded_at    TIMESTAMP DEFAULT NOW(),
    UNIQUE(season, league_name, player_name, team_name)
);
CREATE INDEX IF NOT EXISTS idx_scorers_season ON bronze_top_scorers(season, league_name);

CREATE TABLE IF NOT EXISTS bronze_team_results (
    id          SERIAL PRIMARY KEY,
    season      INT NOT NULL,
    league_name VARCHAR(100) NOT NULL,
    match_date  DATE NOT NULL,
    home_team   VARCHAR(100) NOT NULL,
    away_team   VARCHAR(100) NOT NULL,
    home_goals  INT DEFAULT 0,
    away_goals  INT DEFAULT 0,
    status      VARCHAR(50),
    loaded_at   TIMESTAMP DEFAULT NOW(),
    UNIQUE(season, league_name, match_date, home_team, away_team)
);
CREATE INDEX IF NOT EXISTS idx_results_season ON bronze_team_results(season, league_name);

CREATE TABLE IF NOT EXISTS bronze_player_stats (
    id              SERIAL PRIMARY KEY,
    season          INT NOT NULL,
    league_name     VARCHAR(100) NOT NULL,
    player_name     VARCHAR(100) NOT NULL,
    team_name       VARCHAR(100) NOT NULL,
    position        VARCHAR(50),
    appearances     INT DEFAULT 0,
    goals           INT DEFAULT 0,
    assists         INT DEFAULT 0,
    minutes_played  INT DEFAULT 0,
    loaded_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE(season, league_name, player_name, team_name)
);
CREATE INDEX IF NOT EXISTS idx_player_stats_season ON bronze_player_stats(season, league_name);
