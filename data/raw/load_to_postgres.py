import psycopg2
import json
import os
import glob
from dotenv import load_dotenv

load_dotenv()

# ============================================
# CONNECTION
# ============================================
def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="football_db",
        user="football_user",
        password="football_pass"
    )

# ============================================
# CREATE BRONZE TABLES
# ============================================
def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze_standings (
                id          SERIAL PRIMARY KEY,
                season      INT,
                league_name VARCHAR(100),
                team_name   VARCHAR(100),
                rank        INT,
                points      INT,
                played      INT,
                won         INT,
                drawn       INT,
                lost        INT,
                goals_for   INT,
                goals_against INT,
                loaded_at   TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS bronze_top_scorers (
                id           SERIAL PRIMARY KEY,
                season       INT,
                league_name  VARCHAR(100),
                player_name  VARCHAR(100),
                team_name    VARCHAR(100),
                goals        INT,
                assists      INT,
                yellow_cards INT,
                red_cards    INT,
                loaded_at    TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS bronze_team_results (
                id          SERIAL PRIMARY KEY,
                season      INT,
                league_name VARCHAR(100),
                match_date  VARCHAR(50),
                home_team   VARCHAR(100),
                away_team   VARCHAR(100),
                home_goals  INT,
                away_goals  INT,
                status      VARCHAR(50),
                loaded_at   TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS bronze_player_stats (
                id              SERIAL PRIMARY KEY,
                season          INT,
                league_name     VARCHAR(100),
                player_name     VARCHAR(100),
                team_name       VARCHAR(100),
                position        VARCHAR(50),
                appearances     INT,
                goals           INT,
                assists         INT,
                minutes_played  INT,
                loaded_at       TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
    print("✅ Bronze tables created!")

# ============================================
# LOADERS
# ============================================
def load_standings(conn, season):
    files = glob.glob(f"data/raw/{season}/standings/*.json")
    total = 0
    for file in files:
        with open(file) as f:
            data = json.load(f)
        if not data.get("response"):
            continue
        league_name = data["response"][0]["league"]["name"]
        standings = data["response"][0]["league"]["standings"][0]
        with conn.cursor() as cur:
            for team in standings:
                cur.execute("""
                    INSERT INTO bronze_standings
                    (season, league_name, team_name, rank, points, played, won, drawn, lost, goals_for, goals_against)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    season,
                    league_name,
                    team["team"]["name"],
                    team["rank"],
                    team["points"],
                    team["all"]["played"],
                    team["all"]["win"],
                    team["all"]["draw"],
                    team["all"]["lose"],
                    team["all"]["goals"]["for"],
                    team["all"]["goals"]["against"],
                ))
        conn.commit()
        total += len(standings)
    print(f"  ✅ Standings loaded: {total} rows")

def load_top_scorers(conn, season):
    files = glob.glob(f"data/raw/{season}/top_scorers/top_scorers_*.json")
    total = 0
    for file in files:
        with open(file) as f:
            data = json.load(f)
        if not data.get("response"):
            continue
        league_name = data["parameters"]["league"]
        with conn.cursor() as cur:
            for player in data["response"]:
                stats = player["statistics"][0]
                cur.execute("""
                    INSERT INTO bronze_top_scorers
                    (season, league_name, player_name, team_name, goals, assists, yellow_cards, red_cards)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    season,
                    league_name,
                    player["player"]["name"],
                    stats["team"]["name"],
                    stats["goals"]["total"] or 0,
                    stats["goals"]["assists"] or 0,
                    stats["cards"]["yellow"] or 0,
                    stats["cards"]["red"] or 0,
                ))
        conn.commit()
        total += len(data["response"])
    print(f"  ✅ Top scorers loaded: {total} rows")

def load_team_results(conn, season):
    files = glob.glob(f"data/raw/{season}/team_results/*.json")
    total = 0
    for file in files:
        with open(file) as f:
            data = json.load(f)
        if not data.get("response"):
            continue
        with conn.cursor() as cur:
            for match in data["response"]:
                home_goals = match["goals"]["home"]
                away_goals = match["goals"]["away"]
                cur.execute("""
                    INSERT INTO bronze_team_results
                    (season, league_name, match_date, home_team, away_team, home_goals, away_goals, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    season,
                    match["league"]["name"],
                    match["fixture"]["date"][:10],
                    match["teams"]["home"]["name"],
                    match["teams"]["away"]["name"],
                    home_goals if home_goals is not None else 0,
                    away_goals if away_goals is not None else 0,
                    match["fixture"]["status"]["long"],
                ))
        conn.commit()
        total += len(data["response"])
    print(f"  ✅ Team results loaded: {total} rows")
    
def load_player_stats(conn, season):
    files = glob.glob(f"data/raw/{season}/player_stats/*.json")
    total = 0
    for file in files:
        with open(file) as f:
            data = json.load(f)
        if not data.get("response"):
            continue
        league_name = data["parameters"]["league"]
        with conn.cursor() as cur:
            for player in data["response"]:
                stats = player["statistics"][0]
                cur.execute("""
                    INSERT INTO bronze_player_stats
                    (season, league_name, player_name, team_name, position, appearances, goals, assists, minutes_played)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    season,
                    league_name,
                    player["player"]["name"],
                    stats["team"]["name"],
                    stats["games"]["position"],
                    stats["games"]["appearences"] or 0,
                    stats["goals"]["total"] or 0,
                    stats["goals"]["assists"] or 0,
                    stats["games"]["minutes"] or 0,
                ))
        conn.commit()
        total += len(data["response"])
    print(f"  ✅ Player stats loaded: {total} rows")

# ============================================
# MAIN
# ============================================
def run_all():
    conn = get_connection()
    print("\n🚀 Creating Bronze tables...")
    create_tables(conn)

    for season in [2022, 2023, 2024]:
        print(f"\n📅 Loading season {season}...")
        load_standings(conn, season)
        load_top_scorers(conn, season)
        load_team_results(conn, season)
        load_player_stats(conn, season)

    conn.close()
    print("\n✅ All data loaded into PostgreSQL!")

if __name__ == "__main__":
    run_all()
