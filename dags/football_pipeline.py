from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import json
import os
import time
import psycopg2


# ============================================
# CONFIG
# ============================================
LEAGUES_API = {
    "premier_league":   {"id": 39,  "name": "Premier League"},
    "la_liga":          {"id": 140, "name": "La Liga"},
    "bundesliga":       {"id": 78,  "name": "Bundesliga"},
    "serie_a":          {"id": 135, "name": "Serie A"},
    "ligue_1":          {"id": 61,  "name": "Ligue 1"},
    "champions_league": {"id": 2,   "name": "Champions League"},
}

# Determine current season dynamically (e.g. if before August, use previous year)
current_month = datetime.now().month
current_year = datetime.now().year
SEASON = current_year if current_month >= 8 else current_year - 1
HEADERS = {"x-apisports-key": os.environ["API_KEY"]}
BASE_URL = "https://v3.football.api-sports.io"

# ============================================
# HELPERS
# ============================================
def get_connection():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "postgres"),
        database=os.environ.get("DB_NAME", "football_db"),
        user=os.environ.get("DB_USER", "football_user"),
        password=os.environ.get("DB_PASSWORD", "football_pass")
    )

def api_get(endpoint, params):
    response = requests.get(f"{BASE_URL}/{endpoint}", headers=HEADERS, params=params)
    return response.json()

# ============================================
# TASKS
# ============================================
def fetch_standings():
    print("📊 Fetching standings...")
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM bronze_standings WHERE season = %s", (SEASON,))
        conn.commit()

    for league_key, league_info in LEAGUES_API.items():
        data = api_get("standings", {"league": league_info["id"], "season": SEASON})
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
                    SEASON, league_name, team["team"]["name"],
                    team["rank"], team["points"],
                    team["all"]["played"], team["all"]["win"],
                    team["all"]["draw"], team["all"]["lose"],
                    team["all"]["goals"]["for"], team["all"]["goals"]["against"],
                ))
        conn.commit()
        time.sleep(7)
    conn.close()
    print("✅ Standings done!")

def fetch_top_scorers():
    print("⚽ Fetching top scorers...")
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM bronze_top_scorers WHERE season = %s", (SEASON,))
        conn.commit()

    for league_key, league_info in LEAGUES_API.items():
        for endpoint, prefix in [("players/topscorers", "scorers"), ("players/topassists", "assists")]:
            data = api_get(endpoint, {"league": league_info["id"], "season": SEASON})
            if not data.get("response"):
                continue
            with conn.cursor() as cur:
                for player in data["response"]:
                    stats = player["statistics"][0]
                    cur.execute("""
                        INSERT INTO bronze_top_scorers
                        (season, league_name, player_name, team_name, goals, assists, yellow_cards, red_cards)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        SEASON, league_info["name"],
                        player["player"]["name"], stats["team"]["name"],
                        stats["goals"]["total"] or 0, stats["goals"]["assists"] or 0,
                        stats["cards"]["yellow"] or 0, stats["cards"]["red"] or 0,
                    ))
            conn.commit()
            time.sleep(7)
    conn.close()
    print("✅ Top scorers done!")

def fetch_team_results():
    print("📋 Fetching team results...")
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM bronze_team_results WHERE season = %s", (SEASON,))
        conn.commit()

    for league_key, league_info in LEAGUES_API.items():
        data = api_get("fixtures", {
            "league": league_info["id"],
            "season": SEASON,
            "from": f"{SEASON}-08-01",
            "to": f"{SEASON+1}-06-01",
        })
        if not data.get("response"):
            continue
        with conn.cursor() as cur:
            for match in data["response"]:
                cur.execute("""
                    INSERT INTO bronze_team_results
                    (season, league_name, match_date, home_team, away_team, home_goals, away_goals, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    SEASON, match["league"]["name"],
                    match["fixture"]["date"][:10],
                    match["teams"]["home"]["name"],
                    match["teams"]["away"]["name"],
                    match["goals"]["home"] or 0,
                    match["goals"]["away"] or 0,
                    match["fixture"]["status"]["long"],
                ))
        conn.commit()
        time.sleep(7)
    conn.close()
    print("✅ Team results done!")

# Removed run_dbt Python function since we will use BashOperator directly

# ============================================
# DAG DEFINITION
# ============================================
default_args = {
    "owner": "yassine",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="football_pipeline",
    default_args=default_args,
    description="Daily football data pipeline",
    schedule_interval="0 6 * * *",  # runs every day at 6am
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["football", "pipeline"],
) as dag:

    t1 = PythonOperator(task_id="fetch_standings",    python_callable=fetch_standings)
    t2 = PythonOperator(task_id="fetch_top_scorers",  python_callable=fetch_top_scorers)
    t3 = PythonOperator(task_id="fetch_team_results", python_callable=fetch_team_results)
    
    t4 = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir /opt/airflow/dbt_project",
    )

    # Pipeline order: fetch tasks in parallel, then run dbt
    [t1, t2, t3] >> t4
