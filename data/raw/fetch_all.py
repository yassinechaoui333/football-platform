import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# ============================================
# LEAGUES CONFIGURATION
# ============================================
LEAGUES = {
    "premier_league":   {"id": 39,  "name": "Premier League"},
    "la_liga":          {"id": 140, "name": "La Liga"},
    "bundesliga":       {"id": 78,  "name": "Bundesliga"},
    "serie_a":          {"id": 135, "name": "Serie A"},
    "ligue_1":          {"id": 61,  "name": "Ligue 1"},
    "champions_league": {"id": 2,   "name": "Champions League"},
}

HEADERS = {"x-apisports-key": os.getenv("API_KEY")}
BASE_URL = "https://v3.football.api-sports.io"

# ============================================
# HELPER
# ============================================
def save_json(data, folder, filename, season):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"data/raw/{season}/{folder}/{filename}_{timestamp}.json"
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  ✅ Saved → {path}")
    return path

def get(endpoint, params):
    response = requests.get(f"{BASE_URL}/{endpoint}", headers=HEADERS, params=params)
    return response.json()

# ============================================
# FETCHERS
# ============================================
def fetch_standings(league_id, league_key, season):
    print(f"📊 Fetching standings for {league_key}...")
    data = get("standings", {"league": league_id, "season": season})
    save_json(data, "standings", f"standings_{league_key}", season)

def fetch_top_scorers(league_id, league_key, season):
    print(f"⚽ Fetching top scorers for {league_key}...")
    data = get("players/topscorers", {"league": league_id, "season": season})
    save_json(data, "top_scorers", f"top_scorers_{league_key}", season)

def fetch_top_assists(league_id, league_key, season):
    print(f"🎯 Fetching top assists for {league_key}...")
    data = get("players/topassists", {"league": league_id, "season": season})
    save_json(data, "top_scorers", f"top_assists_{league_key}", season)

def fetch_live_scores(season):
    print(f"🔴 Fetching live scores...")
    data = get("fixtures", {"live": "all"})
    save_json(data, "live_scores", "live_scores", season)

def fetch_team_results(league_id, league_key, season):
    print(f"📋 Fetching results for {league_key}...")
    data = get("fixtures", {
        "league": league_id,
        "season": season,
        "from": f"{season}-08-01",
        "to": f"{season+1}-06-01",
    })
    save_json(data, "team_results", f"results_{league_key}", season)

def fetch_player_stats(league_id, league_key, season):
    print(f"👤 Fetching player stats for {league_key}...")
    data = get("players", {
        "league": league_id,
        "season": season,
        "page": 1
    })
    save_json(data, "player_stats", f"player_stats_{league_key}", season)

# ============================================
# MAIN RUNNER
# ============================================
def run_all(season):
    print(f"\n🚀 Starting full data ingestion for season {season}...\n")
    print("=" * 50)

    fetch_live_scores(season)
    print()

    for league_key, league_info in LEAGUES.items():
        league_id = league_info["id"]
        print(f"\n🏆 League: {league_info['name']}")
        print("-" * 30)
        fetch_standings(league_id, league_key, season)
        fetch_top_scorers(league_id, league_key, season)
        fetch_top_assists(league_id, league_key, season)
        fetch_team_results(league_id, league_key, season)
        fetch_player_stats(league_id, league_key, season)

    print("\n" + "=" * 50)
    print(f"✅ Season {season} data ingested successfully!")
    print(f"📁 Check data/raw/{season}/")

if __name__ == "__main__":
    # Change this number to scrape different seasons
    SEASON = 2023
    run_all(SEASON)