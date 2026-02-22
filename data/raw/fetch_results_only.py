import requests
import json
import os
import time
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

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

def save_json(data, folder, filename, season):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"data/raw/{season}/{folder}/{filename}_{timestamp}.json"
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  ✅ Saved → {path}")

def get(endpoint, params):
    response = requests.get(f"{BASE_URL}/{endpoint}", headers=HEADERS, params=params)
    return response.json()

def fetch_results_for_season(season):
    print(f"\n📅 Fetching results for season {season}...")
    for league_key, league_info in LEAGUES.items():
        print(f"  📋 {league_info['name']}...")
        data = get("fixtures", {
            "league": league_info["id"],
            "season": season,
            "from": f"{season}-08-01",
            "to": f"{season+1}-06-01",
        })
        # Check for errors
        if data.get("errors"):
            print(f"  ⚠️ Error: {data['errors']}")
        else:
            save_json(data, "team_results", f"results_{league_key}", season)
        
        # Wait 7 seconds between requests to stay under 10/minute
        print(f"  ⏳ Waiting 7 seconds...")
        time.sleep(7)

if __name__ == "__main__":
    # Delete old empty files first
    import glob
    for f in glob.glob("data/raw/2023/team_results/*.json"):
        os.remove(f)
    for f in glob.glob("data/raw/2024/team_results/*.json"):
        os.remove(f)
    
    fetch_results_for_season(2023)
    fetch_results_for_season(2024)
    print("\n✅ Done! Team results fetched for 2023 and 2024")