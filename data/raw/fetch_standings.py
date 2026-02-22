import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def fetch_standings(league_id=39, season=2024):
    url = "https://v3.football.api-sports.io/standings"
    
    headers = {
        "x-apisports-key": os.getenv("API_KEY")
    }
    
    params = {
        "league": league_id,
        "season": season
    }

    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/raw/standings_{timestamp}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"✅ Data saved to {filename}")
    return data

if __name__ == "__main__":
    data = fetch_standings()
    # Print top 5 teams
    standings = data['response'][0]['league']['standings'][0]
    print("\n🏆 Premier League Top 5:")
    for team in standings[:5]:
        print(f"{team['rank']}. {team['team']['name']} - {team['points']} pts")
