import soccerdata as sd
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# ============================================
# LEAGUES MAPPING FOR FBREF
# ============================================
LEAGUES = {
    "ENG-Premier League": "Premier League",
    "ESP-La Liga":        "La Liga",
    "GER-Bundesliga":     "Bundesliga",
    "ITA-Serie A":        "Serie A",
    "FRA-Ligue 1":        "Ligue 1",
}

SEASONS = [2023, 2024]

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
# FETCH & LOAD
# ============================================
def fetch_and_load_player_stats():
    conn = get_connection()
    
    # Clear old empty data
    with conn.cursor() as cur:
        cur.execute("TRUNCATE bronze_player_stats;")
        conn.commit()
    print("✅ Cleared old player stats")

    for league_fbref, league_name in LEAGUES.items():
        for season in SEASONS:
            print(f"\n📊 Fetching {league_name} {season}...")
            try:
                fbref = sd.FBref(leagues=league_fbref, seasons=season)
                stats = fbref.read_player_season_stats(stat_type="standard")
                stats = stats.reset_index()

                print(f"  Found {len(stats)} players")

                with conn.cursor() as cur:
                    for _, row in stats.iterrows():
                        cur.execute("""
                            INSERT INTO bronze_player_stats
                            (season, league_name, player_name, team_name, position, 
                             appearances, goals, assists, minutes_played)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (
                            season,
                            league_name,
                            str(row.get("player", "Unknown")),
                            str(row.get("team", "Unknown")),
                            str(row.get("pos", "Unknown")),
                            int(row.get(("Playing Time", "MP"), 0) or 0),
                            int(row.get(("Performance", "Gls"), 0) or 0),
                            int(row.get(("Performance", "Ast"), 0) or 0),
                            int(row.get(("Playing Time", "Min"), 0) or 0),
                        ))
                conn.commit()
                print(f"  ✅ Loaded {len(stats)} players into bronze_player_stats")

            except Exception as e:
                print(f"  ⚠️ Error: {e}")
                continue

    conn.close()
    print("\n✅ All player stats loaded from FBref!")

if __name__ == "__main__":
    fetch_and_load_player_stats()
