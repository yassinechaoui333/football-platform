import asyncio
import aiohttp
import psycopg2
import json
import os
from understat import Understat

LEAGUES = {
    "EPL":          "Premier League",
    "La_liga":      "La Liga",
    "Bundesliga":   "Bundesliga",
    "Serie_A":      "Serie A",
    "Ligue_1":      "Ligue 1",
}

SEASONS = [2023, 2024]

def get_connection():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        database=os.environ.get("DB_NAME", "football_db"),
        user=os.environ.get("DB_USER", "football_user"),
        password=os.environ.get("DB_PASSWORD", "football_pass")
    )

async def fetch_and_load():
    conn = get_connection()

    # Clear old empty data
    with conn.cursor() as cur:
        cur.execute("TRUNCATE bronze_player_stats;")
        conn.commit()
    print("✅ Cleared old player stats")

    async with aiohttp.ClientSession() as session:
        understat = Understat(session)

        for league_key, league_name in LEAGUES.items():
            for season in SEASONS:
                print(f"\n📊 Fetching {league_name} {season}...")
                try:
                    players = await understat.get_league_players(
                        league_key, season
                    )

                    with conn.cursor() as cur:
                        for player in players:
                            cur.execute("""
                                INSERT INTO bronze_player_stats
                                (season, league_name, player_name, team_name,
                                 position, appearances, goals, assists, minutes_played)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """, (
                                season,
                                league_name,
                                player.get("player_name", "Unknown"),
                                player.get("team_title", "Unknown"),
                                "Unknown",
                                int(player.get("games", 0) or 0),
                                int(player.get("goals", 0) or 0),
                                int(player.get("assists", 0) or 0),
                                int(player.get("time", 0) or 0),
                            ))
                    conn.commit()
                    print(f"  ✅ Loaded {len(players)} players for {league_name} {season}")

                except Exception as e:
                    print(f"  ⚠️ Error fetching {league_name} {season}: {e}")
                    continue

    conn.close()
    print("\n✅ All player stats loaded from Understat!")

if __name__ == "__main__":
    asyncio.run(fetch_and_load())
