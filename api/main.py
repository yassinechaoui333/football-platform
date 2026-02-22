from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Optional
import os

app = FastAPI(
    title="Football Analytics API",
    description="REST API for football data across top European leagues",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# DATABASE CONNECTION
# ============================================
def get_db():
    return psycopg2.connect(
        host="postgres",
        database="football_db",
        user="football_user",
        password="football_pass",
        cursor_factory=RealDictCursor
    )

# ============================================
# ENDPOINTS
# ============================================

@app.get("/")
def root():
    return {
        "message": "Football Analytics API",
        "version": "1.0.0",
        "endpoints": {
            "standings": "/standings",
            "top_scorers": "/top-scorers",
            "player_rankings": "/player-rankings",
            "league_summary": "/league-summary",
            "team_performance": "/team-performance"
        }
    }

@app.get("/standings")
def get_standings(league: Optional[str] = None, season: Optional[int] = None):
    """Get league standings"""
    conn = get_db()
    cur = conn.cursor()
    
    query = "SELECT * FROM silver.silver_standings WHERE 1=1"
    params = []
    
    if league:
        query += " AND league_name = %s"
        params.append(league)
    if season:
        query += " AND season = %s"
        params.append(season)
    
    query += " ORDER BY season DESC, league_name, rank"
    
    cur.execute(query, params)
    results = cur.fetchall()
    conn.close()
    
    return {"data": results, "count": len(results)}

@app.get("/top-scorers")
def get_top_scorers(
    league: Optional[str] = None, 
    season: Optional[int] = None,
    limit: int = 20
):
    """Get top scorers"""
    conn = get_db()
    cur = conn.cursor()
    
    query = """
        SELECT player_name, team_name, league_name, season, 
               goals, assists, goal_contributions
        FROM silver.silver_top_scorers 
        WHERE 1=1
    """
    params = []
    
    if league:
        query += " AND league_name = %s"
        params.append(league)
    if season:
        query += " AND season = %s"
        params.append(season)
    
    query += " ORDER BY goal_contributions DESC LIMIT %s"
    params.append(limit)
    
    cur.execute(query, params)
    results = cur.fetchall()
    conn.close()
    
    return {"data": results, "count": len(results)}

@app.get("/player-rankings")
def get_player_rankings(
    league: Optional[str] = None,
    season: Optional[int] = None,
    position: Optional[str] = None,
    limit: int = 50
):
    """Get player rankings from Gold layer"""
    conn = get_db()
    cur = conn.cursor()
    
    query = """
        SELECT player_name, team_name, league_name, season, position,
               goals, assists, goal_contributions, appearances,
               goals_per_game, assists_per_game, position_rank
        FROM gold.gold_player_rankings
        WHERE 1=1
    """
    params = []
    
    if league:
        query += " AND league_name = %s"
        params.append(league)
    if season:
        query += " AND season = %s"
        params.append(season)
    if position:
        query += " AND position = %s"
        params.append(position)
    
    query += " ORDER BY goal_contributions DESC LIMIT %s"
    params.append(limit)
    
    cur.execute(query, params)
    results = cur.fetchall()
    conn.close()
    
    return {"data": results, "count": len(results)}

@app.get("/league-summary")
def get_league_summary(season: Optional[int] = None):
    """Get league-level summaries"""
    conn = get_db()
    cur = conn.cursor()
    
    query = "SELECT * FROM gold.gold_league_summary WHERE 1=1"
    params = []
    
    if season:
        query += " AND season = %s"
        params.append(season)
    
    query += " ORDER BY season DESC, league_name"
    
    cur.execute(query, params)
    results = cur.fetchall()
    conn.close()
    
    return {"data": results, "count": len(results)}

@app.get("/team-performance/{team_name}")
def get_team_performance(team_name: str, season: Optional[int] = None):
    """Get performance stats for a specific team"""
    conn = get_db()
    cur = conn.cursor()
    
    query = """
        SELECT * FROM gold.gold_team_performance 
        WHERE team_name ILIKE %s
    """
    params = [f"%{team_name}%"]
    
    if season:
        query += " AND season = %s"
        params.append(season)
    
    query += " ORDER BY season DESC"
    
    cur.execute(query, params)
    results = cur.fetchall()
    conn.close()
    
    if not results:
        raise HTTPException(status_code=404, detail=f"Team '{team_name}' not found")
    
    return {"data": results, "count": len(results)}

@app.get("/health")
def health_check():
    """Check API and database health"""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM gold.gold_player_rankings")
        count = cur.fetchone()['count']
        conn.close()
        return {"status": "healthy", "database": "connected", "player_count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
