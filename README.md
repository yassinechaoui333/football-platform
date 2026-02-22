# ⚽ Football Analytics & Intelligence Platform

A production-grade data engineering platform that ingests, transforms, and serves football data from Europe's top 5 leagues + Champions League, with an AI-powered RAG chatbot for natural language queries.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.9-red)
![dbt](https://img.shields.io/badge/dbt-1.11-orange)
![FastAPI](https://img.shields.io/badge/FastAPI-latest-green)

## 🎯 Project Overview

This platform demonstrates a complete end-to-end data engineering workflow with modern best practices:

- **Data Ingestion**: Automated daily extraction from API-Football & Understat
- **Data Transformation**: dbt-powered medallion architecture (Bronze → Silver → Gold)
- **Orchestration**: Airflow DAGs for scheduling and monitoring
- **API Layer**: FastAPI REST endpoints serving analytics data
- **RAG System**: AI chatbot powered by Ollama & pgvector for natural language queries
- **Containerization**: Fully Dockerized with Docker Compose

## 🏗️ Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                       Data Sources                               │
│              API-Football + Understat APIs                       │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Airflow (Orchestration)                       │
│    Daily DAGs: fetch_standings → fetch_scorers → fetch_results  │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│              PostgreSQL (Medallion Architecture)                 │
│                                                                  │
│  Bronze (Raw)  →  Silver (Cleaned)  →  Gold (Aggregated)       │
│                                                                  │
│  - Standings        - Cleaned views      - Player rankings      │
│  - Top Scorers      - Enriched metrics   - League summaries     │
│  - Match Results    - Type casting       - Team performance     │
│  - Player Stats                                                  │
└────────────┬──────────────────────────┬─────────────────────────┘
             │                          │
             ▼                          ▼
┌──────────────────────────┐   ┌──────────────────────────┐
│   FastAPI (REST API)     │   │   RAG System (AI Chat)   │
│                          │   │                          │
│  - /standings            │   │  - pgvector embeddings   │
│  - /top-scorers          │   │  - Ollama LLM            │
│  - /player-rankings      │   │  - Natural language Q&A  │
│  - /team-performance     │   │                          │
└──────────────────────────┘   └──────────────────────────┘
```

## 📊 Data Coverage

- **Leagues**: Premier League, La Liga, Bundesliga, Serie A, Ligue 1, Champions League
- **Seasons**: 2023, 2024
- **Metrics**:
  - 400+ team records
  - 5,500+ player statistics
  - 3,000+ match results
  - League-level aggregations

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Ollama (for RAG chatbot)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/football-platform.git
cd football-platform
```

2. **Set up environment variables**
```bash
cp .env.example .env
# Add your API-Football key to .env
```

3. **Start all services**
```bash
docker compose up -d
```

4. **Access the services**
- Airflow UI: http://localhost:8080 (admin/admin123)
- FastAPI Docs: http://localhost:8000/docs
- pgAdmin: http://localhost:5050 (admin@football.com/admin123)

## 📁 Project Structure
```
football-platform/
├── dags/                      # Airflow DAGs
│   └── football_pipeline.py   # Main orchestration DAG
├── dbt_project/               # dbt transformations
│   ├── models/
│   │   ├── silver/            # Cleaned data models
│   │   └── gold/              # Analytics models
│   └── dbt_project.yml
├── data/raw/                  # Bronze layer storage
│   ├── 2023/                  # Season folders
│   ├── 2024/
│   └── fetch_*.py             # Data extraction scripts
├── api/                       # FastAPI application
│   └── main.py                # REST endpoints
├── rag/                       # RAG chatbot
│   ├── football_rag.py        # RAG pipeline
│   └── chat.py                # Interactive CLI
├── docker-compose.yml         # Multi-container orchestration
├── Dockerfile                 # Airflow image
├── Dockerfile.fastapi         # FastAPI image
└── requirements.txt
```

## 🎯 Key Features

### 1. **Automated Data Pipeline**
- Airflow DAG runs daily at 6 AM
- Fetches latest standings, top scorers, and match results
- Handles rate limits and API retries
- Loads data into Bronze layer

### 2. **dbt Transformations**
- **Silver Layer**: Data quality checks, type casting, deduplication
- **Gold Layer**: 
  - Player rankings with goals/assists per game
  - League-level summaries
  - Team performance metrics
  - Top performers by position

### 3. **REST API**
FastAPI endpoints with automatic Swagger documentation:
```bash
GET /standings?league=Premier%20League&season=2024
GET /top-scorers?limit=20
GET /player-rankings?position=Forward
GET /team-performance/Liverpool
```

### 4. **RAG Chatbot**
Natural language queries powered by Ollama + pgvector:
```python
Q: "Who is the top scorer in the Premier League 2024?"
A: "Mohamed Salah leads with 29 goals and 18 assists..."

Q: "Compare Harry Kane's performance in 2023 vs 2024"
A: "In 2023, Kane scored 36 goals in 44 appearances..."
```

## 🛠️ Technologies Used

| Category | Tools |
|----------|-------|
| **Orchestration** | Apache Airflow 2.9 |
| **Data Transformation** | dbt 1.11, SQL |
| **Database** | PostgreSQL 15, pgvector |
| **API** | FastAPI, Uvicorn |
| **AI/ML** | Ollama (llama3.2), LangChain, pgvector |
| **Containerization** | Docker, Docker Compose |
| **Languages** | Python 3.11 |
| **Data Sources** | API-Football, Understat |

## 📈 Sample Queries

### SQL (Direct Database)
```sql
-- Top 10 goal scorers across all leagues
SELECT player_name, team_name, league_name, goals 
FROM gold.gold_player_rankings 
ORDER BY goals DESC 
LIMIT 10;
```

### REST API
```bash
curl "http://localhost:8000/top-scorers?league=Premier%20League&limit=10"
```

### RAG Chatbot
```bash
python rag/chat.py
> Which team has the best home record in 2024?
```

## 🔄 Running the Pipeline Manually

1. **Trigger Airflow DAG**
   - Go to http://localhost:8080
   - Find `football_pipeline`
   - Click ▶️ to trigger

2. **Run dbt transformations**
```bash
docker exec -it airflow_scheduler bash
cd /opt/airflow/dbt_project
dbt run
```

3. **Test FastAPI**
```bash
curl http://localhost:8000/health
```

## 📝 Development

### Adding New Data Sources
1. Create extraction script in `data/raw/`
2. Add task to `dags/football_pipeline.py`
3. Create dbt model in `dbt_project/models/`

### Adding New API Endpoints
Edit `api/main.py`:
```python
@app.get("/new-endpoint")
def new_endpoint():
    # Your logic
    return {"data": results}
```

## 🐛 Troubleshooting

**Airflow DAG not appearing?**
```bash
docker exec -it airflow_scheduler airflow dags list-import-errors
```

**Database connection issues?**
```bash
docker exec -it football_postgres psql -U football_user -d football_db
```

**RAG chatbot connection refused?**
- Ensure Ollama is running on Windows
- Check IP with: `cat /etc/resolv.conf | grep nameserver`

## 🚧 Future Enhancements

- [ ] Add Slack/email alerts for pipeline failures
- [ ] Implement data quality tests with Great Expectations
- [ ] Build Power BI/Tableau dashboard
- [ ] Add CI/CD with GitHub Actions
- [ ] Deploy to AWS/GCP
- [ ] Add real-time match updates via websockets


## 🙏 Acknowledgments

- **API-Football** for football data
- **Understat** for advanced player statistics
- **Anthropic Claude** for development assistance

---

**Built with  by Yassine Chaoui**

*This project demonstrates end-to-end data engineering skills including data extraction, transformation, orchestration, API development, and AI integration.*
