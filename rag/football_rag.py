import os
from dotenv import load_dotenv
import psycopg2
from langchain_community.embeddings import OllamaEmbeddings
from langchain_community.llms import Ollama
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import PGVector

load_dotenv()

# ============================================
# CONFIG
# ============================================
DB_CONNECTION = "postgresql://football_user:football_pass@localhost:5432/football_db"
COLLECTION_NAME = "football_knowledge"

# 👇 Updated to use your actual Windows host IP
embeddings = OllamaEmbeddings(model="llama3.2", base_url="http://172.24.64.1:11434")
llm = Ollama(model="llama3.2", base_url="http://172.24.64.1:11434")

# ============================================
# 1. EXTRACT DATA FROM DATABASE
# ============================================
def extract_football_data():
    """Extract key football facts from Gold layer"""
    print("📊 Extracting football data...")
    
    conn = psycopg2.connect(
        host="localhost",
        database="football_db",
        user="football_user",
        password="football_pass"
    )
    
    documents = []
    
    # Player rankings
    cur = conn.cursor()
    cur.execute("""
        SELECT player_name, team_name, league_name, season, 
               goals, assists, goal_contributions, appearances,
               goals_per_game, position
        FROM gold.gold_player_rankings
        WHERE appearances >= 10
        ORDER BY goal_contributions DESC
        LIMIT 200
    """)
    
    for row in cur.fetchall():
        text = f"""
Player: {row[0]}
Team: {row[1]}
League: {row[2]}
Season: {row[3]}
Goals: {row[4]}
Assists: {row[5]}
Total Contributions: {row[6]}
Appearances: {row[7]}
Goals per Game: {row[8]}
Position: {row[9]}
"""
        documents.append(Document(page_content=text, metadata={
            "type": "player_stats",
            "player": row[0],
            "season": row[3],
            "league": row[2]
        }))
    
    # League summaries
    cur.execute("""
        SELECT league_name, season, total_teams, total_goals, 
               avg_goals_per_team, highest_points, lowest_points
        FROM gold.gold_league_summary
    """)
    
    for row in cur.fetchall():
        text = f"""
League: {row[0]}
Season: {row[1]}
Total Teams: {row[2]}
Total Goals Scored: {row[3]}
Average Goals per Team: {row[4]}
Highest Points: {row[5]}
Lowest Points: {row[6]}
"""
        documents.append(Document(page_content=text, metadata={
            "type": "league_summary",
            "league": row[0],
            "season": row[1]
        }))
    
    # Team performance
    cur.execute("""
        SELECT team_name, league_name, season, total_matches,
               home_wins, draws, home_losses, goals_scored, goals_conceded
        FROM gold.gold_team_performance
        WHERE total_matches >= 10
        LIMIT 200
    """)
    
    for row in cur.fetchall():
        text = f"""
Team: {row[0]}
League: {row[1]}
Season: {row[2]}
Matches Played: {row[3]}
Home Wins: {row[4]}
Draws: {row[5]}
Home Losses: {row[6]}
Goals Scored: {row[7]}
Goals Conceded: {row[8]}
"""
        documents.append(Document(page_content=text, metadata={
            "type": "team_performance",
            "team": row[0],
            "league": row[1],
            "season": row[2]
        }))
    
    conn.close()
    print(f"✅ Extracted {len(documents)} documents")
    return documents

# ============================================
# 2. CREATE VECTOR STORE
# ============================================
def create_vector_store(documents):
    """Create pgvector store with embeddings"""
    print("🔄 Creating vector store...")
    
    # Split documents if needed
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=500,
        chunk_overlap=50
    )
    splits = text_splitter.split_documents(documents)
    
    # Create vector store
    vectorstore = PGVector.from_documents(
        documents=splits,
        embedding=embeddings,
        collection_name=COLLECTION_NAME,
        connection_string=DB_CONNECTION,
        pre_delete_collection=True  # Clear old data
    )
    
    print(f"✅ Vector store created with {len(splits)} chunks")
    return vectorstore

# ============================================
# 3. QUERY THE RAG SYSTEM
# ============================================
def query_rag(question: str, vectorstore):
    """Ask a question and get an answer from the RAG system"""
    
    # Retrieve relevant documents
    retriever = vectorstore.as_retriever(search_kwargs={"k": 5})
    docs = retriever.invoke(question)  # 👈 FIX: use invoke() instead of get_relevant_documents()
    
    # Build context
    context = "\n\n".join([doc.page_content for doc in docs])
    
    # Create prompt
    prompt = f"""You are a football analytics expert. Answer the question based ONLY on the provided context.

Context:
{context}

Question: {question}

Answer:"""
    
    # Get response
    response = llm.invoke(prompt)
    return response, docs

# ============================================
# MAIN
# ============================================
if __name__ == "__main__":
    print("\n🚀 Building Football RAG System...\n")
    
    # Extract and index data
    documents = extract_football_data()
    vectorstore = create_vector_store(documents)
    
    print("\n✅ RAG System Ready!\n")
    print("=" * 60)
    
    # Example queries
    questions = [
        "Who is the top scorer in the Premier League 2024?",
        "How many goals did Mohamed Salah score in 2024?",
        "Which league had the most total goals in 2024?",
    ]
    
    for q in questions:
        print(f"\nQ: {q}")
        answer, docs = query_rag(q, vectorstore)
        print(f"A: {answer}")
        print("-" * 60)
