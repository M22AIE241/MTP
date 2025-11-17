import os
import json
import duckdb
import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer
import requests

# -------------------------------------------------------
# PATH SETTINGS
# -------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(BASE_DIR, "../qa2_csv_exports")
VECTORDB_DIR = os.path.join(BASE_DIR, "../vectordb")

# -------------------------------------------------------
# Load CSVs into DuckDB (SQL engine)
# -------------------------------------------------------
def load_duckdb_tables(csv_dir):
    con = duckdb.connect(database=":memory:")
    csv_files = [f for f in os.listdir(csv_dir) if f.endswith(".csv")]

    for f in csv_files:
        table = os.path.splitext(f)[0]
        path = os.path.join(csv_dir, f)
        con.execute(
            f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_csv_auto('{path}');"
        )

    return con

duck = load_duckdb_tables(CSV_DIR)
print("DuckDB tables loaded:", duck.execute("SHOW TABLES").fetchall())

# -------------------------------------------------------
# Load ChromaDB
# -------------------------------------------------------
client = chromadb.PersistentClient(path=VECTORDB_DIR)
collection = client.get_or_create_collection(
    name="qa_tables",
    metadata={"hnsw:space": "cosine"}
)

# -------------------------------------------------------
# Embedding wrapper
# -------------------------------------------------------
class STEmbeddingWrapper:
    def __init__(self, model_name="all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)

    def embed_query(self, text):
        return self.model.encode([text], show_progress_bar=False)[0].tolist()

embedder = STEmbeddingWrapper()

# -------------------------------------------------------
# OLLAMA CONFIG
# -------------------------------------------------------
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "gemma3:1b"

# -------------------------------------------------------
# Ollama streaming helper (NDJSON)
# -------------------------------------------------------
def call_ollama(prompt):
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": True
    }

    response = requests.post(OLLAMA_URL, json=payload, stream=True)

    full_text = ""

    for line in response.iter_lines():
        if not line:
            continue

        try:
            data = json.loads(line.decode("utf-8"))
            if "response" in data:
                full_text += data["response"]
        except:
            continue

    return full_text.strip()

# -------------------------------------------------------
# SQL generation with context
# -------------------------------------------------------
def generate_sql(question, context):
    prompt = f"""
You are a text-to-SQL AI assistant.
Write only SQL. No explanations, no comments, no backticks.

Tables available in DuckDB:
{duck.execute("SHOW TABLES").fetchall()}

Context that describes tables:
{context}

User question:
{question}

Write a single valid SELECT query:
"""

    sql = call_ollama(prompt)
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql

# -------------------------------------------------------
# SMART HYBRID ANSWERING (SQL or RAG)
# -------------------------------------------------------
def answer_question(question):

    print("\n🔍 QUESTION:", question)

    # ---------------------------------------------------
    # 1. Retrieve context from Chroma
    # ---------------------------------------------------
    results = collection.query(
        query_embeddings=[embedder.embed_query(question)],
        n_results=6  # increased context for better reasoning
    )

    docs = results["documents"][0]
    context = "\n".join(docs)

    # ---------------------------------------------------
    # 2. Decide whether SQL is needed
    # ---------------------------------------------------
    sql_keywords = [
        "count", "sum", "avg", "maximum", "minimum", "total",
        "highest", "lowest", "top", "list", "show", "find",
        "price", "orders", "revenue", "units", "how many",
        "greater", "less", "compare", "filter"
    ]

    should_use_sql = any(k in question.lower() for k in sql_keywords)

    # ---------------------------------------------------
    # 3. PURE RAG (NO SQL REQUIRED)
    # ---------------------------------------------------
    if not should_use_sql:
        rag_prompt = f"""
You are an intelligent business assistant.

You have access to context extracted from database tables.

CONTEXT:
{context}

QUESTION:
{question}

TASK:
- Use ONLY the information from the context.
- Infer, summarize, and combine all relevant details.
- Give a **complete, descriptive, human-friendly answer** (3-5 sentences).
- Avoid SQL or technical wording.
- If a question is ambiguous, clarify using context.
"""

        final_answer = call_ollama(rag_prompt)
        print("\n🗣 FINAL ANSWER:\n")
        return final_answer.strip()

    # ---------------------------------------------------
    # 4. SQL MODE — generate SQL
    # ---------------------------------------------------
    sql = generate_sql(question, context)

    if not sql.lower().startswith("select"):
        # fallback to descriptive context answer
        fallback_rag = f"""
The following question could not generate a valid SQL query:

QUESTION:
{question}

CONTEXT:
{context}

Provide the **best detailed answer** using only context.
"""
        return call_ollama(fallback_rag).strip()

    # ---------------------------------------------------
    # 5. Execute SQL
    # ---------------------------------------------------
    try:
        df = duck.execute(sql).fetchdf()
    except Exception as e:
        fallback_on_error = f"""
SQL execution error: {str(e)}

Use the database context to answer:

QUESTION:
{question}

CONTEXT:
{context}

Provide a **complete descriptive answer** using only context.
"""
        return call_ollama(fallback_on_error).strip()

    print("\n📄 SQL RESULT:\n", df.to_string(index=False))

    # ---------------------------------------------------
    # 6. HYBRID ANSWER: Blend SQL + Context
    # ---------------------------------------------------
    hybrid_prompt = f"""
You are a highly skilled data analyst.

Below is:
1. A user question
2. Retrieved database context
3. SQL generated
4. SQL result

Your job is to combine them into a **single, well-rounded, human-friendly answer**.

QUESTION:
{question}

SQL EXECUTED:
{sql}

SQL RESULT:
{df.to_string(index=False)}

CONTEXT FROM VECTOR DATABASE:
{context}

INSTRUCTIONS:
- Give a **thorough, descriptive answer**.
- Combine insights from both SQL result AND context.
- If context adds details missing from SQL, include them.
- Aim for **4–6 sentences**, insightful and business-friendly.
- Never show SQL or table formatting.
"""

    final_answer = call_ollama(hybrid_prompt)

    print("\n🗣 FINAL ANSWER:\n")
    return final_answer.strip()



# -------------------------------------------------------
# CLI LOOP
# -------------------------------------------------------
if __name__ == "__main__":
    while True:
        q = input("\nHello! What can I help you with today?\n> ")
        if q.lower() == "exit":
            break
        print(answer_question(q))
