import os
import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer
from doc_builder import make_docs_from_csvs

# ---------------------------------------------------
# FIXED PATH: Always resolves relative to this file
# ---------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(BASE_DIR, "../qa2_csv_exports")

print("Base dir:", BASE_DIR)
print("CSV dir:", CSV_DIR)

# ---------------------------------------------------
# Embedding wrapper
# ---------------------------------------------------
class STEmbeddingWrapper:
    def __init__(self, model_name="all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)

    def embed_documents(self, texts):
        return self.model.encode(texts, show_progress_bar=True).tolist()

    def embed_query(self, text):
        return self.model.encode([text], show_progress_bar=False)[0].tolist()

# ---------------------------------------------------
# Load docs
# ---------------------------------------------------
all_docs = make_docs_from_csvs(CSV_DIR)
print("Total docs:", len(all_docs))

# ---------------------------------------------------
# Initialize Chroma
# ---------------------------------------------------
client = chromadb.PersistentClient(path=os.path.join(BASE_DIR, "../vectordb"))

collection = client.get_or_create_collection(
    name="qa_tables",
    metadata={"hnsw:space": "cosine"}
)

embedding_model = STEmbeddingWrapper()

# ---------------------------------------------------
# Insert into Chroma
# ---------------------------------------------------
if len(all_docs) == 0:
    print("❌ No documents found. Check CSV_DIR path:", CSV_DIR)
    exit()

ids = [f"doc_{i}" for i in range(len(all_docs))]
embeddings = embedding_model.embed_documents(all_docs)

collection.add(
    ids=ids,
    documents=all_docs,
    embeddings=embeddings
)

print("Chroma DB indexing complete!")
print("Total vectors stored:", len(collection.get()["ids"]))


# TEST RETRIEVAL

query = "show me all orders of maximum unit price"
results = collection.query(
    query_embeddings=[embedding_model.embed_query(query)],
    n_results=3
)

for doc in results["documents"][0]:
    print("\nResult:", doc)
