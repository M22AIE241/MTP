from sentence_transformers import SentenceTransformer
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from get_embeddings import all_docs

class STEmbeddingWrapper:
    def __init__(self, model_name="all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)

    def embed_documents(self, texts):
        return self.model.encode(texts, show_progress_bar=True).tolist()

    def embed_query(self, text):
        return self.model.encode([text], show_progress_bar=True)[0].tolist()

import chromadb
from chromadb.config import Settings

# 1. Initialize embedding wrapper
embedding_model = STEmbeddingWrapper("all-MiniLM-L6-v2")

# 2. Initialize persistent ChromaDB
client = chromadb.PersistentClient(path="./vectordb")  
collection = client.get_or_create_collection(
    name="qa_tables",
    metadata={"hnsw:space": "cosine"}
)

# 3. Generate IDs
ids = [f"doc_{i}" for i in range(len(all_docs))]

# 4. Embed and insert into Chroma
collection.add(
    ids=ids,
    documents=all_docs,
    embeddings=embedding_model.embed_documents(all_docs)
)

print("Chroma DB indexing complete!")
print("Total vectors stored:", len(collection.get()['ids']))


#Retrieve test
query = "show me maximum unit priced order"
results = collection.query(
    query_embeddings=[embedding_model.embed_query(query)],
    n_results=3
)

for doc in results["documents"][0]:
    print("\nResult:", doc)

