This project implements a Hybrid Retrieval-Augmented Generation (RAG) system capable of answering natural-language questions about structured CSV datasets.

It combines:

ChromaDB → Semantic vector retrieval

DuckDB → Local SQL execution

Ollama (Gemma 3B) → Text-to-SQL + Natural language reasoning

Hybrid decision logic → Uses SQL when needed, RAG when enough context exists

Descriptive answers → Merges SQL results + retrieved context

Ask anything, such as:

“What are the different kinds of products we have?”

“Which Sony products exist and what is their price?”

“What is the highest unit price?”

“Tell me about customers from Delhi.”

Hybrid_RAG/
│
├── csv_loader.py
├── csv_maker.py
├── doc_builder.py
├── get_embeddings.py
├── index_to_chroma.py
├── vectorize.py
├── hybrid_agent.py        <-- MAIN Hybrid RAG + SQL Agent
│
├── qa2_csv_exports/       <-- CSV input data
│   ├── customer.csv
│   ├── order_items.csv
│   ├── orders.csv
│   └── product.csv
│
├── vectordb/              <-- Persistent ChromaDB
└── README.md
