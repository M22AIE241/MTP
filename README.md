This project implements a Hybrid Retrieval-Augmented Generation (RAG) system capable of answering natural-language questions about structured CSV datasets while supporting schema evolution and dynamic data change tracking.

The system intelligently switches between semantic retrieval and SQL execution, ensuring both analytical accuracy and contextual reasoning.

## Core Components
1. ChromaDB — Semantic Vector Retrieval

Stores vector embeddings of table rows, column descriptions, and metadata for natural-language semantic search.

2. DuckDB — Local SQL Execution

Loads CSVs as in-memory relational tables and dynamically adapts to:

New CSVs

Modified CSV structures

Added or removed columns

Changing datatypes

3. Ollama (Gemma 3B)

Provides:

Text-to-SQL generation

Context reasoning

Hybrid summarization across SQL + semantic data

4. Hybrid Decision Logic

Automatically decides:

SQL Mode → for aggregations, filters, and numeric analysis

## Schema Evolution & Change Tracking
This system supports continuous ingestion and updates of CSV-based datasets:

✔ Dynamic Schema Detection

Automatically detects new or altered columns

Rebuilds DuckDB tables if structure changes

Updates ChromaDB metadata with new schema context

✔ Incremental Data Tracking

Detects new rows added to CSVs

Identifies modified values

Updates vector embeddings only for changed records

✔ Metadata Versioning

Maintains a lightweight history of:

Column changes

Table additions/removals

Datatype modifications

✔ Automatic Re-indexing

Upon schema or data changes:

Only affected sections of the vector DB are re-embedded

Old embeddings are marked stale

Retrieval remains consistent and accurate

This makes the system resilient to real-world operational datasets that change frequently.

## Example Questions You Can Ask
“What are the different kinds of products we have?”

“Which Sony products exist and what is their price?”

“What is the highest unit price?”

“Tell me about customers from Delhi.”

“What new fields were added to the product table last week?”

“Have customer details changed recently?”

## Project Structure
Hybrid_RAG/
│
├── csv_loader.py          # Loads CSVs into DuckDB with schema detection
├── csv_maker.py           # CSV generation utility
├── doc_builder.py         # Builds column + table documentation
├── get_embeddings.py      # Embedding helper (SentenceTransformer)
├── index_to_chroma.py     # Indexes tables into ChromaDB w/ schema metadata
├── vectorize.py           # Handles vector formatting + incremental updates
├── hybrid_agent.py        # MAIN Hybrid RAG + SQL Agent (LLM pipeline)
│
├── qa2_csv_exports/       # Input CSV data
│   ├── customer.csv
│   ├── order_items.csv
│   ├── orders.csv
│   └── product.csv
│
├── vectordb/              # Persistent ChromaDB
└── README.md              # Documentation


RAG Mode → when textual context is sufficient

Hybrid Mode → merges SQL results + contextual embeddings for richer answers
