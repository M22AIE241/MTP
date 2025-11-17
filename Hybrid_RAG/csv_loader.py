import duckdb
import os

# Directory containing all CSVs
CSV_DIR = "qa2_csv_exports"

# Connect to in-memory DuckDB
con = duckdb.connect(database=':memory:')

# List all CSV files
csv_files = [f for f in os.listdir(CSV_DIR) if f.lower().endswith(".csv")]

print("Found CSV files:", csv_files)

# Dynamically load each CSV
for file_name in csv_files:
    table_name = os.path.splitext(file_name)[0].lower()   # customer.csv -> customer
    
    file_path = os.path.join(CSV_DIR, file_name)
    abs_path = os.path.abspath(file_path)

    print(f"Loading {file_name} → table: {table_name}")

    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS 
        SELECT * FROM read_csv_auto('{abs_path}');
    """)

# Test: show tables
print("\nTables in DuckDB:")
print(con.execute("SHOW TABLES").fetchdf())

# Example: query
print("\nSample from first table:")
print(con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf())
