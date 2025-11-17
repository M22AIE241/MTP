import pandas as pd
import os
import sys

# Add parent directory to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from connection_mod import snowflake_connection

# ---------------- Snowflake Config ----------------
sf_db='sf_landing_db'
sf_schema="qa2_test"

OUTPUT_DIR = "./qa2_csv_exports"   # directory to save CSVs
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------------- Fetch ALL table names dynamically ----------------
def get_all_tables():
    cur = snowflake_connection.cursor()
    try:
        cur.execute(f"SHOW TABLES IN {sf_db}.{sf_schema}")
        rows = cur.fetchall()
        table_names = [row[1] for row in rows]  # column 1 = table name
        return table_names
    finally:
        cur.close()
        # snowflake_connection.close()

# ---------------- Fetch table data ----------------
def fetch_table(table_name: str) -> pd.DataFrame:
    cur = snowflake_connection.cursor()
    try:
        cur.execute(f"SELECT * FROM {sf_db}.{sf_schema}.{table_name}")
        df = pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])
        return df
    finally:
        cur.close()
        # snowflake_connection.close()

# ---------------- Export each table to CSV ----------------
def export_all_tables():
    table_list = get_all_tables()
    print(f"Found tables: {table_list}")

    for table in table_list:
        df = fetch_table(table)
        output_path = f"{OUTPUT_DIR}/{table.lower()}.csv"
        df.to_csv(output_path, index=False)
        print(f"Saved: {output_path}")

# ---------------- Run ----------------
if __name__ == "__main__":
    export_all_tables()
