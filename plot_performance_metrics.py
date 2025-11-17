import time
import matplotlib.pyplot as plt
from connection_mod import snowflake_connection

cursor = snowflake_connection.cursor()

# Define sample sizes for 20 data points (in rows)
sample_sizes = [
    50_000, 1_100_000, 2_150_000, 3_200_000, 4_250_000,
    5_300_000, 6_350_000, 7_400_000, 8_450_000, 9_500_000,
    10_550_000, 11_600_000, 12_650_000, 13_700_000, 14_750_000,
    15_800_000, 16_850_000, 17_900_000, 18_950_000, 20_000_000
]

# Source table and schema
source_table = "SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER"
target_db = "SF_LANDING_DB"
target_schema = "QA1_TEST"
sample_table_prefix = f"{target_db}.{target_schema}.CUSTOMER_SAMPLE_"

# Total rows in the source table
source_table_row_count = 100_000_000  # Adjust this value if you know the exact size

# Metrics storage
metrics_with_cache = {}
metrics_without_cache = {}

# Step 1: Create Sample Tables
print("\n--- Creating Sample Tables ---\n")
for i, sample_size in enumerate(sample_sizes, start=1):
    sample_table_name = f"{sample_table_prefix}{sample_size // 1_000_000}M"
    print(f"Creating sample table: {sample_table_name} with ~{sample_size:,} rows")
    
    # Calculate sampling percentage
    sample_percentage = (sample_size / source_table_row_count) * 100
    
    # Generate random sampling SQL
    sample_sql = f"""
    CREATE OR REPLACE TABLE {sample_table_name} AS
    SELECT *
    FROM {source_table}
    SAMPLE ({sample_percentage:.6f});
    """
    cursor.execute(sample_sql)
    print(f"Sample table {sample_table_name} created.")

# Helper function to measure query performance with or without cache
def measure_query_time(use_cache):
    query_times = {}

    # Set cache state explicitly
    if use_cache:
        cursor.execute("ALTER SESSION SET USE_CACHED_RESULT = TRUE")
        print("\nQuery result cache is ENABLED.")
    else:
        cursor.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
        print("\nQuery result cache is DISABLED.")

    # Measure query performance for each sample table
    for i, sample_size in enumerate(sample_sizes, start=1):
        sample_table_name = f"{sample_table_prefix}{sample_size // 1_000_000}M"
        print(f"Querying sample table: {sample_table_name}")
        
        query_sql = f"SELECT * FROM {sample_table_name}"
        
        # Measure query execution time
        start_time = time.time()
        cursor.execute(query_sql)
        end_time = time.time()
        
        # Calculate query time
        query_time = end_time - start_time
        query_times[sample_table_name] = {
            "rows": sample_size,
            "time_taken": query_time
        }
        
        print(f"Query completed for {sample_table_name}: Time taken = {query_time:.2f} seconds.")
    
    return query_times

try:
    # Measure performance with query cache
    print("\n--- Measuring Performance With Cache ---\n")
    metrics_with_cache = measure_query_time(use_cache=True)

    # Measure performance without query cache
    print("\n--- Measuring Performance Without Cache ---\n")
    metrics_without_cache = measure_query_time(use_cache=False)

finally:
    # Close the cursor and connection
    cursor.close()
    snowflake_connection.close()

# Step 4: Compare and plot metrics
data_points = [f"{size // 1_000}K" if size < 1_000_000 else f"{size // 1_000_000}M" for size in sample_sizes]
time_with_cache = [metrics_with_cache[f"{sample_table_prefix}{size // 1_000_000}M"]["time_taken"] for size in sample_sizes]
time_without_cache = [metrics_without_cache[f"{sample_table_prefix}{size // 1_000_000}M"]["time_taken"] for size in sample_sizes]

# Plot the results
plt.figure(figsize=(12, 6))
plt.plot(data_points, time_with_cache, label="With Cache", marker="o", linestyle='-')
plt.plot(data_points, time_without_cache, label="Without Cache", marker="o", linestyle='--')
plt.title("Query Performance: With and Without Cache", fontsize=16)
plt.xlabel("Sample Size", fontsize=12)
plt.ylabel("Query Time (Seconds)", fontsize=12)
plt.legend(fontsize=12)
plt.grid(True)
plt.xticks(rotation=45, fontsize=10)
plt.tight_layout()
plt.show()
