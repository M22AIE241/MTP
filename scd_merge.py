from connection_mod import snowflake_connection
import pandas as pd

# Function to dynamically get the list of tables in a schema
def get_table_list(connection, schema_name, database_name):
    cursor = connection.cursor()
    query = f"""
        SHOW TABLES IN SCHEMA {database_name}.{schema_name};
    """
    cursor.execute(query)
    tables = cursor.fetchall()
    table_names = [row[1].upper() for row in tables]
    cursor.close()
    return table_names

# Function to dynamically get column names for a table
def get_column_names(connection, table_name, schema_name, database_name):
    cursor = connection.cursor()
    query = f"""
        SELECT COLUMN_NAME 
        FROM {database_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name.upper()}' AND TABLE_SCHEMA = '{schema_name.upper()}';
    """
    cursor.execute(query)
    columns = cursor.fetchall()
    column_names = [row[0].upper() for row in columns]
    cursor.close()
    return column_names

# Function to dynamically get the primary key of a table
def get_primary_key(connection, table_name, schema_name, database_name):
    cursor = connection.cursor()
    query = f"DESCRIBE TABLE {database_name}.{schema_name}.{table_name}"
    cursor.execute(query)
    result = cursor.fetchall()
    primary_key = [row[0].upper() for row in result if row[5] == 'Y']
    cursor.close()
    return primary_key

# Function to check if a stream exists for a table
def check_stream_exists(table_name, source_schema, source_db):
    cursor = snowflake_connection.cursor()
    stream_name = f"{table_name}_STREAM_TYPE1"
    query = f"""
        SHOW STREAMS IN SCHEMA {source_db}.{source_schema};
    """
    cursor.execute(query)
    streams = cursor.fetchall()
    stream_names = [row[1].upper() for row in streams]
    cursor.close()
    if stream_name.upper() in stream_names:
        print(f"Stream exists: {stream_name}")
        return True
    else:
        print(f"Stream does not exist: {stream_name}")
        return False

# Function to generate the SCD1 MERGE SQL dynamically
def generate_scd1_merge_sql(table_name, source_schema, target_schema, source_db, target_db):
    # Get column names dynamically
    source_columns = get_column_names(snowflake_connection, table_name, source_schema, source_db)
    target_columns = get_column_names(snowflake_connection, table_name, target_schema, target_db)
    primary_key = get_primary_key(snowflake_connection, table_name, source_schema, source_db)

    if not primary_key:
        raise ValueError(f"No primary key found for table {table_name}")

    # Remove ETL columns from the comparison
    etl_columns = ['ETL_RECORD_PROCESS_TIME', 'ETL_RECORD_CAPTURE_TIME', 'ETL_RECORD_STATUS_CD']
    source_columns = [col for col in source_columns if col not in etl_columns]
    target_columns = [col for col in target_columns if col not in etl_columns]

    # Prepare the dynamic column strings for the MERGE query
    update_set = ", ".join([f"tgt.{col} = src.{col}" for col in source_columns])
    insert_columns = ", ".join(source_columns + ['ETL_RECORD_PROCESS_TIME', 'ETL_RECORD_CAPTURE_TIME', 'ETL_RECORD_STATUS_CD'])
    insert_values = ", ".join([f"src.{col}" for col in source_columns] + ['CURRENT_TIMESTAMP()', 'CURRENT_TIMESTAMP()', "'A'"])

    # Generate the MERGE SQL
    merge_sql = f"""
    MERGE INTO {target_db}.{target_schema}.{table_name} tgt
    USING {source_db}.{source_schema}.{table_name}_STREAM_TYPE1 src
    ON { " AND ".join([f"tgt.{pk} = src.{pk}" for pk in primary_key]) }

    WHEN MATCHED AND src.metadata$action = 'DELETE' AND src.METADATA$ISUPDATE = 'FALSE' THEN
        DELETE

    WHEN MATCHED AND src.metadata$action = 'INSERT' THEN
        UPDATE SET
            {update_set},
            tgt.ETL_RECORD_PROCESS_TIME = CURRENT_TIMESTAMP(),
            tgt.ETL_RECORD_CAPTURE_TIME = CURRENT_TIMESTAMP(),
            tgt.ETL_RECORD_STATUS_CD =
            CASE
                WHEN src.ETL_RECORD_DELETED = 1 THEN 'D'
                WHEN src.ETL_RECORD_DELETED = 0 THEN 'A'
                ELSE tgt.ETL_RECORD_STATUS_CD
            END

    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values});
    """
    return merge_sql

# Source and target schema details
source_db = "SF_LANDING_DB"
target_db = "SF_LANDING_DB"
source_schema = "qa1_test"
target_schema = "qa2_test"

# Get the list of tables dynamically from the source schema
tables = get_table_list(snowflake_connection, source_schema, source_db)
print(f"Tables in source schema ({source_schema}): {tables}")

# Loop through each table and execute the SCD1 MERGE
for table_name in tables:
    try:
        # Check if the stream exists
        if not check_stream_exists(table_name, source_schema, source_db):
            print(f"Skipping table {table_name} as the stream does not exist.")
            continue

        # Generate the MERGE SQL
        merge_sql = generate_scd1_merge_sql(table_name, source_schema, target_schema, source_db, target_db)
        print(f"Generated MERGE SQL for {table_name}:\n{merge_sql}")

        # Execute the MERGE SQL
        cursor = snowflake_connection.cursor()
        cursor.execute(merge_sql)
        print(f"SCD1 Merge executed successfully for table: {table_name}")

        # Fetch and display the result of the merge
        result = cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))").fetchall()
        if result:
            result_df = pd.DataFrame(result, columns=["ROWS_INSERTED", "ROWS_UPDATED", "ROWS_DELETED"])
            print(f"Merge result for {table_name}:")
            print(result_df)
        else:
            print(f"No changes detected for table: {table_name}")

        cursor.close()
    except Exception as e:
        print(f"Error processing table {table_name}: {e}")