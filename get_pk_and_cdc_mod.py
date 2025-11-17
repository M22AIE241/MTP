from connection_mod import snowflake_connection

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
    if not columns:  # Handle empty results
        print(f"No columns found for table {schema_name}.{table_name}")
        return []
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
    if not primary_key:
        raise ValueError(f"No primary key found for table {database_name}.{schema_name}.{table_name}")
    return primary_key

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

# Source and target schemas
source_db = "SF_LANDING_DB"
target_db = "SF_LANDING_DB"
source_schema = "qa1_test"
target_schema = "qa2_test"

# Get the list of tables from the source schema
table_list = get_table_list(snowflake_connection, source_schema, source_db)
print(f"Tables in source schema: {table_list}")

# Iterate over each table and apply CDC logic
for table_name in table_list:
    print(f"\n🔄 Processing table: {table_name}")
    temp_table_name = f"{table_name}_changes"

    # Get column names dynamically
    source_columns = get_column_names(snowflake_connection, table_name, source_schema, source_db)
    if not source_columns:
        print(f"Skipping table {table_name} as no source columns were found.")
        continue
    print(f"Source columns: {source_columns}")

    target_columns = get_column_names(snowflake_connection, table_name, target_schema, target_db)
    if not target_columns:
        print(f"Skipping table {table_name} as no target columns were found.")
        continue
    print(f"Target columns: {target_columns}")

    # Get primary key dynamically
    try:
        primary_key_columns = get_primary_key(snowflake_connection, table_name, source_schema, source_db)
    except ValueError as e:
        print(e)
        continue
    print(f"Primary key columns: {primary_key_columns}")
    primary_key = ", ".join(primary_key_columns)  # For use in SQL statements

    # Ensure both tables have the same columns
    if sorted(source_columns) != sorted(target_columns):
        print("Warning: Column structures differ between source and target!")
        print(f"Source columns (sorted): {sorted(source_columns)}")
        print(f"Target columns (sorted): {sorted(target_columns)}")
    else:
        print("Column structures are identical between source and target.")

    # Generate the temporary table creation SQL
    cdc_table_sql = f"""
    CREATE OR REPLACE TEMP TABLE {target_db}.{target_schema}.{temp_table_name} AS
    SELECT 
        {", ".join([f"src.{col}" for col in source_columns if col != 'ETL_RECORD_DELETED'])},
        tgt.ETL_RECORD_DELETED,
        CASE 
            WHEN tgt.{primary_key_columns[0]} IS NULL THEN 'INSERT'
            WHEN tgt.{primary_key_columns[0]} IS NOT NULL AND ({' OR '.join([f'src.{col} != tgt.{col}' for col in source_columns if col != 'ETL_RECORD_DELETED'])}) THEN 'UPDATE'
            WHEN tgt.{primary_key_columns[0]} IS NOT NULL AND src.{primary_key_columns[0]} IS NULL THEN 'DELETE'
            ELSE 'UNCHANGED'
        END AS change_type
    FROM {source_db}.{source_schema}.{table_name} src
    FULL OUTER JOIN {target_db}.{target_schema}.{table_name} tgt
        ON { " AND ".join([f'src.{col} = tgt.{col}' for col in primary_key_columns])};
    """

    # Create the temporary table
    print(f"Creating temporary table with SQL:\n{cdc_table_sql}")
    cursor = snowflake_connection.cursor()
    cursor.execute(cdc_table_sql)

    # Retrieve columns from the temporary table
    temp_columns = get_column_names(snowflake_connection, temp_table_name, target_schema, target_db)
    print(f"Temporary table columns: {temp_columns}")

    # Filter target columns to include only those present in the temporary table
    filtered_target_columns = [col for col in target_columns if col in temp_columns]
    print(f"Filtered target columns: {filtered_target_columns}")

    # INSERT new rows into the target table
    insert_sql = f"""
    INSERT INTO {target_db}.{target_schema}.{table_name} ({", ".join(filtered_target_columns)})
    SELECT {", ".join(filtered_target_columns)}
    FROM {target_db}.{target_schema}.{temp_table_name}
    WHERE change_type = 'INSERT';
    """
    cursor.execute(insert_sql)
    inserted_rows = cursor.rowcount  # Get the number of rows inserted
    print(f"Number of rows inserted: {inserted_rows}")

    # UPDATE existing rows in the target table
    update_sql = f"""
    MERGE INTO {target_db}.{target_schema}.{table_name} tgt
    USING {target_db}.{target_schema}.{temp_table_name} src
    ON { " AND ".join([f"tgt.{col} = src.{col}" for col in primary_key_columns]) }
    WHEN MATCHED AND src.change_type = 'UPDATE' THEN
    UPDATE SET 
        {", ".join([f"tgt.{col} = src.{col}" for col in source_columns if col != 'ETL_RECORD_DELETED'])},
        tgt.ETL_RECORD_DELETED = src.ETL_RECORD_DELETED;
    """
    cursor.execute(update_sql)
    updated_rows = cursor.rowcount  # Get the number of rows updated
    print(f"Number of rows updated: {updated_rows}")

    # DELETE rows in the target table not present in the source
    delete_sql = f"""
    UPDATE {target_db}.{target_schema}.{table_name}
    SET ETL_RECORD_DELETED = TRUE
    WHERE {primary_key_columns[0]} IN (
        SELECT {primary_key_columns[0]}
        FROM {target_db}.{target_schema}.{temp_table_name}
        WHERE change_type = 'DELETE'
    );
    """
    cursor.execute(delete_sql)
    deleted_rows = cursor.rowcount  # Get the number of rows marked as deleted
    print(f"Number of rows marked as deleted: {deleted_rows}")

    # Close the cursor for this table
    cursor.close()

print("CDC logic executed successfully for all tables.")
snowflake_connection.close()

# from connection_mod import snowflake_connection

# # Function to dynamically get column names for a table
# def get_column_names(connection, table_name, schema_name, database_name):
#     cursor = connection.cursor()
#     query = f"""
#         SELECT COLUMN_NAME 
#         FROM {database_name}.INFORMATION_SCHEMA.COLUMNS
#         WHERE TABLE_NAME = '{table_name.upper()}' AND TABLE_SCHEMA = '{schema_name.upper()}';
#     """
#     cursor.execute(query)
#     columns = cursor.fetchall()
#     if not columns:  # Handle empty results
#         print(f"No columns found for table {schema_name}.{table_name}")
#         return []
#     column_names = [row[0].upper() for row in columns]
#     cursor.close()
#     return column_names

# # Function to dynamically get the primary key of a table
# def get_primary_key(sf_table_name):
#     cursor = snowflake_connection.cursor()
#     query = f"DESCRIBE TABLE SF_LANDING_DB.QA1_TEST.{sf_table_name}"
#     cursor.execute(query)
#     result = cursor.fetchall()
#     primary_key = [row[0].upper() for row in result if row[5] == 'Y']
#     cursor.close()
#     if not primary_key:
#         raise ValueError(f"No primary key found for table SF_LANDING_DB.QA1_TEST.{sf_table_name}")
#     return primary_key

# # Source and target tables
# source_db = "SF_LANDING_DB"
# target_db = "SF_LANDING_DB"
# source_schema = "qa1_test"
# target_schema = "qa2_test"
# table_name = "NATION"
# temp_table_name = f"{table_name}_changes"

# # Get column names dynamically
# source_columns = get_column_names(snowflake_connection, table_name, source_schema, source_db)
# if not source_columns:
#     raise ValueError("Source columns could not be retrieved.")
# print(f"Source columns: {source_columns}")

# target_columns = get_column_names(snowflake_connection, table_name, target_schema, target_db)
# if not target_columns:
#     raise ValueError("Target columns could not be retrieved.")
# print(f"Target columns: {target_columns}")

# # Get primary key dynamically
# primary_key_columns = get_primary_key(table_name)
# print(f"Primary key columns: {primary_key_columns}")
# primary_key = ", ".join(primary_key_columns)  # For use in SQL statements

# # Ensure both tables have the same columns
# if sorted(source_columns) != sorted(target_columns):
#     print("Warning: Column structures differ between source and target!")
#     print(f"Source columns (sorted): {sorted(source_columns)}")
#     print(f"Target columns (sorted): {sorted(target_columns)}")
# else:
#     print("Column structures are identical between source and target.")

# # Generate the temporary table creation SQL
# cdc_table_sql = f"""
# CREATE OR REPLACE TEMP TABLE {target_db}.{target_schema}.{temp_table_name} AS
# SELECT 
#     {", ".join([f"src.{col}" for col in source_columns if col != 'ETL_RECORD_DELETED'])},
#     tgt.ETL_RECORD_DELETED,
#     CASE 
#         WHEN tgt.{primary_key_columns[0]} IS NULL THEN 'INSERT'
#         WHEN tgt.{primary_key_columns[0]} IS NOT NULL AND ({' OR '.join([f'src.{col} != tgt.{col}' for col in source_columns if col != 'ETL_RECORD_DELETED'])}) THEN 'UPDATE'
#         WHEN tgt.{primary_key_columns[0]} IS NOT NULL AND src.{primary_key_columns[0]} IS NULL THEN 'DELETE'
#         ELSE 'UNCHANGED'
#     END AS change_type
# FROM {source_db}.{source_schema}.{table_name} src
# FULL OUTER JOIN {target_db}.{target_schema}.{table_name} tgt
#     ON { " AND ".join([f'src.{col} = tgt.{col}' for col in primary_key_columns])};
# """

# # Create the temporary table
# print(f"Creating temporary table with SQL:\n{cdc_table_sql}")
# cursor = snowflake_connection.cursor()
# cursor.execute(cdc_table_sql)

# # Retrieve columns from the temporary table
# temp_columns = get_column_names(snowflake_connection, temp_table_name, target_schema, target_db)
# print(f"Temporary table columns: {temp_columns}")

# # Filter target columns to include only those present in the temporary table
# filtered_target_columns = [col for col in target_columns if col in temp_columns]
# print(f"Filtered target columns: {filtered_target_columns}")

# # INSERT new rows into the target table
# insert_sql = f"""
# INSERT INTO {target_db}.{target_schema}.{table_name} ({", ".join(filtered_target_columns)})
# SELECT {", ".join(filtered_target_columns)}
# FROM {target_db}.{target_schema}.{temp_table_name}
# WHERE change_type = 'INSERT';
# """
# cursor.execute(insert_sql)
# inserted_rows = cursor.rowcount  # Get the number of rows inserted
# print(f"Number of rows inserted: {inserted_rows}")

# # UPDATE existing rows in the target table
# update_sql = f"""
# MERGE INTO {target_db}.{target_schema}.{table_name} tgt
# USING {target_db}.{target_schema}.{temp_table_name} src
# ON { " AND ".join([f"tgt.{col} = src.{col}" for col in primary_key_columns]) }
# WHEN MATCHED AND src.change_type = 'UPDATE' THEN
# UPDATE SET 
#     {", ".join([f"tgt.{col} = src.{col}" for col in source_columns if col != 'ETL_RECORD_DELETED'])},
#     tgt.ETL_RECORD_DELETED = src.ETL_RECORD_DELETED;
# """
# cursor.execute(update_sql)
# updated_rows = cursor.rowcount  # Get the number of rows updated
# print(f"Number of rows updated: {updated_rows}")

# # DELETE rows in the target table not present in the source
# delete_sql = f"""
# UPDATE {target_db}.{target_schema}.{table_name}
# SET ETL_RECORD_DELETED = TRUE
# WHERE {primary_key_columns[0]} IN (
#     SELECT {primary_key_columns[0]}
#     FROM {target_db}.{target_schema}.{temp_table_name}
#     WHERE change_type = 'DELETE'
# );
# """
# cursor.execute(delete_sql)
# deleted_rows = cursor.rowcount  # Get the number of rows marked as deleted
# print(f"Number of rows marked as deleted: {deleted_rows}")

# # Close the connection
# cursor.close()
# snowflake_connection.close()

# print("CDC logic executed successfully.")