from IPython.display import display

"""
This will compare the ddl of the tables which are common to both schema.
1. parameter which will decide which schemas to look into
2. upon receiving parameter it will give a list of commom tables.
3. with that common tables it will compare the DDL and return if those are matching 


The DDL for not common tables will not be compared as the particular table needs to be migrated across the schemas
"""
from connection_mod import mysql_qa1_connection,mysql_qa2_connection,snowflake_connection
from table_list_comparison import common_tables,sf_common_tables,sf_db,sf_schema1,sf_schema2
import pandas as pd

def compare_snowflake_schemas():
    unmatched_sf_table = []

    # List of audit columns that must always be present in the target table
    AUDIT_COLUMNS = [
        ('ETL_RECORD_PROCESS_TIME', 'TIMESTAMP_LTZ(9)', 'N'),
        ('ETL_RECORD_CAPTURE_TIME', 'TIMESTAMP_LTZ(9)', 'N'),
        ('ETL_RECORD_STATUS_CD', 'VARCHAR(10)', 'N')
    ]

    # Dynamically fetch tables from both schemas
    cursor = snowflake_connection.cursor()
    cursor.execute(f"SHOW TABLES IN SCHEMA {sf_db}.{sf_schema1}")
    tables_schema1 = {row[1].upper() for row in cursor.fetchall()}

    cursor.execute(f"SHOW TABLES IN SCHEMA {sf_db}.{sf_schema2}")
    tables_schema2 = {row[1].upper() for row in cursor.fetchall()}

    # Find tables that are missing or extra
    missing_in_schema2 = tables_schema1 - tables_schema2
    extra_in_schema2 = tables_schema2 - tables_schema1
    common_tables = tables_schema1.intersection(tables_schema2)

    print(f"Tables missing in {sf_schema2}: {missing_in_schema2}")
    print(f"Tables extra in {sf_schema2}: {extra_in_schema2}")
    print(f"Common tables in {sf_schema1} and {sf_schema2}: {common_tables}")

    # Compare structures for common tables
    for table in common_tables:
        cursor = snowflake_connection.cursor()
        cursor.execute(f"DESCRIBE TABLE {sf_db}.{sf_schema1}.{table}")
        result = cursor.fetchall()
        sf_structure = [(row[0].upper(), row[1].upper(), row[5].upper()) for row in result]
        sf_structure_df_1 = pd.DataFrame(data=sf_structure, columns=['COLUMN NAME', 'DATATYPE', 'PKEY'])

        cursor.execute(f"DESCRIBE TABLE {sf_db}.{sf_schema2}.{table}")
        result = cursor.fetchall()
        sf_structure = [(row[0].upper(), row[1].upper(), row[5].upper()) for row in result]
        sf_structure_df_2 = pd.DataFrame(data=sf_structure, columns=['COLUMN NAME', 'DATATYPE', 'PKEY'])

        # Check if all source columns exist in the target table
        source_columns = set(sf_structure_df_1['COLUMN NAME'])
        target_columns = set(sf_structure_df_2['COLUMN NAME'])
        missing_columns = source_columns - target_columns

        # Check if all audit columns exist in the target table
        audit_columns_missing = [
            col for col in AUDIT_COLUMNS if col[0] not in target_columns
        ]

        if missing_columns or audit_columns_missing:
            print(f"❌ Structures did not match for: {table}")
            if missing_columns:
                print(f"   - Missing columns in target: {missing_columns}")
            if audit_columns_missing:
                print(f"   - Missing audit columns in target: {[col[0] for col in audit_columns_missing]}")
            unmatched_sf_table.append(table)
        else:
            print(f"✅ Structures matched for: {table}")

    print(f"Snowflake tables to be synced: {unmatched_sf_table}")
    return unmatched_sf_table, missing_in_schema2, extra_in_schema2

if __name__ == "__main__":
    # Input as requirement
    env = input("Enter database environment (mysql/snowflake/mysql_snowflake): ").strip().lower()

    if env == "snowflake" or env == "sf":
        compare_snowflake_schemas()
    elif env == "mysql_snowflake":
        print("Feature for MySQL to Snowflake comparison is yet to develop.")
    else:
        print("Choose a correct option.")