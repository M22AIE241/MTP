from connection_mod import mysql_qa1_connection,mysql_qa2_connection,snowflake_connection
from IPython.display import display


# define the db schemas : 
sf_db='sf_landing_db'
sf_schema1="qa1_test"
sf_schema2="qa2_test"


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


try:

    #mysql checks
    cursor = mysql_qa1_connection.cursor()

    # Query to get table names from qa1_test
    cursor.execute("SHOW TABLES FROM qa1_test")
    tables_qa1 = [table[0] for table in cursor.fetchall()]

    # Query to get table names from qa2_test
    cursor.execute("SHOW TABLES FROM qa2_test")
    tables_qa2 = [table[0] for table in cursor.fetchall()]

    common_tables=[]
    for tables in tables_qa1:
        if tables in tables_qa2:
            common_tables.append(tables)

    unique_tables_qa1=[]
    for tables in tables_qa1:
        if tables not in tables_qa2:
            unique_tables_qa1.append(tables)

    unique_tables_qa2=[]
    for tables in tables_qa2:
        if tables not in tables_qa1:
            unique_tables_qa2.append(tables)
    
    # #Print results
    # print("=====================mysql environment objects validation :==========================")
    # print("Common Tables:", common_tables)
    # print("Tables unique to qa1_test:", unique_tables_qa1)
    # print("Tables unique to qa2_test:", unique_tables_qa2)
    # print("\n"*10)

    #snowflake checks
    # def function_sf_check(db,schema1,schema2):
    cursor_sf=snowflake_connection.cursor()
    cursor_sf.execute(f"show tables in {sf_db}.{sf_schema1}")
    sf_tables_schema1=[table[1] for table in cursor_sf.fetchall()]
    cursor_sf.execute(f"show tables in {sf_db}.{sf_schema2}")
    sf_tables_schema2=[table[1] for table in cursor_sf.fetchall()]

    sf_common_tables=[]
    for tables in sf_tables_schema1:
        if tables in sf_tables_schema2:
            sf_common_tables.append(tables)

    unique_sf_tables_schema1=[]
    for tables in sf_tables_schema1:
        if tables not in sf_tables_schema2:  
            unique_sf_tables_schema1.append(tables)

    unique_sf_tables_schema2=[]
    for tables in sf_tables_schema2:
        if tables not in sf_tables_schema1:  
            unique_sf_tables_schema2.append(tables)

    print("=====================Snowflake environment objects validation :==========================")
    print("Common Tables snowflake:", sf_common_tables)
    print("Tables unique to qa1_test:", unique_sf_tables_schema1)
    print("Tables unique to qa2_test:", unique_sf_tables_schema2)
    print("Table to be replicated from qa1_test to qa2_test:", unique_sf_tables_schema1)


    source_db = "SF_LANDING_DB"
    source_schema = "qa1_test"

    # Get the list of tables
    tables = get_table_list(snowflake_connection, source_schema, source_db)
    # print(f"Tables in schema {source_schema}: {tables}")

    # Get column names and primary key for each table
    for table in tables:
        columns = get_column_names(snowflake_connection, table, source_schema, source_db)
        primary_key = get_primary_key(snowflake_connection, table, source_schema, source_db)
        # print(f"Table: {table}, Columns: {columns}, Primary Key: {primary_key}")


except ValueError as e:
    print(e)
