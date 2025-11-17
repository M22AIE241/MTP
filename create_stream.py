from connection_mod import snowflake_connection

def create_stream_if_not_exists(source_table, source_schema, source_db, stream_name):
    cursor = snowflake_connection.cursor()
    
    # Check if the source table exists
    table_check_query = f"""
        SELECT COUNT(*) 
        FROM {source_db}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{source_table.upper()}' AND TABLE_SCHEMA = '{source_schema.upper()}';
    """
    cursor.execute(table_check_query)
    table_exists = cursor.fetchone()[0] > 0

    if not table_exists:
        print(f"Source table {source_schema}.{source_table} does not exist.")
        return

    # Check if the stream exists
    stream_check_query = f"""
        SHOW STREAMS IN SCHEMA {source_db}.{source_schema};
    """
    cursor.execute(stream_check_query)
    streams = cursor.fetchall()
    stream_exists = any(stream[1] == stream_name.upper() for stream in streams)

    if stream_exists:
        print(f"Stream {stream_name} already exists on table {source_schema}.{source_table}.")
    else:
        # Create the stream if it does not exist
        create_stream_query = f"""
            CREATE OR REPLACE STREAM {source_db}.{source_schema}.{stream_name}
            ON TABLE {source_db}.{source_schema}.{source_table}
            SHOW_INITIAL_ROWS = TRUE;
        """
        cursor.execute(create_stream_query)
        print(f"Stream {stream_name} created on table {source_schema}.{source_table}.")

    cursor.close()

# Example usage
source_db = "SF_LANDING_DB"
source_schema = "qa1_test"
source_table = "CUSTOMER"
stream_name = f"{source_table}_STREAM_TYPE1"

create_stream_if_not_exists(source_table, source_schema, source_db, stream_name)