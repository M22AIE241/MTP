import mysql.connector

# MySQL Connection to qa1_test
try:
    mysql_qa1_connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        port=3306,
        database='qa1_test'
    )
    if not mysql_qa1_connection.is_connected():
        raise Exception("Failed to connect to qa1_test database.")
except mysql.connector.Error as err:
    # Handle the error appropriately
    # For example, you can raise an exception or handle it silently
    raise err

# MySQL Connection to qa2_test
try:
    mysql_qa2_connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        port=3306,
        database='qa2_test'
    )
    if not mysql_qa2_connection.is_connected():
        raise Exception("Failed to connect to qa2_test database.")
except mysql.connector.Error as err:
    # Handle the error appropriately
    raise err

# Snowflake Connection
import snowflake.connector

try:
    snowflake_connection = snowflake.connector.connect(
        user='DBADMIN',
        password='Dbadmin@12345678',
        account='NCPLJSM-YY65029', #account identifier from snowflake computing link
        role='ACCOUNTADMIN',
        warehouse='COMPUTE_WH'
        # database='SF_LANDING_DB',
        # schema='qa1_test'
    )
    # Optionally, you can check if the connection is established
    # by executing a simple query
    # cursor = snowflake_connection.cursor()
    # cursor.execute("SELECT 1")
except snowflake.connector.errors.Error as err:
    # Handle the error appropriately
    raise err
