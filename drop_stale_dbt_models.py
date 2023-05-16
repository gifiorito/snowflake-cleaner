# The following script drops stale dbt-generated models
import snowflake.connector

# Snowflake connection details
account = '<your_snowflake_account_url>'
user = '<your_user>'
password = '<your_password>'
warehouse = '<your_warehouse>'

# Databases to check all schemas
databases = ['<database1>', '<database2>', '<database3>']

# SQL query to identify dbt models that haven't been updated in over 60 days
query = """
SELECT table_catalog, table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_catalog = '{database}'
    AND DATE(last_altered) < DATEADD('day',-60,CURRENT_DATE())
"""

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=account,
    user=user,
    password=password,
    warehouse=warehouse
)

# Create a cursor to execute SQL queries
cursor = conn.cursor()

# Iterate over the databases
for database in databases:
    # Switch to the current database
    cursor.execute(f"USE DATABASE {database}")

    # Execute the custom SQL query
    cursor.execute(query.format(database=database))

    # Iterate over the result set and drop the tables/views
    for row in cursor:
        table_catalog, table_schema, table_name, table_type = row
        object_type = 'TABLE' if table_type == 'BASE TABLE' else 'VIEW'
        drop_statement = f"DROP {object_type} IF EXISTS {table_catalog}.{table_schema}.{table_name}"
        cursor.execute(drop_statement)
        print(f"Dropped {object_type}: {table_catalog}.{table_schema}.{table_name}")

# Close the cursor and connection
cursor.close()
conn.close()
