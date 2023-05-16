# The following script drops schemas generated on dbt's CI check
import snowflake.connector

# Snowflake connection details
account = '<your_snowflake_account_url>'
user = '<your_user>'
password = '<your_password>'
schema_pattern = 'dbt_cloud_pr%'

# Snowflake databases to delete schemas from
databases = ['<database1>', '<database2>', '<database3>']

# Iterate over each database
for database in databases:
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        database=database
    )

    # Get the list of schemas matching the pattern
    cursor = conn.cursor()
    cursor.execute(f"SHOW SCHEMAS LIKE '{schema_pattern}'")

    drop_statements = []

    # Generate the DROP SCHEMA statements
    for row in cursor:
        schema_name = row[1]
        drop_statement = f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"
        drop_statements.append(drop_statement)

    # Execute the DROP SCHEMA statements
    for statement in drop_statements:
        cursor.execute(statement)
        print(f"Dropped schema: {statement}")

    # Close the cursor and connection
    cursor.close()
    conn.close()
