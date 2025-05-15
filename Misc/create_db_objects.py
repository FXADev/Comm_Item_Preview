import pyodbc
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Connect to SQL Server
conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv('SQL_SERVER')};DATABASE={os.getenv('SQL_DATABASE')};UID={os.getenv('SQL_USERNAME')};PWD={os.getenv('SQL_PASSWORD')}"

try:
    print(f"Connecting to {os.getenv('SQL_SERVER')}...")
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()
    
    print("Loading create_staging_tables.sql...")
    with open('sql/create_staging_tables.sql', 'r') as f:
        sql_script = f.read()
    
    print("Creating staging tables...")
    # Split the script by GO statements (if any)
    statements = sql_script.split('GO')
    
    # Execute each statement
    for statement in statements:
        if statement.strip():
            cursor.execute(statement)
            conn.commit()
    
    print("Creating stored procedure...")
    with open('sql/sp_nightly_commission_preview.sql', 'r') as f:
        sp_script = f.read()
    
    # Split the script by GO statements (if any)
    sp_statements = sp_script.split('GO')
    
    # Execute each statement
    for statement in sp_statements:
        if statement.strip():
            cursor.execute(statement)
            conn.commit()
    
    print("Database objects created successfully!")
    conn.close()

except Exception as e:
    print(f"Error: {e}")