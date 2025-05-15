#!/usr/bin/env python
"""
SQL Server Data Loader for Bridgepointe Commission Preview

This script loads the data retrieved from Redshift and Salesforce
into SQL Server staging tables.

Usage:
  python load_to_sql.py [--manual]
"""

import os
import sys
import logging
import datetime
import argparse
import pyodbc
import pandas as pd
from dotenv import load_dotenv

# Setup command-line arguments
parser = argparse.ArgumentParser(description='Load data to SQL Server staging tables')
parser.add_argument('--manual', action='store_true', help='Run in manual mode with prompts')
args = parser.parse_args()

# Generate batch ID and timestamp for logging
now = datetime.datetime.now()
batch_id = now.strftime("%Y%m%d%H%M%S")

# Setup logging
if not os.path.exists('logs'):
    os.makedirs('logs')
log_filename = f"logs/sql_load_{now.strftime('%Y%m%d_%H%M%S')}.txt"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)
logging.info(f"Logging to {log_filename}")

# Load environment variables
load_dotenv()

def get_sql_connection():
    """Create a connection to SQL Server"""
    try:
        server = os.getenv('SQL_SERVER')
        database = os.getenv('SQL_DATABASE')
        username = os.getenv('SQL_USERNAME')
        password = os.getenv('SQL_PASSWORD')
        driver = os.getenv('SQL_DRIVER', '{ODBC Driver 17 for SQL Server}')
        
        connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        logging.info(f"Connecting to SQL Server: {server}, Database: {database}")
        
        return pyodbc.connect(connection_string)
    except Exception as e:
        logging.error(f"Failed to connect to SQL Server: {e}")
        raise

def prepare_staging_tables(conn):
    """Run the stored procedure to prepare staging tables"""
    try:
        logging.info("Executing stored procedure to prepare staging tables")
        cursor = conn.cursor()
        cursor.execute("EXEC dbo.sp_nightly_commission_preview")
        conn.commit()
        logging.info("Staging tables prepared successfully")
    except Exception as e:
        logging.error(f"Failed to prepare staging tables: {e}")
        raise

def load_redshift_data():
    """Load sample data from Redshift queries"""
    # In a real scenario, this would come from the Redshift queries
    # For testing, we'll create some sample dataframes
    
    # Commission Items (sample data)
    commission_items = pd.DataFrame({
        'run_id': ['R123'] * 5,
        'run_ym': ['2025-05'] * 5,
        'primary_rep_name': ['John Doe', 'Jane Smith', 'Alice Jones', 'Bob Brown', 'Carol White'],
        'account_id': ['A001', 'A002', 'A003', 'A004', 'A005'],
        'commission_item_gross_commission': [1000.50, 1500.25, 750.75, 2000.00, 1250.80]
    })
    
    # Referral Payments (sample data)
    referral_payments = pd.DataFrame({
        'run_id': ['R123'] * 3,
        'run_ym': ['2025-05'] * 3,
        'primary_rep_name': ['John Doe', 'Jane Smith', 'Alice Jones'],
        'account_id': ['A001', 'A002', 'A003'],
        'referral_payment_amount': [250.00, 300.50, 175.25]
    })
    
    # Adjustment Items (empty sample)
    adjustment_items = pd.DataFrame({
        'run_id': [],
        'run_ym': [],
        'agency_id': [],
        'supplier_id': [],
        'commission_adjustment_gross_commission': []
    })
    
    return {
        'commission_items': commission_items,
        'referral_payments': referral_payments,
        'adjustment_items': adjustment_items
    }

def load_salesforce_data():
    """Load sample data from Salesforce queries"""
    # Agency data (sample)
    agency = pd.DataFrame({
        'Id': ['SF001', 'SF002', 'SF003'],
        'Name': ['Agency One', 'Agency Two', 'Agency Three'],
        'Agency_ID__c': ['A1', 'A2', 'A3'],
        'RPM_Agency_Name__c': ['RPM One', 'RPM Two', 'RPM Three']
    })
    
    # More sample dataframes for other Salesforce objects
    # ... (add as needed)
    
    return {
        'agency': agency,
        # Add other Salesforce dataframes
    }

def insert_dataframe_to_table(conn, df, table_name, batch_id):
    """Insert a dataframe into a SQL Server table"""
    if df.empty:
        logging.info(f"No data to insert for {table_name}")
        return 0
    
    cursor = conn.cursor()
    row_count = 0
    
    try:
        # Add ETL tracking fields
        df['etl_batch_id'] = batch_id
        df['extracted_at'] = datetime.datetime.now()
        
        # Get table columns
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        columns = [row[0] for row in cursor.fetchall()]
        
        # Filter dataframe to only include columns that exist in the table
        df_columns = [col for col in df.columns if col.lower() in [c.lower() for c in columns]]
        filtered_df = df[df_columns]
        
        logging.info(f"Inserting {len(filtered_df)} rows into {table_name}")
        
        if args.manual:
            print(f"\nWould insert {len(filtered_df)} rows into {table_name}")
            print(f"First 3 rows sample:")
            print(filtered_df.head(3))
            if input(f"Continue with {table_name} insert? (y/n): ").lower() != 'y':
                logging.info(f"Skipping {table_name}")
                return 0
        
        # Generate placeholders for SQL
        placeholders = ', '.join(['?' for _ in df_columns])
        column_names = ', '.join([f"[{col}]" for col in df_columns])
        
        # Prepare the insert statement
        insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        # Insert data in batches
        batch_size = 1000
        for i in range(0, len(filtered_df), batch_size):
            batch = filtered_df.iloc[i:i+batch_size]
            cursor.executemany(insert_sql, batch.values.tolist())
            row_count += len(batch)
            logging.info(f"Inserted batch of {len(batch)} rows into {table_name}")
        
        conn.commit()
        logging.info(f"Successfully inserted {row_count} rows into {table_name}")
        return row_count
    
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting data into {table_name}: {e}")
        raise
    
def main():
    logging.info(f"Starting SQL load process with batch ID: {batch_id}")
    
    if args.manual:
        logging.info("Running in manual mode - operations will require confirmation")
        print("\n=== MANUAL SQL LOAD EXECUTION ===")
        print(f"Batch ID: {batch_id}")
        print("This will test the SQL load process with sample data.\n")
        
        # For testing, use sample data
        redshift_data = load_redshift_data()
        salesforce_data = load_salesforce_data()
    else:
        logging.error("This script needs to be integrated with run_etl.py to use real data")
        print("\nERROR: This script currently only supports --manual mode for testing.")
        print("For production use, this functionality needs to be integrated with run_etl.py")
        return
    
    try:
        # Connect to SQL Server
        conn = get_sql_connection()
        
        # Prepare staging tables (truncate)
        if args.manual:
            if input("Prepare (truncate) staging tables? (y/n): ").lower() == 'y':
                prepare_staging_tables(conn)
            else:
                logging.info("Skipping table preparation")
        else:
            prepare_staging_tables(conn)
        
        # Insert Redshift data
        for table_name, df in redshift_data.items():
            target_table = f"dbo.stg_{table_name}"
            insert_dataframe_to_table(conn, df, target_table, batch_id)
        
        # Insert Salesforce data
        for object_name, df in salesforce_data.items():
            target_table = f"dbo.stg_sf_{object_name}"
            insert_dataframe_to_table(conn, df, target_table, batch_id)
        
        logging.info("SQL load process completed successfully")
        
        if args.manual:
            print("\n=== SQL LOAD PROCESS SIMULATION COMPLETE ===")
            print("In production mode, this would load actual data from Redshift and Salesforce.")
    
    except Exception as e:
        logging.error(f"Error in SQL load process: {e}")
        print(f"\nERROR: Failed to complete SQL load process: {e}")
    
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    main()
