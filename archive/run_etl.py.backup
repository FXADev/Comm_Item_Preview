#!/usr/bin/env python
"""
Bridgepointe Commission Preview ETL

This script loads SQL queries from files defined in config.yml,
executes them against the appropriate data sources,
and loads the results into staging tables.

Usage:
  python run_etl.py [--manual]
"""

import os
import sys
import yaml
import logging
import datetime
import time
import argparse
import pandas as pd
import pyodbc
from decimal import Decimal
from dotenv import load_dotenv

# Generate batch ID and timestamp for logging
now = datetime.datetime.now()
batch_id = now.strftime("%Y%m%d%H%M%S")

# Setup command-line arguments
parser = argparse.ArgumentParser(description='Run Bridgepointe Commission Preview ETL')
parser.add_argument('--manual', action='store_true', help='Run in manual mode with user prompts')
args = parser.parse_args()

# Setup logging
import os

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Generate log filename with timestamp
log_filename = f"logs/etl_log_{now.strftime('%Y%m%d_%H%M%S')}.txt"

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
loading_result = load_dotenv()
if not loading_result:
    logging.warning("No .env file found or it couldn't be loaded. Make sure credentials are available in environment variables.")
else:
    logging.info(".env file loaded successfully")

# Verify essential credentials are present
def verify_credentials():
    missing = []
    
    # Check Redshift credentials
    for var in ['REDSHIFT_HOST', 'REDSHIFT_USER', 'REDSHIFT_PASSWORD', 'REDSHIFT_DATABASE']:
        if not os.getenv(var):
            missing.append(var)
    
    # Check Salesforce credentials
    for var in ['SF_USERNAME', 'SF_PASSWORD', 'SF_SECURITY_TOKEN']:
        if not os.getenv(var):
            missing.append(var)
    
    # Check SQL Server credentials (for staging table insertion)
    for var in ['SQL_SERVER', 'SQL_DATABASE', 'SQL_USERNAME', 'SQL_PASSWORD']:
        if not os.getenv(var):
            missing.append(var)
    
    if missing:
        logging.error(f"Missing required environment variables: {', '.join(missing)}")
        if not args.manual:
            logging.error("Run 'python verify_credentials.py' to diagnose credential issues")
            logging.error("For testing without proper credentials, use --manual flag")
            return False
    
    return True

def load_config():
    """Load the configuration from config.yml"""
    try:
        with open('config.yml', 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        sys.exit(1)

def load_query_from_file(file_path):
    """Load a query from a file"""
    try:
        with open(file_path, 'r') as f:
            return f.read()
    except Exception as e:
        logging.error(f"Failed to load query from {file_path}: {e}")
        return None

def get_sql_connection():
    """Create a connection to SQL Server"""
    try:
        server = os.getenv('SQL_SERVER')
        database = os.getenv('SQL_DATABASE')
        username = os.getenv('SQL_USERNAME')
        password = os.getenv('SQL_PASSWORD')
        driver = os.getenv('SQL_DRIVER', '{ODBC Driver 18 for SQL Server}')
        
        connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        logging.info(f"Connecting to SQL Server: {server}, Database: {database}")
        
        return pyodbc.connect(connection_string)
    except Exception as e:
        logging.error(f"Failed to connect to SQL Server: {e}")
        raise

def prepare_staging_tables(sql_conn, config):
    """Prepare staging tables by truncating existing data"""
    try:
        cursor = sql_conn.cursor()
        
        # First attempt to run the stored procedure (legacy approach)
        logging.info("Executing stored procedure to prepare staging tables")
        try:
            cursor.execute("EXEC dbo.sp_nightly_commission_preview")
            sql_conn.commit()
            logging.info("Stored procedure executed successfully")
        except Exception as e:
            logging.warning(f"Stored procedure execution failed: {e}")
            logging.info("Will proceed with direct table truncation")
        
        # Also explicitly truncate all staging tables to ensure clean state
        logging.info("Explicitly truncating staging tables...")
        
        # Truncate Redshift data tables
        for query_config in config['redshift']['queries']:
            table_name = f"dbo.stg_{query_config['name']}"
            try:
                logging.info(f"Truncating {table_name}")
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                sql_conn.commit()
                logging.info(f"Successfully truncated {table_name}")
            except Exception as e:
                logging.error(f"Failed to truncate {table_name}: {e}")
                # Continue with other tables even if one fails
        
        # Truncate Salesforce data tables
        for query_config in config['salesforce']['queries']:
            table_name = f"dbo.stg_sf_{query_config['name']}"
            try:
                logging.info(f"Truncating {table_name}")
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                sql_conn.commit()
                logging.info(f"Successfully truncated {table_name}")
            except Exception as e:
                logging.error(f"Failed to truncate {table_name}: {e}")
                # Continue with other tables even if one fails
        
        logging.info("All staging tables prepared successfully")
    except Exception as e:
        logging.error(f"Failed to prepare staging tables: {e}")
        raise

def insert_to_sql_table(conn, table_name, data, columns, batch_id, batch_size=5000):
    """Insert data into a SQL Server table using optimized bulk loading
    
    Args:
        conn: SQL Server connection
        table_name: Target table name
        data: List of rows to insert
        columns: List of column names
        batch_id: Batch ID for tracking
        batch_size: Batch size for inserts (increased for performance)
    
    Returns:
        Number of rows inserted
    """
    if not data or len(data) == 0:
        logging.info(f"No data to insert for {table_name}")
        return 0
    
    try:
        cursor = conn.cursor()
        total_rows = len(data)
        row_count = 0
        
        # Add tracking columns - match the SQL table column names
        all_columns = columns.copy()
        all_columns.append('etl_batch_id')  # Changed from 'batch_id' to match SQL table
        all_columns.append('extracted_at')  # Changed from 'extraction_time' to match SQL table
        
        # Create parameterized query
        placeholders = ', '.join(['?' for _ in range(len(all_columns))])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(all_columns)}) VALUES ({placeholders})"
        
        # Insert in larger batches for better performance
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Optimize by preparing all data upfront instead of per batch
        start_prep = datetime.datetime.now()
        logging.info(f"Preparing {total_rows} rows for insertion into {table_name}")
        
        # Pre-process all data at once
        modified_data = []
        for row in data:
            # Convert tuple to list if needed
            row_list = list(row) if isinstance(row, tuple) else row
            
            # Fix decimal precision issues
            for i, val in enumerate(row_list):
                # Handle Decimal values to ensure they fit within SQL Server precision
                if isinstance(val, (Decimal, float)):
                    if val is None:
                        row_list[i] = None
                    else:
                        try:
                            # Limit to max value for decimal(18,2)
                            max_value = 9999999999999999.99
                            min_value = -9999999999999999.99
                            
                            if val > max_value:
                                logging.warning(f"Value {val} exceeds maximum decimal(18,2) limit, capping at {max_value}")
                                row_list[i] = max_value
                            elif val < min_value:
                                logging.warning(f"Value {val} below minimum decimal(18,2) limit, capping at {min_value}")
                                row_list[i] = min_value
                            else:
                                # Round to 2 decimal places
                                row_list[i] = round(val, 2)
                        except Exception as e:
                            logging.warning(f"Error handling decimal value {val}: {str(e)}. Setting to NULL.")
                            row_list[i] = None
            
            # Use the same column names as defined in the SQL tables
            row_list.append(batch_id)  # This will go into etl_batch_id column
            row_list.append(now)       # This will go into extracted_at column
            modified_data.append(row_list)
        
        prep_time = (datetime.datetime.now() - start_prep).total_seconds()
        logging.info(f"Data preparation completed in {prep_time:.2f} seconds")
        
        # Enable fast loading
        cursor.fast_executemany = True
        
        start_time = datetime.datetime.now()
        for i in range(0, total_rows, batch_size):
            batch_end = min(i + batch_size, total_rows)
            batch = modified_data[i:batch_end]
            
            cursor.executemany(insert_sql, batch)
            row_count += len(batch)
            
            # Only log every 10,000 rows to reduce log overhead
            if i % 20000 == 0 or batch_end == total_rows:
                elapsed = (datetime.datetime.now() - start_time).total_seconds()
                rate = (i + len(batch)) / elapsed if elapsed > 0 else 0
                logging.info(f"Inserted {i + len(batch):,} of {total_rows:,} rows into {table_name} ({(i + len(batch))/total_rows*100:.1f}%) - {rate:.1f} rows/sec")
        
        # Single commit at the end for better performance
        conn.commit()
        
        total_time = (datetime.datetime.now() - start_time).total_seconds()
        rate = total_rows / total_time if total_time > 0 else 0
        logging.info(f"Completed inserting {total_rows:,} rows into {table_name} in {total_time:.2f} seconds ({rate:.1f} rows/sec)")
        
        return row_count
    
    except Exception as e:
        logging.error(f"Error inserting data into {table_name}: {e}")
        conn.rollback()
        raise

def execute_redshift_queries(config, batch_id):
    """Execute Redshift queries and return results for SQL insertion"""
    import psycopg2  # Import here so script doesn't fail if not needed
    
    redshift_results = {}
    
    logging.info("Connecting to Redshift...")
    
    # In a manual run, we'll just simulate this
    if args.manual:
        for query_config in config['redshift']['queries']:
            query_name = query_config['name']
            query_file = query_config['file']
            query = load_query_from_file(query_file)
            
            if query:
                logging.info(f"Would execute Redshift query: {query_name} from {query_file}")
                if input(f"Continue with {query_name}? (y/n): ").lower() != 'y':
                    logging.info(f"Skipping {query_name}")
        return redshift_results
    
    # In automated mode, we'd actually connect to Redshift
    try:
        conn = psycopg2.connect(
            host=os.getenv('REDSHIFT_HOST'),
            user=os.getenv('REDSHIFT_USER'),
            password=os.getenv('REDSHIFT_PASSWORD'),
            database=os.getenv('REDSHIFT_DATABASE'),
            port=int(os.getenv('REDSHIFT_PORT', '5439'))
        )
        
        cursor = conn.cursor()
        
        for query_config in config['redshift']['queries']:
            query_name = query_config['name']
            query_file = query_config['file']
            query = load_query_from_file(query_file)
            
            if query:
                logging.info(f"Executing Redshift query: {query_name}")
                cursor.execute(query)
                results = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                
                # Store results for SQL insertion
                redshift_results[query_name] = {
                    'data': results,
                    'columns': column_names
                }
                
                logging.info(f"Retrieved {len(results)} rows for {query_name}")
        
        conn.close()
        logging.info("Redshift queries completed")
        return redshift_results
        
    except Exception as e:
        logging.error(f"Redshift query execution failed: {e}")
        return redshift_results

def execute_salesforce_queries(config, batch_id):
    """Execute Salesforce SOQL queries and return results for SQL insertion"""
    salesforce_results = {}
    
    try:
        from simple_salesforce import Salesforce  # Import here so script doesn't fail if not needed
        
        logging.info("Connecting to Salesforce...")
        
        # In a manual run, we'll just simulate this
        if args.manual:
            for query_config in config['salesforce']['queries']:
                query_name = query_config['name']
                query_file = query_config['file']
                query = load_query_from_file(query_file)
                
                if query:
                    logging.info(f"Would execute Salesforce query: {query_name} from {query_file}")
                    if input(f"Continue with {query_name}? (y/n): ").lower() != 'y':
                        logging.info(f"Skipping {query_name}")
            return salesforce_results
        
        # In automated mode, we'd actually connect to Salesforce
        # Get Salesforce credentials
        sf_username = os.getenv('SF_USERNAME')
        sf_password = os.getenv('SF_PASSWORD')
        sf_security_token = os.getenv('SF_SECURITY_TOKEN')
        sf_domain = os.getenv('SF_DOMAIN', 'login')
        
        logging.info(f"Connecting to Salesforce with domain: {sf_domain}")
        
        sf = Salesforce(
            username=sf_username,
            password=sf_password,
            security_token=sf_security_token,
            domain=sf_domain
        )
        
        for query_config in config['salesforce']['queries']:
            query_name = query_config['name']
            query_file = query_config['file']
            query = load_query_from_file(query_file)
            
            if query:
                logging.info(f"Executing Salesforce query: {query_name}")
                
                # Initialize variables for pagination
                all_records = []
                total_size = 0
                result = sf.query(query)
                batch_count = 1
                
                # Process initial batch of records
                records = result['records']
                total_size = result['totalSize']
                all_records.extend(records)
                logging.info(f"Retrieved batch {batch_count}: {len(records)} records for {query_name}")
                
                # Handle pagination if there are more records
                while 'nextRecordsUrl' in result and result.get('done') is False:
                    batch_count += 1
                    logging.info(f"Fetching next batch (batch {batch_count}) for {query_name}...")
                    result = sf.query_more(result['nextRecordsUrl'], identifier_is_url=True)
                    records = result['records']
                    all_records.extend(records)
                    logging.info(f"Retrieved batch {batch_count}: {len(records)} records for {query_name}")
                
                # Now process all the records
                if all_records:
                    # Get column names from first record (excluding attributes)
                    all_fields = list(all_records[0].keys())
                    fields = [f for f in all_fields if f != 'attributes']
                    
                    # Extract data from records
                    data = []
                    for record in all_records:
                        row = [record.get(field) for field in fields]
                        data.append(row)
                    
                    # Store results for SQL insertion
                    salesforce_results[query_name] = {
                        'data': data,
                        'columns': fields
                    }
                
                logging.info(f"Retrieved a total of {len(all_records)} records out of {total_size} for {query_name}")
        
        logging.info("Salesforce queries completed successfully")
        return salesforce_results
        
    except Exception as e:
        logging.error(f"Salesforce query execution failed: {e}")
        return salesforce_results

def main():
    """Main ETL process"""
    logging.info(f"Starting ETL process with batch ID: {batch_id}")
    
    # Load configuration
    config = load_config()
    logging.info(f"Loaded configuration with {len(config['redshift']['queries'])} Redshift queries and {len(config['salesforce']['queries'])} Salesforce queries")
    
    if args.manual:
        logging.info("Running in manual mode - queries will be displayed but not executed")
        print("\n=== MANUAL ETL EXECUTION ===")
        print(f"Batch ID: {batch_id}")
        print("This will test the ETL process without actually connecting to data sources.\n")
    else:
        # Verify credentials before proceeding
        if not verify_credentials():
            logging.error("Missing required credentials - cannot proceed with ETL process")
            print("\nERROR: Missing required credentials.")
            print("Please run 'python verify_credentials.py' to diagnose credential issues")
            print("Or run with --manual flag to test without connecting to data sources")
            return
        logging.info("Credentials verified successfully")
    
    # Variable to track if we need to load to SQL
    load_to_sql = not args.manual
    sql_conn = None
    
    try:
        # Execute queries and get results
        redshift_results = execute_redshift_queries(config, batch_id)
        salesforce_results = execute_salesforce_queries(config, batch_id)
        
        # Load data to SQL Server if we have results and not in manual mode
        if load_to_sql and (redshift_results or salesforce_results):
            logging.info("Preparing to load data to SQL Server staging tables")
            
            # Connect to SQL Server
            sql_conn = get_sql_connection()
            
            # Prepare staging tables
            logging.info("Preparing staging tables (truncating existing data)")
            prepare_staging_tables(sql_conn, config)
            
            # Load Redshift data
            for query_name, result in redshift_results.items():
                target_table = f"dbo.stg_{query_name}"
                logging.info(f"Loading data to {target_table}")
                
                try:
                    rows_inserted = insert_to_sql_table(
                        sql_conn,
                        target_table,
                        result['data'],
                        result['columns'],
                        batch_id
                    )
                    logging.info(f"Loaded {rows_inserted} rows into {target_table}")
                except Exception as e:
                    logging.error(f"Error loading data to {target_table}: {e}")
            
            # Load Salesforce data
            for query_name, result in salesforce_results.items():
                target_table = f"dbo.stg_sf_{query_name}"
                logging.info(f"Loading data to {target_table}")
                
                try:
                    rows_inserted = insert_to_sql_table(
                        sql_conn, 
                        target_table,
                        result['data'],
                        result['columns'],
                        batch_id
                    )
                    logging.info(f"Loaded {rows_inserted} rows into {target_table}")
                except Exception as e:
                    logging.error(f"Error loading data to {target_table}: {e}")
            
            logging.info("Data loading to SQL Server completed")
    
    except Exception as e:
        logging.error(f"Error in ETL process: {e}")
        print(f"\nERROR: {e}")
    
    finally:
        # Close SQL connection if open
        if sql_conn:
            sql_conn.close()
            logging.info("SQL Server connection closed")
    
    logging.info("ETL process completed successfully")
    
    if args.manual:
        print("\n=== ETL PROCESS SIMULATION COMPLETE ===")
        print("In automated mode, this would have:")
        print("1. Connected to Redshift and Salesforce")
        print("2. Executed the queries defined in config.yml")
        print("3. Loaded results into staging tables")
        print(f"4. Used batch ID {batch_id} for tracking")
    else:
        print("\n=== ETL PROCESS COMPLETE ===")
        print(f"Process completed successfully with batch ID: {batch_id}")
        print(f"Log file: {log_filename}")
        if load_to_sql:
            print("Data has been loaded to SQL Server staging tables")
            print("You can now query the staging tables to verify the data")

if __name__ == "__main__":
    main()
