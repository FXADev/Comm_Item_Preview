#!/usr/bin/env python
"""
Bridgepointe Commission Preview ETL - Main Orchestrator

This script coordinates the entire ETL workflow by:
1. Loading configuration
2. Verifying credentials
3. Extracting data from Redshift and Salesforce 
4. Loading data to SQL Server staging tables
5. Generating metrics for email notifications

The implementation follows a modular approach for better maintainability and testability.

Usage:
  python etl_main.py [--manual]
"""

import os
import sys
import argparse
import logging
from dotenv import load_dotenv

# Import custom modules
from utils.logger import setup_logging
from utils.config_loader import load_config, verify_credentials
from utils.common import get_sql_connection, prepare_staging_tables
from extractors.redshift_extractor import execute_redshift_queries, RedshiftConnectionError
from extractors.salesforce_extractor import execute_salesforce_queries, SalesforceConnectionError
from loaders.sql_server_loader import load_data_to_sql_server
from utils.notification_helper import save_etl_metrics, generate_metrics_table_markdown


def main():
    """
    Main ETL process orchestration function.
    
    This function coordinates the entire ETL workflow and serves as the
    entry point for the ETL process. It also collects metrics for email notifications.
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    # Setup command-line arguments
    parser = argparse.ArgumentParser(description='Run Bridgepointe Commission Preview ETL')
    parser.add_argument('--manual', action='store_true', help='Run in manual mode with user prompts')
    args = parser.parse_args()
    
    # Setup logging and generate batch ID
    log_filename, batch_id = setup_logging(args.manual)
    
    # Load environment variables
    loading_result = load_dotenv()
    if not loading_result:
        logging.warning("No .env file found or it couldn't be loaded. Make sure credentials are available in environment variables.")
    else:
        logging.info(".env file loaded successfully")
    
    # Start ETL process
    logging.info("Starting ETL process")
    
    if args.manual:
        logging.info("Running in MANUAL mode - This is a simulation only")
        print("\n=== MANUAL MODE - SIMULATION ONLY ===")
        print("No database connections will be made")
        print("Sample data will be generated for testing\n")
    else:
        # Verify credentials before proceeding
        if not verify_credentials(args.manual):
            logging.error("Missing required credentials - cannot proceed with ETL process")
            print("\nERROR: Missing required credentials.")
            print("Please run 'python verify_credentials.py' to diagnose credential issues")
            print("Or run with --manual flag to test without connecting to data sources")
            return 1
        logging.info("Credentials verified successfully")
    
    # Load configuration
    config = load_config()
    if not config:
        logging.error("Failed to load configuration")
        return 1
    
    logging.info(f"Loaded configuration with {len(config.get('redshift', {}).get('queries', []))} Redshift queries and {len(config.get('salesforce', {}).get('queries', []))} Salesforce queries")
    
    # Variable to track if we need to load to SQL
    load_to_sql = not args.manual
    sql_conn = None
    
    # Initialize metrics dictionary to track row counts
    etl_metrics = {
        'sources': {
            'redshift': {},
            'salesforce': {}
        },
        'summary': {
            'total_queried': 0,
            'total_inserted': 0
        }
    }
    
    # Track if we had a connection failure
    connection_failed = False
    connection_error_msg = ""
    
    try:
        # Extract data from sources
        logging.info("Extracting data from Redshift")
        try:
            redshift_results = execute_redshift_queries(config, batch_id, args.manual)
            
            # Track Redshift query row counts
            for query_name, result in redshift_results.items():
                rows_queried = len(result['data']) if result['data'] else 0
                etl_metrics['sources']['redshift'][query_name] = {
                    'rows_queried': rows_queried,
                    'rows_inserted': 0  # Will be updated after insertion
                }
                etl_metrics['summary']['total_queried'] += rows_queried
                logging.info(f"Redshift query {query_name} returned {rows_queried} rows")
                
        except RedshiftConnectionError as e:
            connection_failed = True
            connection_error_msg = f"REDSHIFT CONNECTION FAILURE: {str(e)}"
            logging.error(connection_error_msg)
            raise  # Re-raise to be caught by outer try/except
        
        logging.info("Extracting data from Salesforce")
        try:
            salesforce_results = execute_salesforce_queries(config, batch_id, args.manual)
            
            # Track Salesforce query row counts
            for query_name, result in salesforce_results.items():
                rows_queried = len(result['data']) if result['data'] else 0
                etl_metrics['sources']['salesforce'][query_name] = {
                    'rows_queried': rows_queried,
                    'rows_inserted': 0  # Will be updated after insertion
                }
                etl_metrics['summary']['total_queried'] += rows_queried
                logging.info(f"Salesforce query {query_name} returned {rows_queried} rows")
                
        except SalesforceConnectionError as e:
            connection_failed = True
            connection_error_msg = f"SALESFORCE CONNECTION FAILURE: {str(e)}"
            logging.error(connection_error_msg)
            raise  # Re-raise to be caught by outer try/except
        
        # Load data to SQL Server if we have results and not in manual mode
        if load_to_sql and (redshift_results or salesforce_results):
            logging.info("Preparing to load data to SQL Server staging tables")
            
            # Connect to SQL Server
            sql_conn = get_sql_connection()
            
            # Prepare staging tables
            logging.info("Preparing staging tables (truncating existing data)")
            prepare_staging_tables(sql_conn, config)
            
            # Load Redshift data to SQL Server
            if redshift_results:
                logging.info("Loading Redshift data to staging tables")
                redshift_load_summary = load_data_to_sql_server(sql_conn, redshift_results, batch_id, 'redshift')
                for table, rows in redshift_load_summary.items():
                    # Extract query name from table name (remove prefix)
                    query_name = table.replace('dbo.stg_', '')
                    
                    # Update metrics with insertion counts
                    if query_name in etl_metrics['sources']['redshift']:
                        etl_metrics['sources']['redshift'][query_name]['rows_inserted'] = rows
                        etl_metrics['summary']['total_inserted'] += rows
                    
                    logging.info(f"Loaded {rows} rows into {table}")
            
            # Load Salesforce data to SQL Server
            if salesforce_results:
                logging.info("Loading Salesforce data to staging tables")
                salesforce_load_summary = load_data_to_sql_server(sql_conn, salesforce_results, batch_id, 'salesforce')
                for table, rows in salesforce_load_summary.items():
                    # Extract query name from table name (remove prefix)
                    query_name = table.replace('dbo.stg_sf_', '')
                    
                    # Update metrics with insertion counts
                    if query_name in etl_metrics['sources']['salesforce']:
                        etl_metrics['sources']['salesforce'][query_name]['rows_inserted'] = rows
                        etl_metrics['summary']['total_inserted'] += rows
                    
                    logging.info(f"Loaded {rows} rows into {table}")
            
            logging.info("Data loading to SQL Server completed")
    
    except (RedshiftConnectionError, SalesforceConnectionError) as e:
        # Connection errors - fail the process with clear message
        logging.error(f"ETL PROCESS FAILED DUE TO CONNECTION ERROR")
        print(f"\n{'='*60}")
        print(f"ETL PROCESS FAILED - CONNECTION ERROR")
        print(f"{'='*60}")
        print(f"\n{connection_error_msg}\n")
        print(f"Action Required:")
        print(f"1. Verify your credentials in the .env file or GitHub Secrets")
        print(f"2. Check if your password has expired")
        print(f"3. Ensure network connectivity to the database")
        print(f"\nLog file: {log_filename}")
        print(f"{'='*60}\n")
        return 1
        
    except Exception as e:
        logging.error(f"Error in ETL process: {e}")
        print(f"\nERROR: {e}")
        return 1
        
    finally:
        # Close SQL connection if open
        if sql_conn:
            sql_conn.close()
            logging.info("SQL Server connection closed")
    
    # Save metrics for notification
    metrics_file = save_etl_metrics(etl_metrics, batch_id)
    if metrics_file:
        logging.info(f"ETL metrics saved to {metrics_file}")
        
        # Generate metrics table for console display
        metrics_table = generate_metrics_table_markdown(etl_metrics)
    else:
        metrics_table = "No metrics available"
    
    logging.info("ETL process completed")
    
    if args.manual:
        print("\n=== ETL PROCESS SIMULATION COMPLETE ===")
        print("In automated mode, this would have:")
        print("1. Connected to Redshift and Salesforce")
        print("2. Executed the queries defined in config.yml")
        print("3. Loaded results into staging tables")
        print(f"4. Used batch ID {batch_id} for tracking")
        print("\n=== ETL METRICS (SIMULATED) ===")
        print(metrics_table)
    else:
        print("\n=== ETL PROCESS COMPLETE ===")
        print(f"Process completed successfully with batch ID: {batch_id}")
        print(f"Log file: {log_filename}")
        if load_to_sql:
            print("Data has been loaded to SQL Server staging tables")
            print("You can now query the staging tables to verify the data")
        print("\n=== ETL METRICS ===")
        print(metrics_table)
    
    return 0  # Success


if __name__ == "__main__":
    sys.exit(main())
