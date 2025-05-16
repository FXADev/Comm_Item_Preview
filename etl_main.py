#!/usr/bin/env python
"""
Bridgepointe Commission Preview ETL - Main Orchestrator

This script coordinates the entire ETL workflow by:
1. Loading configuration
2. Verifying credentials
3. Extracting data from Redshift and Salesforce 
4. Loading data to SQL Server staging tables

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
from extractors.redshift_extractor import execute_redshift_queries
from extractors.salesforce_extractor import execute_salesforce_queries
from loaders.sql_server_loader import load_data_to_sql_server


def main():
    """
    Main ETL process orchestration function.
    
    This function coordinates the entire ETL workflow and serves as the
    entry point for the ETL process.
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
            return
        logging.info("Credentials verified successfully")
    
    # Load configuration
    config = load_config()
    if not config:
        logging.error("Failed to load configuration")
        return
    
    logging.info(f"Loaded configuration with {len(config.get('redshift', {}).get('queries', []))} Redshift queries and {len(config.get('salesforce', {}).get('queries', []))} Salesforce queries")
    
    # Variable to track if we need to load to SQL
    load_to_sql = not args.manual
    sql_conn = None
    
    try:
        # Extract data from sources
        logging.info("Extracting data from Redshift")
        redshift_results = execute_redshift_queries(config, batch_id, args.manual)
        
        logging.info("Extracting data from Salesforce")
        salesforce_results = execute_salesforce_queries(config, batch_id, args.manual)
        
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
                    logging.info(f"Loaded {rows} rows into {table}")
            
            # Load Salesforce data to SQL Server
            if salesforce_results:
                logging.info("Loading Salesforce data to staging tables")
                salesforce_load_summary = load_data_to_sql_server(sql_conn, salesforce_results, batch_id, 'salesforce')
                for table, rows in salesforce_load_summary.items():
                    logging.info(f"Loaded {rows} rows into {table}")
            
            logging.info("Data loading to SQL Server completed")
    
    except Exception as e:
        logging.error(f"Error in ETL process: {e}")
        print(f"\nERROR: {e}")
    
    finally:
        # Close SQL connection if open
        if sql_conn:
            sql_conn.close()
            logging.info("SQL Server connection closed")
    
    logging.info("ETL process completed")
    
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
