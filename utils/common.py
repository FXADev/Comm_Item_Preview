"""
Common Utilities Module

This module provides common utility functions used across the ETL process.
"""

import logging
import pyodbc
import os


def get_sql_connection():
    """
    Create a connection to SQL Server using environment variables.
    
    Returns:
        pyodbc.Connection: Connection to SQL Server
        
    Raises:
        Exception: If connection fails
    """
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
    """
    Prepare staging tables by truncating existing data.
    
    Args:
        sql_conn (pyodbc.Connection): SQL Server connection
        config (dict): Configuration dictionary containing data source settings
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        cursor = sql_conn.cursor()
        
        # Truncate Redshift-derived tables
        if 'redshift' in config and 'queries' in config['redshift']:
            for query_config in config['redshift']['queries']:
                table_name = f"dbo.stg_{query_config['name']}"
                logging.info(f"Truncating table: {table_name}")
                cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        # Truncate Salesforce-derived tables
        if 'salesforce' in config and 'queries' in config['salesforce']:
            for query_config in config['salesforce']['queries']:
                table_name = f"dbo.stg_sf_{query_config['name']}"
                logging.info(f"Truncating table: {table_name}")
                cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        sql_conn.commit()
        logging.info("All staging tables truncated successfully")
        return True
    except Exception as e:
        logging.error(f"Error preparing staging tables: {e}")
        sql_conn.rollback()
        return False
