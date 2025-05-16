"""
Redshift Extractor Module

This module handles all interactions with the Redshift database,
including connection management and query execution.
"""

import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# Import utilities
from utils.config_loader import load_query_from_file


def get_redshift_connection(manual_mode=False):
    """
    Create a connection to the Redshift database.
    
    Args:
        manual_mode (bool): Whether the ETL is running in manual mode
        
    Returns:
        psycopg2.connection or None: Connection object if successful, None if manual mode
    """
    if manual_mode:
        logging.info("MANUAL MODE: Simulating Redshift connection")
        return None
        
    try:
        host = os.getenv('REDSHIFT_HOST')
        user = os.getenv('REDSHIFT_USER')
        password = os.getenv('REDSHIFT_PASSWORD')
        database = os.getenv('REDSHIFT_DATABASE')
        port = os.getenv('REDSHIFT_PORT', '5439')
        
        logging.info(f"Connecting to Redshift: {host}, Database: {database}")
        
        conn = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            dbname=database,
            port=port
        )
        
        logging.info("Connected to Redshift successfully")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Redshift: {e}")
        raise


def execute_redshift_queries(config, batch_id, manual_mode=False):
    """
    Execute Redshift queries defined in the configuration and return results.
    
    Args:
        config (dict): Configuration dictionary containing Redshift query settings
        batch_id (str): Batch ID for tracking
        manual_mode (bool): Whether the ETL is running in manual mode
        
    Returns:
        dict: Dictionary of query results with format:
            {query_name: {'data': rows, 'columns': column_names}}
    """
    results = {}
    
    if 'redshift' not in config or 'queries' not in config['redshift']:
        logging.warning("No Redshift queries defined in configuration")
        return results
    
    redshift_conn = None
    try:
        if not manual_mode:
            redshift_conn = get_redshift_connection()
        
        for query_config in config['redshift']['queries']:
            query_name = query_config['name']
            query_file = query_config['file']
            
            logging.info(f"Processing Redshift query: {query_name} from {query_file}")
            
            # Load SQL from file
            sql = load_query_from_file(query_file)
            if not sql:
                logging.error(f"Failed to load SQL from {query_file}")
                continue
            
            if manual_mode:
                logging.info(f"MANUAL MODE: Would execute Redshift query: {query_name}")
                # Create mock data for simulation
                results[query_name] = {
                    'data': [['mock_data']] * 5,  # 5 rows of mock data
                    'columns': ['column1', 'column2']
                }
            else:
                logging.info(f"Executing Redshift query: {query_name}")
                # Execute query
                with redshift_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    
                    # Convert to list of lists for bulk insert (with column order preserved)
                    data_rows = []
                    if rows:
                        columns = list(rows[0].keys())
                        for row in rows:
                            data_rows.append([row[col] for col in columns])
                    else:
                        columns = []
                    
                    logging.info(f"Redshift query {query_name} returned {len(data_rows)} rows")
                    
                    results[query_name] = {
                        'data': data_rows,
                        'columns': columns
                    }
        
        return results
    
    except Exception as e:
        logging.error(f"Error executing Redshift queries: {e}")
        return {}
    
    finally:
        # Close connection if it was opened
        if redshift_conn and not manual_mode:
            redshift_conn.close()
            logging.info("Redshift connection closed")
