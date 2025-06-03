"""
Redshift Extractor Module

This module handles all interactions with the Redshift database,
including connection management, query execution, and data transformation
to ensure SQL Server compatibility.
"""

import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# Import utilities
from utils.config_loader import load_query_from_file
from utils.data_transformers import transform_row_data, log_transformation_summary


class RedshiftConnectionError(Exception):
    """Custom exception for Redshift connection failures"""
    pass


def get_redshift_connection(manual_mode=False):
    """
    Create a connection to the Redshift database.
    
    Args:
        manual_mode (bool): Whether the ETL is running in manual mode
        
    Returns:
        psycopg2.connection or None: Connection object if successful, None if manual mode
        
    Raises:
        RedshiftConnectionError: If connection fails (including authentication issues)
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
        
        # Test the connection by executing a simple query
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        
        logging.info("Connected to Redshift successfully")
        return conn
        
    except psycopg2.OperationalError as e:
        error_msg = f"Redshift connection failed for user {user}@{host}. "
        error_code = str(e)
        
        if "password authentication failed" in error_code.lower():
            error_msg += "Authentication failed - please verify your username and password."
        elif "timeout" in error_code.lower():
            error_msg += "Connection timeout - please verify the host and port are correct."
        elif "could not translate host name" in error_code.lower():
            error_msg += "Invalid hostname - please verify the Redshift endpoint."
        elif "password" in error_code.lower() and "expired" in error_code.lower():
            error_msg += "PASSWORD EXPIRED - Please update your Redshift password."
        else:
            error_msg += f"Error: {error_code}"
        
        logging.error(error_msg)
        raise RedshiftConnectionError(error_msg) from e
        
    except Exception as e:
        error_msg = f"Failed to connect to Redshift: {str(e)}"
        logging.error(error_msg)
        raise RedshiftConnectionError(error_msg) from e


def execute_redshift_queries(config, batch_id, manual_mode=False):
    """
    Execute Redshift queries defined in the configuration and return transformed results.
    
    Args:
        config (dict): Configuration dictionary containing Redshift query settings
        batch_id (str): Batch ID for tracking
        manual_mode (bool): Whether the ETL is running in manual mode
        
    Returns:
        dict: Dictionary of query results with format:
            {query_name: {'data': transformed_rows, 'columns': column_names}}
            
    Raises:
        RedshiftConnectionError: If connection to Redshift fails
    """
    results = {}
    
    if 'redshift' not in config or 'queries' not in config['redshift']:
        logging.warning("No Redshift queries defined in configuration")
        return results
    
    redshift_conn = None
    try:
        if not manual_mode:
            # This will raise RedshiftConnectionError if connection fails
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
                # Create mock data for simulation with some realistic financial values
                mock_data = [
                    ['12345', 1250.75, 0.15, '2024-05-20 10:30:00'],
                    ['12346', 2500.50, 0.20, '2024-05-21 11:45:00'],
                    ['12347', 999.99, 0.12, '2024-05-22 09:15:00']
                ]
                columns = ['item_id', 'commission_amount', 'rate', 'created_date']
                
                # Apply transformations even to mock data for testing
                transformed_data = []
                total_capped_values = 0
                for row in mock_data:
                    transformed_row, capped_count = transform_row_data(row, columns, 'redshift')
                    transformed_data.append(transformed_row)
                    total_capped_values += capped_count
                
                results[query_name] = {
                    'data': transformed_data,
                    'columns': columns
                }
                log_transformation_summary(len(mock_data), len(transformed_data), 'Redshift', query_name, total_capped_values)
            else:
                logging.info(f"Executing Redshift query: {query_name}")
                try:
                    # Execute query
                    with redshift_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        
                        # Convert to list of lists for bulk insert (with column order preserved)
                        raw_data_rows = []
                        if rows:
                            columns = list(rows[0].keys())
                            for row in rows:
                                raw_data_rows.append([row[col] for col in columns])
                            
                            logging.info(f"Redshift query {query_name} returned {len(raw_data_rows)} raw rows")
                            
                            # Apply data transformations
                            logging.info(f"Applying Redshift-specific transformations to {query_name}")
                            transformed_data_rows = []
                            transformation_errors = 0
                            large_value_count = 0
                            total_capped_values = 0
                            
                            # Check for potentially problematic fields in this query
                            numeric_fields = [col for col in columns if any(keyword in col.lower() for keyword in 
                                            ['amount', 'commission', 'billed', 'payment', 'profit', 'deduction', 'override', 'rate'])]
                            if numeric_fields:
                                logging.info(f"Monitoring numeric fields in {query_name}: {numeric_fields}")
                            
                            for i, row in enumerate(raw_data_rows):
                                try:
                                    # Check for large values before transformation
                                    for j, val in enumerate(row):
                                        if isinstance(val, (int, float)) and val is not None:
                                            field_name = columns[j] if j < len(columns) else f'column_{j}'
                                            # Use appropriate limit based on field type  
                                            if any(keyword in field_name.lower() for keyword in ['rate', 'split']):
                                                limit = 9999.9999
                                            else:
                                                limit = 999999999999999.99  # decimal(18,2) limit
                                            
                                            if abs(val) > limit:
                                                logging.warning(f"Row {i}, field '{field_name}': Large value detected: {val} (limit: {limit})")
                                                large_value_count += 1
                                    
                                    transformed_row, capped_count = transform_row_data(row, columns, 'redshift')
                                    transformed_data_rows.append(transformed_row)
                                    total_capped_values += capped_count
                                except Exception as e:
                                    logging.error(f"Error transforming row {i} in {query_name}: {e}")
                                    transformation_errors += 1
                                    # Skip problematic rows instead of crashing
                                    continue
                            
                            if large_value_count > 0:
                                logging.warning(f"Found {large_value_count} values exceeding limits in {query_name}")
                            
                            if transformation_errors > 0:
                                logging.warning(f"Skipped {transformation_errors} problematic rows during transformation in {query_name}")
                            
                            log_transformation_summary(len(raw_data_rows), len(transformed_data_rows), 'Redshift', query_name, total_capped_values)
                            
                            results[query_name] = {
                                'data': transformed_data_rows,
                                'columns': columns
                            }
                        else:
                            columns = []
                            logging.warning(f"Redshift query {query_name} returned no rows")
                            results[query_name] = {
                                'data': [],
                                'columns': []
                            }
                            
                except psycopg2.OperationalError as query_error:
                    # Check for connection-related errors during query execution
                    error_msg = str(query_error)
                    if any(phrase in error_msg.lower() for phrase in ["connection", "timeout", "closed", "terminated"]):
                        connection_error_msg = f"Redshift connection lost while executing query {query_name}: {error_msg}"
                        logging.error(connection_error_msg)
                        raise RedshiftConnectionError(connection_error_msg) from query_error
                    else:
                        # Re-raise other operational errors
                        raise
                        
                except psycopg2.ProgrammingError as query_error:
                    # Check for permission/access errors
                    error_msg = str(query_error)
                    if "permission denied" in error_msg.lower():
                        permission_error_msg = f"Permission denied accessing Redshift table in query {query_name}. Please verify user permissions."
                        logging.error(permission_error_msg)
                        raise RedshiftConnectionError(permission_error_msg) from query_error
                    else:
                        # Re-raise other programming errors
                        raise
        
        return results
    
    except RedshiftConnectionError:
        # Re-raise connection errors to fail the ETL process
        raise
    
    except Exception as e:
        error_msg = f"Unexpected error executing Redshift queries: {str(e)}"
        logging.error(error_msg)
        # Convert unexpected errors to connection errors to fail the process
        raise RedshiftConnectionError(error_msg) from e
    
    finally:
        # Close connection if it was opened
        if redshift_conn and not manual_mode:
            try:
                redshift_conn.close()
                logging.info("Redshift connection closed")
            except Exception as close_error:
                logging.warning(f"Error closing Redshift connection: {close_error}")
