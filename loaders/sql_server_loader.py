"""
SQL Server Loader Module

This module handles all operations related to loading data into SQL Server,
including bulk inserts and table preparation.
"""

import logging
import pandas as pd
import pyodbc
from datetime import datetime
from decimal import Decimal, InvalidOperation
import time


def insert_to_sql_table(conn, table_name, data, columns, batch_id, batch_size=5000):
    """
    Insert data into a SQL Server table using optimized bulk loading.
    
    Args:
        conn: SQL Server connection
        table_name: Target table name
        data: List of rows to insert
        columns: List of column names
        batch_id: Batch ID for tracking
        batch_size: Batch size for inserts (increased for performance)
    
    Returns:
        int: Number of rows inserted
    """
    if not data or not columns:
        logging.warning(f"No data to insert into {table_name}")
        return 0
        
    try:
        cursor = conn.cursor()
        row_count = 0
        
        # Add standard audit columns
        columns.extend(['etl_batch_id', 'extracted_at'])
        
        # Create placeholders for the SQL query
        placeholders = ', '.join(['?' for _ in range(len(columns))])
        
        # Create INSERT SQL template
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Format timestamp properly for SQL Server
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Optimize by preparing all data upfront instead of per batch
        start_prep = datetime.now()
        logging.info(f"Preparing {len(data)} rows for insertion into {table_name}")
        
        # Pre-process all data at once
        modified_data = []
        for row in data:
            # Convert tuple to list if needed
            row_list = list(row) if isinstance(row, tuple) else row[:]
            
            # Fix decimal precision issues
            for i, val in enumerate(row_list):
                row_list[i] = _handle_decimal_value(val)
                
            # Add audit columns
            row_list.append(batch_id)  # etl_batch_id
            row_list.append(current_timestamp)  # extracted_at
            modified_data.append(row_list)
        
        prep_time = (datetime.now() - start_prep).total_seconds()
        logging.info(f"Data preparation completed in {prep_time:.2f} seconds")
        
        # Enable fast loading
        cursor.fast_executemany = True
        
        # Insert in batches for better performance
        start_time = datetime.now()
        total_rows = len(modified_data)
        
        for i in range(0, total_rows, batch_size):
            batch_end = min(i + batch_size, total_rows)
            batch = modified_data[i:batch_end]
            
            try:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                row_count += len(batch)
                
                # Only log every 20,000 rows to reduce log overhead
                if i % 20000 == 0 or batch_end == total_rows:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = (i + len(batch)) / elapsed if elapsed > 0 else 0
                    logging.info(f"Inserted {i + len(batch):,} of {total_rows:,} rows into {table_name} ({(i + len(batch))/total_rows*100:.1f}%) - {rate:.1f} rows/sec")
            except Exception as e:
                # For debugging: log the data that caused the issue
                logging.error(f"Error during batch insert to {table_name}: {e}")
                
                # Log sample of problematic batch for debugging
                logging.error(f"Batch size: {len(batch)}, Sample row: {str(batch[0] if batch else 'No data')[:200]}")
                
                # Check for problematic values in the batch
                _debug_batch_values(batch, table_name)
                
                # Switch to row-by-row insert for error handling
                logging.info("Switching to row-by-row insert for error recovery")
                conn.rollback()
                
                for j, single_row in enumerate(batch):
                    try:
                        cursor.execute(insert_sql, single_row)
                        conn.commit()
                        row_count += 1
                    except Exception as row_error:
                        conn.rollback()
                        logging.error(f"Error inserting row {i+j}: {row_error}")
                        # Log problematic data for diagnostics
                        _debug_single_row(single_row, columns, i+j)
        
        # Log performance statistics
        total_time = (datetime.now() - start_time).total_seconds()
        rate = total_rows / total_time if total_time > 0 else 0
        logging.info(f"Completed inserting {total_rows:,} rows into {table_name} in {total_time:.2f} seconds ({rate:.1f} rows/sec)")
        
        logging.info(f"Total of {row_count} rows inserted into {table_name}")
        return row_count
        
    except Exception as e:
        logging.error(f"Failed to insert data into {table_name}: {e}")
        conn.rollback()
        return 0


def _debug_batch_values(batch, table_name):
    """Debug helper to identify problematic values in a batch."""
    try:
        logging.error(f"Debugging batch values for {table_name}")
        
        # Check first few rows for problematic values
        for i, row in enumerate(batch[:3]):  # Check first 3 rows
            for j, val in enumerate(row):
                if isinstance(val, (int, float, Decimal)):
                    if isinstance(val, float):
                        if not (val == val):  # NaN
                            logging.error(f"Found NaN at row {i}, column {j}")
                        elif val == float('inf') or val == float('-inf'):
                            logging.error(f"Found infinite value {val} at row {i}, column {j}")
                        elif abs(val) > 999999999999999.99:
                            logging.error(f"Found out-of-range value {val} at row {i}, column {j}")
                    elif isinstance(val, Decimal):
                        if abs(val) > Decimal('999999999999999.99'):
                            logging.error(f"Found out-of-range Decimal {val} at row {i}, column {j}")
    except Exception as debug_error:
        logging.error(f"Error during batch debugging: {debug_error}")


def _debug_single_row(row, columns, row_index):
    """Debug helper to identify problematic values in a single row."""
    try:
        logging.error(f"Debugging row {row_index}")
        for i, (col_name, val) in enumerate(zip(columns, row)):
            if isinstance(val, (int, float, Decimal)):
                val_info = f"Column '{col_name}' (index {i}): {val} (type: {type(val).__name__})"
                if isinstance(val, float):
                    if not (val == val):  # NaN
                        logging.error(f"NaN found - {val_info}")
                    elif val == float('inf') or val == float('-inf'):
                        logging.error(f"Infinite value found - {val_info}")
                    elif abs(val) > 999999999999999.99:
                        logging.error(f"Out-of-range value found - {val_info}")
                elif isinstance(val, Decimal):
                    if abs(val) > Decimal('999999999999999.99'):
                        logging.error(f"Out-of-range Decimal found - {val_info}")
            elif val is None:
                continue  # NULL values are fine
            else:
                # Log non-numeric values that might cause issues
                if len(str(val)) > 100:  # Very long strings
                    logging.error(f"Very long value in column '{col_name}': {str(val)[:100]}...")
    except Exception as debug_error:
        logging.error(f"Error during single row debugging: {debug_error}")


def _handle_decimal_value(val):
    """
    Handle decimal/numeric values to ensure SQL Server compatibility.
    
    Args:
        val: The value to process
        
    Returns:
        Processed value safe for SQL Server insertion
    """
    # Handle None/NULL values
    if val is None:
        return None
    
    # Handle pandas NaT and NaN first
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        # pd.isna might fail on some types, continue with other checks
        pass
    
    # Handle Decimal and float values
    if isinstance(val, (Decimal, float, int)):
        try:
            # Convert to float first for consistent handling
            if isinstance(val, Decimal):
                float_val = float(val)
            elif isinstance(val, int):
                float_val = float(val)
            else:
                float_val = val
            
            # Check for special float values
            if not (float_val == float_val):  # NaN check (NaN != NaN)
                logging.warning(f"NaN value encountered, setting to NULL")
                return None
            if float_val == float('inf') or float_val == float('-inf'):
                logging.warning(f"Infinite value {float_val} encountered, setting to NULL")
                return None
            
            # SQL Server numeric constraints - be more conservative
            # Using smaller limits to ensure compatibility
            max_value = 999999999999999.99  # 15 digits before decimal
            min_value = -999999999999999.99
            
            if float_val > max_value:
                logging.warning(f"Value {float_val} exceeds maximum limit, capping at {max_value}")
                return max_value
            elif float_val < min_value:
                logging.warning(f"Value {float_val} below minimum limit, capping at {min_value}")
                return min_value
            else:
                # Round to 2 decimal places
                return round(float_val, 2)
                
        except (InvalidOperation, ValueError, OverflowError, TypeError) as e:
            logging.warning(f"Error handling numeric value {val} (type: {type(val)}): {str(e)}. Setting to NULL.")
            return None
    
    # Handle date/time objects by converting to string
    elif isinstance(val, (datetime, pd.Timestamp)):
        try:
            # Check for pandas NaT
            if pd.isna(val):
                return None
            return val.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logging.warning(f"Error formatting datetime {val}: {str(e)}. Setting to NULL.")
            return None
    
    # Handle string representations of numbers
    elif isinstance(val, str):
        # Check if it's a numeric string
        try:
            float_val = float(val)
            # Recursively handle the converted float
            return _handle_decimal_value(float_val)
        except (ValueError, TypeError):
            # Not a numeric string, return as-is
            return val
        
    # Return value as-is for other types
    return val


def load_data_to_sql_server(sql_conn, data_results, batch_id, source_type='redshift'):
    """
    Load extracted data to SQL Server staging tables.
    
    Args:
        sql_conn: SQL Server connection
        data_results: Dictionary of query results from extractors
        batch_id: Batch ID for tracking
        source_type: Source type ('redshift' or 'salesforce') to determine table name prefix
    
    Returns:
        dict: Summary of load operations with format:
            {table_name: rows_inserted}
    """
    if not data_results:
        logging.warning(f"No {source_type} data to load to SQL Server")
        return {}
    
    load_summary = {}
    prefix = "dbo.stg_" if source_type == 'redshift' else "dbo.stg_sf_"
    
    for query_name, result in data_results.items():
        target_table = f"{prefix}{query_name}"
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
            load_summary[target_table] = rows_inserted
        except Exception as e:
            logging.error(f"Error loading data to {target_table}: {e}")
            load_summary[target_table] = 0
    
    return load_summary