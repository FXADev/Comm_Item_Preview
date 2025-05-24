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
                        # Log problematic data for diagnostics (first 200 chars)
                        logging.error(f"Problematic row data (truncated): {str(single_row)[:200]}")
        
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
    
    # Handle Decimal and float values
    if isinstance(val, (Decimal, float)):
        try:
            # Convert to Decimal for precise handling
            if isinstance(val, float):
                # Check for special float values
                if not (val == val):  # NaN check (NaN != NaN)
                    logging.warning(f"NaN value encountered, setting to NULL")
                    return None
                if val == float('inf') or val == float('-inf'):
                    logging.warning(f"Infinite value {val} encountered, setting to NULL")
                    return None
                    
                # Convert float to Decimal for precise handling
                decimal_val = Decimal(str(val))
            else:
                decimal_val = val
            
            # SQL Server decimal(18,2) limits
            max_value = Decimal('9999999999999999.99')
            min_value = Decimal('-9999999999999999.99')
            
            if decimal_val > max_value:
                logging.warning(f"Value {decimal_val} exceeds maximum decimal(18,2) limit, capping at {max_value}")
                return float(max_value)
            elif decimal_val < min_value:
                logging.warning(f"Value {decimal_val} below minimum decimal(18,2) limit, capping at {min_value}")
                return float(min_value)
            else:
                # Round to 2 decimal places and convert to float for SQL Server
                rounded_val = decimal_val.quantize(Decimal('0.01'))
                return float(rounded_val)
                
        except (InvalidOperation, ValueError, OverflowError) as e:
            logging.warning(f"Error handling decimal value {val}: {str(e)}. Setting to NULL.")
            return None
    
    # Handle date/time objects by converting to string
    elif isinstance(val, (datetime, pd.Timestamp)):
        try:
            # Format as SQL Server compatible datetime
            if pd.isna(val):
                return None
            return val.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logging.warning(f"Error formatting datetime {val}: {str(e)}. Setting to NULL.")
            return None
    
    # Handle pandas NaT and NaN
    elif pd.isna(val):
        return None
        
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