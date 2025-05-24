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


def insert_to_sql_table(conn, table_name, data, columns, batch_id, batch_size=2000):
    """
    Insert data into a SQL Server table using optimized bulk loading.
    
    Args:
        conn: SQL Server connection
        table_name: Target table name
        data: List of rows to insert
        columns: List of column names
        batch_id: Batch ID for tracking
        batch_size: Batch size for inserts (reduced for stability)
    
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
        validation_errors = 0
        
        for row_idx, row in enumerate(data):
            # Convert tuple to list if needed
            row_list = list(row) if isinstance(row, tuple) else row[:]
            
            # Fix decimal precision issues and validate
            valid_row = True
            for i, val in enumerate(row_list):
                processed_val = _handle_decimal_value(val)
                
                # Additional validation for SQL Server compatibility
                if processed_val is not None and isinstance(processed_val, (int, float)):
                    # Final safety check for numeric values
                    if abs(processed_val) > 9999999999999.99:
                        logging.warning(f"Row {row_idx}: Value {processed_val} still out of range after processing, setting to NULL")
                        processed_val = None
                        validation_errors += 1
                
                row_list[i] = processed_val
                
            # Add audit columns
            row_list.append(batch_id)  # etl_batch_id
            row_list.append(current_timestamp)  # extracted_at
            modified_data.append(row_list)
        
        if validation_errors > 0:
            logging.warning(f"Fixed {validation_errors} out-of-range values during preprocessing")
        
        # Quick test to identify any remaining problematic values
        logging.info("Checking first 5 rows for potential numeric issues...")
        for row_idx, row in enumerate(modified_data[:5]):  # Check first 5 rows
            for col_idx, val in enumerate(row):
                if isinstance(val, (int, float)) and val is not None:
                    if abs(val) > 9999999999999.99:
                        col_name = columns[col_idx] if col_idx < len(columns) else f"column_{col_idx}"
                        logging.error(f"Row {row_idx}, Column '{col_name}' (index {col_idx}): {val} - still too large after processing!")
                    elif isinstance(val, float):
                        if not (val == val):  # NaN check
                            col_name = columns[col_idx] if col_idx < len(columns) else f"column_{col_idx}"
                            logging.error(f"Row {row_idx}, Column '{col_name}' (index {col_idx}): NaN detected!")
                        elif val == float('inf') or val == float('-inf'):
                            col_name = columns[col_idx] if col_idx < len(columns) else f"column_{col_idx}"
                            logging.error(f"Row {row_idx}, Column '{col_name}' (index {col_idx}): Infinite value {val} detected!")
        
        logging.info("Pre-insertion validation complete")
        
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
                # Log the error but don't attempt row-by-row recovery to avoid segfaults
                logging.error(f"Error during batch insert to {table_name}: {e}")
                logging.error(f"Failed batch: rows {i} to {batch_end-1}")
                
                # Skip this batch and continue with next batch instead of row-by-row
                # This prevents segmentation faults and memory issues
                conn.rollback()
                logging.warning(f"Skipping batch of {len(batch)} rows due to data issues")
                
                # Optionally try smaller sub-batches (safer approach)
                sub_batch_success = False
                if len(batch) > 100:  # Only try sub-batches for larger batches
                    logging.info("Attempting smaller sub-batches")
                    sub_batch_size = 50
                    for sub_i in range(0, len(batch), sub_batch_size):
                        sub_batch_end = min(sub_i + sub_batch_size, len(batch))
                        sub_batch = batch[sub_i:sub_batch_end]
                        try:
                            cursor.executemany(insert_sql, sub_batch)
                            conn.commit()
                            row_count += len(sub_batch)
                            sub_batch_success = True
                        except Exception as sub_e:
                            logging.error(f"Sub-batch {sub_i}-{sub_batch_end} also failed: {sub_e}")
                            conn.rollback()
                            # Skip this sub-batch entirely
                            continue
                
                if not sub_batch_success and len(batch) > 100:
                    logging.warning(f"All sub-batches failed, skipping {len(batch)} rows entirely")
                elif len(batch) <= 100:
                    logging.warning(f"Small batch of {len(batch)} rows failed, skipping entirely")
        
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
                # Handle very large Decimal values that might cause float overflow
                str_val = str(val)
                if len(str_val.replace('.', '').replace('-', '')) > 15:
                    logging.warning(f"Very large Decimal value {val}, setting to NULL")
                    return None
                float_val = float(val)
            elif isinstance(val, int):
                # Handle very large integers
                if abs(val) > 999999999999999:
                    logging.warning(f"Very large integer {val}, setting to NULL")
                    return None
                float_val = float(val)
            else:
                float_val = val
            
            # Check for special float values
            if not (float_val == float_val):  # NaN check (NaN != NaN)
                return None
            if float_val == float('inf') or float_val == float('-inf'):
                return None
            
            # Very conservative SQL Server limits - use decimal(15,2) instead of (18,2)
            # This prevents the numeric out of range errors
            max_value = 9999999999999.99   # 13 digits before decimal
            min_value = -9999999999999.99
            
            if abs(float_val) > max_value:
                return None  # Set to NULL instead of capping to avoid any issues
            else:
                # Round to 2 decimal places
                rounded_val = round(float_val, 2)
                # Double-check after rounding
                if abs(rounded_val) > max_value:
                    return None
                return rounded_val
                
        except (InvalidOperation, ValueError, OverflowError, TypeError) as e:
            return None
    
    # Handle date/time objects by converting to string
    elif isinstance(val, (datetime, pd.Timestamp)):
        try:
            # Check for pandas NaT
            if pd.isna(val):
                return None
            return val.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            return None
    
    # Handle string representations of numbers
    elif isinstance(val, str):
        # Check if it's a numeric string
        try:
            # Avoid processing very long strings that might be large numbers
            if len(val) > 20:
                return val  # Return as string, don't try to convert
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