"""
SQL Server Loader Module

This module handles all operations related to loading data into SQL Server,
including bulk inserts and table preparation.
"""

import logging
import pandas as pd
import pyodbc
from datetime import datetime


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
        columns.extend(['batch_id', 'insert_ts'])
        
        # Create placeholders for the SQL query
        placeholders = ', '.join(['?' for _ in range(len(columns))])
        
        # Create INSERT SQL template
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Prepare batches for bulk insert
        rows_to_insert = []
        current_timestamp = datetime.now()
        
        for row in data:
            # Add audit columns to row
            row_with_audit = row.copy() if isinstance(row, list) else row
            if isinstance(row_with_audit, list):
                row_with_audit.extend([batch_id, current_timestamp])
            else:
                # Handle case where row might be a dictionary
                row_with_audit['batch_id'] = batch_id
                row_with_audit['insert_ts'] = current_timestamp
                
            rows_to_insert.append(row_with_audit)
            
            # Execute in batches for better performance
            if len(rows_to_insert) >= batch_size:
                try:
                    # Using executemany for bulk insert
                    cursor.fast_executemany = True
                    cursor.executemany(insert_sql, rows_to_insert)
                    conn.commit()
                    row_count += len(rows_to_insert)
                    logging.debug(f"Inserted batch of {len(rows_to_insert)} rows into {table_name}")
                    rows_to_insert = []
                except Exception as e:
                    # For debugging: log the data that caused the issue
                    logging.error(f"Error during batch insert to {table_name}: {e}")
                    
                    # Switch to row-by-row insert for error handling
                    logging.info("Switching to row-by-row insert for error recovery")
                    conn.rollback()
                    
                    for i, single_row in enumerate(rows_to_insert):
                        try:
                            cursor.execute(insert_sql, single_row)
                            conn.commit()
                            row_count += 1
                        except Exception as row_error:
                            conn.rollback()
                            logging.error(f"Error inserting row {i}: {row_error}")
                            # Log problematic data for diagnostics (first 200 chars)
                            logging.error(f"Problematic row data (truncated): {str(single_row)[:200]}")
                    
                    rows_to_insert = []
        
        # Insert any remaining rows
        if rows_to_insert:
            try:
                cursor.fast_executemany = True
                cursor.executemany(insert_sql, rows_to_insert)
                conn.commit()
                row_count += len(rows_to_insert)
            except Exception as e:
                logging.error(f"Error during final batch insert to {table_name}: {e}")
                
                # Switch to row-by-row insert for remaining rows
                conn.rollback()
                for i, single_row in enumerate(rows_to_insert):
                    try:
                        cursor.execute(insert_sql, single_row)
                        conn.commit()
                        row_count += 1
                    except Exception as row_error:
                        conn.rollback()
                        logging.error(f"Error inserting row {i}: {row_error}")
        
        logging.info(f"Total of {row_count} rows inserted into {table_name}")
        return row_count
        
    except Exception as e:
        logging.error(f"Failed to insert data into {table_name}: {e}")
        conn.rollback()
        return 0


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
