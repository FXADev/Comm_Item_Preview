"""
SQL Server Loader Module

This module handles all operations related to loading data into SQL Server,
focusing purely on efficient bulk insertion. Data transformation is handled
by the extractors to ensure clean data reaches this layer.
"""

import logging
import pyodbc
from datetime import datetime


def insert_to_sql_table(conn, table_name, data, columns, batch_id, batch_size=2000):
    """
    Insert pre-transformed data into a SQL Server table using optimized bulk loading.
    
    Args:
        conn: SQL Server connection
        table_name: Target table name
        data: List of pre-transformed rows to insert
        columns: List of column names
        batch_id: Batch ID for tracking
        batch_size: Batch size for inserts (optimized for stability)
    
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
        
        # Prepare data for insertion (data should already be transformed by extractors)
        start_prep = datetime.now()
        logging.info(f"Preparing {len(data)} pre-transformed rows for insertion into {table_name}")
        
        # Add audit columns to each row (data should already be transformed by extractors)
        modified_data = []
        for row in data:
            # Convert tuple to list if needed and create a copy
            row_list = list(row) if isinstance(row, tuple) else row[:]
            
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
                
                # Log progress every 20,000 rows or at completion
                if i % 20000 == 0 or batch_end == total_rows:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = (i + len(batch)) / elapsed if elapsed > 0 else 0
                    logging.info(f"Inserted {i + len(batch):,} of {total_rows:,} rows into {table_name} ({(i + len(batch))/total_rows*100:.1f}%) - {rate:.1f} rows/sec")
            
            except Exception as e:
                # Log the error and try smaller sub-batches
                logging.error(f"Error during batch insert to {table_name}: {e}")
                logging.error(f"Failed batch: rows {i} to {batch_end-1}")
                conn.rollback()
                
                # Try smaller sub-batches for problematic batches
                sub_batch_success = False
                if len(batch) > 50:  # Only try sub-batches for larger batches
                    logging.info("Attempting smaller sub-batches of 25 rows each")
                    sub_batch_size = 25
                    
                    for sub_i in range(0, len(batch), sub_batch_size):
                        sub_batch_end = min(sub_i + sub_batch_size, len(batch))
                        sub_batch = batch[sub_i:sub_batch_end]
                        try:
                            cursor.executemany(insert_sql, sub_batch)
                            conn.commit()
                            row_count += len(sub_batch)
                            sub_batch_success = True
                        except Exception as sub_e:
                            logging.error(f"Sub-batch {sub_i}-{sub_batch_end} failed: {sub_e}")
                            conn.rollback()
                            
                            # For very small batches that still fail, log sample data for debugging
                            if len(sub_batch) <= 5:
                                logging.error(f"Sample of problematic data: {str(sub_batch[0])[:200] if sub_batch else 'No data'}")
                            continue
                
                if not sub_batch_success:
                    logging.warning(f"Unable to insert batch of {len(batch)} rows, skipping entirely")
                else:
                    logging.info(f"Successfully recovered {len(batch)} rows using sub-batches")
        
        # Log performance statistics
        total_time = (datetime.now() - start_time).total_seconds()
        rate = total_rows / total_time if total_time > 0 else 0
        logging.info(f"Completed inserting {total_rows:,} rows into {table_name} in {total_time:.2f} seconds ({rate:.1f} rows/sec)")
        
        logging.info(f"Total of {row_count} rows successfully inserted into {table_name}")
        return row_count
        
    except Exception as e:
        logging.error(f"Failed to insert data into {table_name}: {e}")
        conn.rollback()
        return 0


def load_data_to_sql_server(sql_conn, data_results, batch_id, source_type='redshift'):
    """
    Load extracted and pre-transformed data to SQL Server staging tables.
    
    Args:
        sql_conn: SQL Server connection
        data_results: Dictionary of query results from extractors (pre-transformed)
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
        logging.info(f"Loading pre-transformed data to {target_table}")
        
        if not result.get('data'):
            logging.warning(f"No data to load for {target_table}")
            load_summary[target_table] = 0
            continue
        
        try:
            rows_inserted = insert_to_sql_table(
                sql_conn,
                target_table,
                result['data'],
                result['columns'],
                batch_id
            )
            logging.info(f"Successfully loaded {rows_inserted} rows into {target_table}")
            load_summary[target_table] = rows_inserted
        except Exception as e:
            logging.error(f"Error loading data to {target_table}: {e}")
            load_summary[target_table] = 0
    
    return load_summary