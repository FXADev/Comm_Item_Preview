"""
SQL Extractor for SharePoint Upload

This module handles extracting commission data from SQL Server
using queries defined in the configuration and parameterized by Run_YM.
"""

import os
import sys
import logging
import pandas as pd
import pyodbc
from pathlib import Path
from string import Template

# Add parent directory to path for relative imports
sys.path.insert(0, str(Path(__file__).parent.absolute()))
from sp_utils import load_config

# Try to import the load_query_from_file function from the main project utils
try:
    sys.path.insert(0, str(Path(__file__).parent.parent.absolute()))
    from utils.config_loader import load_query_from_file as project_load_query
except ImportError:
    # If it fails, define our own version
    def project_load_query(file_path):
        """
        Load a query from a file.
        
        Args:
            file_path (str): Path to the query file
            
        Returns:
            str or None: Query content if successful, None otherwise
        """
        try:
            project_root = Path(__file__).parent.parent.absolute()
            absolute_path = os.path.join(project_root, file_path)
                
            with open(absolute_path, 'r') as f:
                query = f.read()
            logging.debug(f"Query loaded from {absolute_path}")
            return query
        except Exception as e:
            logging.error(f"Failed to load query from {file_path}: {e}")
            return None


def connect_to_sql():
    """
    Connect to SQL Server using credentials from environment variables.
    
    Returns:
        pyodbc.Connection: SQL Server connection object
    
    Raises:
        Exception: If connection fails
    """
    try:
        server = os.environ.get('SQL_SERVER')
        database = os.environ.get('SQL_DATABASE')
        username = os.environ.get('SQL_USERNAME')
        password = os.environ.get('SQL_PASSWORD')
        driver = os.environ.get('SQL_DRIVER', '{ODBC Driver 18 for SQL Server}')
        
        if not all([server, database, username, password]):
            raise ValueError("Missing SQL Server credentials in environment variables")
        
        connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(connection_string)
        logging.info(f"Connected to SQL Server {server}, database {database}")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to SQL Server: {e}")
        raise


def extract_commission_data(run_ym=None, conn=None, chunksize=50000):
    """
    Extract commission data from SQL Server using the query defined in config,
    parameterized by Run_YM. Uses chunking for memory efficiency with large datasets.
    
    Args:
        run_ym (str, optional): Year and month for the current run (format: YYYYMM)
        conn (pyodbc.Connection, optional): SQL Server connection object
        chunksize (int, optional): Number of rows to read at a time, default 50,000
        
    Returns:
        pandas.DataFrame: DataFrame containing commission data
    """
    try:
        # Load configuration
        config = load_config()
        
        # Use provided run_ym or get from config
        if run_ym is None:
            run_ym = config.get('run_ym')
            if not run_ym:
                raise ValueError("Run_YM not provided and not found in configuration")
        
        # Connect to SQL Server if not provided
        if conn is None:
            conn = connect_to_sql()
        
        # Find commission data query file path in config
        sql_query_file = None
        for query in config['sql']['queries']:
            if query['name'] == 'commission_data':
                sql_query_file = query['file']
                break
        
        if not sql_query_file:
            raise ValueError("Commission data query file not found in configuration")
        
        # Load the query from the file
        sql_query = project_load_query(sql_query_file)
        if not sql_query:
            raise ValueError(f"Failed to load SQL query from file: {sql_query_file}")
        
        # Parameterize the query with run_ym
        sql_template = Template(sql_query)
        parameterized_query = sql_template.safe_substitute(run_ym=run_ym)
        
        # Execute SQL query with chunking to manage memory usage
        logging.info(f"Executing SQL query for commission data with Run_YM={run_ym}")
        
        # Initialize an empty list to collect chunks
        chunks = []
        total_rows = 0
        chunk_count = 0
        
        try:
            # Read data in chunks to manage memory usage
            for chunk in pd.read_sql(parameterized_query, conn, chunksize=chunksize):
                chunk_count += 1
                chunk_size = len(chunk)
                total_rows += chunk_size
                chunks.append(chunk)
                logging.info(f"Loaded chunk {chunk_count} with {chunk_size} rows, total so far: {total_rows}")
                
                # Force garbage collection after processing each chunk
                import gc
                gc.collect()
                
            # Combine all chunks if any data was retrieved
            if chunks:
                # More memory-efficient concatenation using pd.concat with copy=False
                df = pd.concat(chunks, ignore_index=True, copy=False)
                logging.info(f"Combined {chunk_count} chunks into a single DataFrame with {len(df)} rows")
                
                # Free memory from the original chunks
                del chunks
                gc.collect()
                
                return df
            else:
                logging.warning(f"No commission data found for Run_YM={run_ym}")
                return pd.DataFrame()
        
        except MemoryError as me:
            logging.error(f"Memory error while processing commission data: {me}")
            logging.warning("Attempting to reduce memory usage by processing less data")
            
            # If we hit a memory error, try to at least return what we've processed so far
            if chunks:
                try:
                    # Try to concatenate what we have so far with reduced memory settings
                    df = pd.concat(chunks, ignore_index=True, copy=False)
                    logging.info(f"Partially recovered data: {len(df)} rows from {chunk_count} chunks")
                    return df
                except Exception as e:
                    logging.error(f"Could not recover partial data: {e}")
            
            return pd.DataFrame()
    except Exception as e:
        logging.error(f"Failed to extract commission data: {e}")
        return pd.DataFrame()


def store_execution_notification(run_ym, status, message, conn=None):
    """
    Store a notification about the execution in a SQL table.
    
    Args:
        run_ym (str): Year and month for the current run (format: YYYYMM)
        status (str): Execution status (e.g., 'Success', 'Failure', 'Partial')
        message (str): Notification message
        conn (pyodbc.Connection, optional): SQL Server connection object
        
    Returns:
        bool: True if the notification was stored successfully, False otherwise
    """
    try:
        # Connect to SQL Server if not provided
        should_close = False
        if conn is None:
            conn = connect_to_sql()
            should_close = True
        
        cursor = conn.cursor()
        
        # Check if the notification table exists, create it if it doesn't
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SharePointUploadNotifications')
        BEGIN
            CREATE TABLE SharePointUploadNotifications (
                Id INT IDENTITY(1,1) PRIMARY KEY,
                Run_YM VARCHAR(6) NOT NULL,
                Status VARCHAR(20) NOT NULL,
                Message NVARCHAR(MAX) NOT NULL,
                Created_At DATETIME NOT NULL DEFAULT GETDATE()
            )
        END
        """)
        conn.commit()
        
        # Insert the notification
        cursor.execute("""
        INSERT INTO SharePointUploadNotifications (Run_YM, Status, Message)
        VALUES (?, ?, ?)
        """, (run_ym, status, message))
        conn.commit()
        
        logging.info(f"Stored notification for Run_YM={run_ym}, Status={status}")
        
        # Close the connection if we opened it
        if should_close:
            conn.close()
        
        return True
    except Exception as e:
        logging.error(f"Failed to store execution notification: {e}")
        return False


if __name__ == "__main__":
    # For testing/development purposes
    logging.basicConfig(level=logging.INFO)
    try:
        conn = connect_to_sql()
        df = extract_commission_data('202505', conn)
        print(f"Retrieved {len(df)} commission records")
        print(df.head())
        
        # Test storing a notification
        store_execution_notification('202505', 'Test', 'Testing notification storage', conn)
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")
