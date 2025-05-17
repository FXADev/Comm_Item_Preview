"""
Redshift Extractor for SharePoint Upload

This module handles extracting agency group information from Redshift
using queries defined in the configuration.
"""

import os
import sys
import logging
import pandas as pd
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


def connect_to_redshift():
    """
    Connect to Redshift using credentials from environment variables.
    
    Returns:
        psycopg2.connection: Redshift connection object
    
    Raises:
        Exception: If connection fails
    """
    try:
        import psycopg2
        
        host = os.environ.get('REDSHIFT_HOST')
        port = os.environ.get('REDSHIFT_PORT', '5439')
        database = os.environ.get('REDSHIFT_DATABASE')
        user = os.environ.get('REDSHIFT_USER')
        password = os.environ.get('REDSHIFT_PASSWORD')
        schema = os.environ.get('REDSHIFT_SCHEMA', 'public')
        
        if not all([host, database, user, password]):
            raise ValueError("Missing Redshift credentials in environment variables")
        
        connection_string = f"host={host} port={port} dbname={database} user={user} password={password}"
        conn = psycopg2.connect(connection_string)
        
        # Set search path to schema
        with conn.cursor() as cursor:
            cursor.execute(f"SET search_path TO {schema}")
        
        logging.info(f"Connected to Redshift {host}, database {database}, schema {schema}")
        return conn
    except ImportError:
        logging.error("Failed to import psycopg2. Make sure it's installed.")
        raise
    except Exception as e:
        logging.error(f"Failed to connect to Redshift: {e}")
        raise


def extract_agency_groups(chunksize=50000):
    """
    Extract agency group information from Redshift using memory-efficient chunking.
    
    Args:
        chunksize (int, optional): Number of rows to read at a time, default 50,000
    
    Returns:
        pandas.DataFrame: DataFrame containing agency group information with
                         properly formatted RPM_Agency_ID
    """
    try:
        # Load configuration
        config = load_config(Path(__file__).parent / 'sp_config.yml')
        
        # Connect to Redshift
        conn = connect_to_redshift()
        
        # Find agency group query file path in config
        query_file = None
        for query in config.get('redshift', {}).get('queries', []):
            if query['name'] == 'agency_groups':
                query_file = query['file']
                break
        
        if not query_file:
            raise ValueError("Agency groups query file not found in configuration")
        
        # Load the query from the file
        query = project_load_query(query_file)
        if not query:
            raise ValueError(f"Failed to load Redshift query from file: {query_file}")
        
        # Execute query with chunking for memory efficiency
        logging.info(f"Executing Redshift query for agency groups")
        
        # Initialize empty list to collect chunks
        chunks = []
        total_rows = 0
        chunk_count = 0
        
        try:
            # Read data in chunks to manage memory usage
            for chunk in pd.read_sql(query, conn, chunksize=chunksize):
                chunk_count += 1
                chunk_size = len(chunk)
                total_rows += chunk_size
                chunks.append(chunk)
                logging.info(f"Loaded chunk {chunk_count} with {chunk_size} rows, total so far: {total_rows}")
                
                # Force garbage collection after processing each chunk
                import gc
                gc.collect()
            
            # Close connection when done
            conn.close()
            
            # Combine all chunks if any data was retrieved
            if not chunks:
                logging.warning("No agency group records returned from Redshift")
                return pd.DataFrame()
                
            # Memory-efficient concatenation
            df = pd.concat(chunks, ignore_index=True, copy=False)
            logging.info(f"Combined {chunk_count} chunks into a single DataFrame with {len(df)} rows")
            
            # Free memory from chunks
            del chunks
            gc.collect()
            
        except MemoryError as me:
            # Handle out-of-memory conditions
            logging.error(f"Memory error while processing agency group data: {me}")
            logging.warning("Attempting to recover with partial data")
            
            # Close the connection
            conn.close()
            
            # Try to save what we have
            if chunks:
                try:
                    # Try to concatenate available chunks with reduced memory settings
                    df = pd.concat(chunks, ignore_index=True, copy=False)
                    logging.info(f"Partially recovered data: {len(df)} rows from {chunk_count} chunks")
                except Exception as e:
                    logging.error(f"Could not recover partial data: {e}")
                    return pd.DataFrame()
            else:
                return pd.DataFrame()
        
        # Convert RPM_Agency_ID to string and add prefix
        df['RPM_Agency_ID'] = df['rpm_agency_id'].astype(str).apply(lambda x: f"bpt3__{x}")
        
        # Drop the original column to avoid confusion
        if 'rpm_agency_id' in df.columns and 'RPM_Agency_ID' in df.columns:
            df = df.drop('rpm_agency_id', axis=1)
            
        # Rename other columns to match our convention
        column_mapping = {
            'agency_name': 'Agency_Name',
            'agency_group': 'Agency_Group'
        }
        df = df.rename(columns=column_mapping)
        
        logging.info(f"Retrieved {len(df)} agency group records from Redshift")
        return df
    
    except Exception as e:
        logging.error(f"Failed to extract agency groups from Redshift: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    # For testing/development purposes
    logging.basicConfig(level=logging.INFO)
    try:
        df = extract_agency_groups()
        print(f"Retrieved {len(df)} agency group records")
        print(df.head())
    except Exception as e:
        print(f"Error: {e}")
