"""
Salesforce Extractor Module

This module handles all interactions with Salesforce,
including authentication and SOQL query execution.
"""

import os
import logging
import pandas as pd
from simple_salesforce import Salesforce

# Import utilities
from utils.config_loader import load_query_from_file


def get_salesforce_connection(manual_mode=False):
    """
    Create a connection to Salesforce.
    
    Args:
        manual_mode (bool): Whether the ETL is running in manual mode
        
    Returns:
        Salesforce or None: Salesforce connection object if successful, None if manual mode
    """
    if manual_mode:
        logging.info("MANUAL MODE: Simulating Salesforce connection")
        return None
        
    try:
        username = os.getenv('SF_USERNAME')
        password = os.getenv('SF_PASSWORD')
        security_token = os.getenv('SF_SECURITY_TOKEN')
        
        logging.info(f"Connecting to Salesforce as: {username}")
        
        sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token
        )
        
        logging.info("Connected to Salesforce successfully")
        return sf
    except Exception as e:
        logging.error(f"Failed to connect to Salesforce: {e}")
        raise


def execute_salesforce_queries(config, batch_id, manual_mode=False):
    """
    Execute Salesforce SOQL queries defined in the configuration and return results.
    
    Args:
        config (dict): Configuration dictionary containing Salesforce query settings
        batch_id (str): Batch ID for tracking
        manual_mode (bool): Whether the ETL is running in manual mode
        
    Returns:
        dict: Dictionary of query results with format:
            {query_name: {'data': rows, 'columns': column_names}}
    """
    results = {}
    
    if 'salesforce' not in config or 'queries' not in config['salesforce']:
        logging.warning("No Salesforce queries defined in configuration")
        return results
    
    sf = None
    try:
        if not manual_mode:
            sf = get_salesforce_connection()
        
        for query_config in config['salesforce']['queries']:
            query_name = query_config['name']
            query_file = query_config['file']
            
            logging.info(f"Processing Salesforce query: {query_name} from {query_file}")
            
            # Load SOQL from file
            soql = load_query_from_file(query_file)
            if not soql:
                logging.error(f"Failed to load SOQL from {query_file}")
                continue
            
            if manual_mode:
                logging.info(f"MANUAL MODE: Would execute Salesforce query: {query_name}")
                # Create mock data for simulation
                results[query_name] = {
                    'data': [['mock_sf_data']] * 5,  # 5 rows of mock data
                    'columns': ['Id', 'Name', 'CreatedDate']
                }
            else:
                logging.info(f"Executing Salesforce query: {query_name}")
                # Execute query
                sf_result = sf.query_all(soql)
                records = sf_result.get('records', [])
                
                if not records:
                    logging.warning(f"Salesforce query {query_name} returned no records")
                    results[query_name] = {
                        'data': [],
                        'columns': []
                    }
                    continue
                
                # Process records to standardize format (remove Salesforce metadata)
                processed_records = []
                for record in records:
                    # Remove Salesforce metadata attributes
                    record.pop('attributes', None)
                    processed_records.append(record)
                
                # Extract columns (all records should have the same structure)
                if processed_records:
                    columns = list(processed_records[0].keys())
                    
                    # Convert to list of lists for bulk insert (with column order preserved)
                    data_rows = []
                    for record in processed_records:
                        # Handle nested objects by flattening them to strings
                        row_data = []
                        for col in columns:
                            value = record.get(col)
                            # If the value is a dict (nested object), convert to string representation
                            if isinstance(value, dict):
                                # If it has a 'records' key, it's a related list
                                if 'records' in value:
                                    row_data.append(str(len(value['records'])) + " records")
                                else:
                                    # Remove attributes and convert to string
                                    if 'attributes' in value:
                                        value.pop('attributes')
                                    row_data.append(str(value))
                            else:
                                row_data.append(value)
                        
                        data_rows.append(row_data)
                    
                    logging.info(f"Salesforce query {query_name} returned {len(data_rows)} rows")
                    
                    results[query_name] = {
                        'data': data_rows,
                        'columns': columns
                    }
                else:
                    logging.warning(f"No valid records found in Salesforce query {query_name}")
                    results[query_name] = {
                        'data': [],
                        'columns': []
                    }
        
        return results
    
    except Exception as e:
        logging.error(f"Error executing Salesforce queries: {e}")
        return {}
    
    finally:
        # No need to explicitly close Salesforce connection
        pass
