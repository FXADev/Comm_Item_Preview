"""
Salesforce Extractor Module

This module handles all interactions with Salesforce,
including authentication, SOQL query execution, and data transformation
to ensure SQL Server compatibility.
"""

import os
import logging
import pandas as pd
from simple_salesforce import Salesforce

# Import utilities
from utils.config_loader import load_query_from_file
from utils.data_transformers import transform_row_data, log_transformation_summary


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


def _process_salesforce_value(value):
    """
    Process individual Salesforce field values to handle nested objects and special types.
    
    Args:
        value: Raw value from Salesforce
        
    Returns:
        Processed value suitable for SQL insertion
    """
    if value is None:
        return None
    
    # Handle nested objects (relationships)
    if isinstance(value, dict):
        # If it has a 'records' key, it's a related list
        if 'records' in value:
            return f"{len(value['records'])} records"
        else:
            # Remove attributes and convert to string representation
            if 'attributes' in value:
                value.pop('attributes')
            return str(value) if value else None
    
    return value


def execute_salesforce_queries(config, batch_id, manual_mode=False):
    """
    Execute Salesforce SOQL queries defined in the configuration and return transformed results.
    
    Args:
        config (dict): Configuration dictionary containing Salesforce query settings
        batch_id (str): Batch ID for tracking
        manual_mode (bool): Whether the ETL is running in manual mode
        
    Returns:
        dict: Dictionary of query results with format:
            {query_name: {'data': transformed_rows, 'columns': column_names}}
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
                mock_data = [
                    ['001XX000001', 'Test Agency', 'BPT3__12345', '2024-05-20T10:30:00.000Z'],
                    ['001XX000002', 'Another Agency', 'BPT3__12346', '2024-05-21T11:45:00.000Z'],
                    ['001XX000003', 'Third Agency', 'BPT3__12347', '2024-05-22T09:15:00.000Z']
                ]
                columns = ['Id', 'Name', 'Agency_ID__c', 'CreatedDate']
                
                # Apply transformations to mock data
                transformed_data = []
                total_capped_values = 0
                for row in mock_data:
                    transformed_row, capped_count = transform_row_data(row, columns, 'salesforce')
                    transformed_data.append(transformed_row)
                    total_capped_values += capped_count
                
                results[query_name] = {
                    'data': transformed_data,
                    'columns': columns
                }
                log_transformation_summary(len(mock_data), len(transformed_data), 'Salesforce', query_name, total_capped_values)
            else:
                logging.info(f"Executing Salesforce query: {query_name}")
                # Execute query with pagination support
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
                
                # Extract columns and convert to list of lists
                if processed_records:
                    columns = list(processed_records[0].keys())
                    
                    # Convert to list of lists and process Salesforce-specific values
                    raw_data_rows = []
                    for record in processed_records:
                        row_data = []
                        for col in columns:
                            value = record.get(col)
                            processed_value = _process_salesforce_value(value)
                            row_data.append(processed_value)
                        raw_data_rows.append(row_data)
                    
                    logging.info(f"Salesforce query {query_name} returned {len(raw_data_rows)} raw rows")
                    
                    # Apply Salesforce-specific transformations
                    logging.info(f"Applying Salesforce-specific transformations to {query_name}")
                    transformed_data_rows = []
                    transformation_errors = 0
                    total_capped_values = 0
                    
                    for i, row in enumerate(raw_data_rows):
                        try:
                            transformed_row, capped_count = transform_row_data(row, columns, 'salesforce')
                            transformed_data_rows.append(transformed_row)
                            total_capped_values += capped_count
                        except Exception as e:
                            logging.error(f"Error transforming row {i} in {query_name}: {e}")
                            transformation_errors += 1
                            # Skip problematic rows instead of crashing
                            continue
                    
                    if transformation_errors > 0:
                        logging.warning(f"Skipped {transformation_errors} problematic rows during transformation in {query_name}")
                    
                    log_transformation_summary(len(raw_data_rows), len(transformed_data_rows), 'Salesforce', query_name, total_capped_values)
                    
                    results[query_name] = {
                        'data': transformed_data_rows,
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