"""
Data Transformation Utilities

This module provides utilities for transforming data from various sources
to ensure SQL Server compatibility, particularly for numeric and date values.
"""

import logging
import pandas as pd
from datetime import datetime
from decimal import Decimal, InvalidOperation


def transform_numeric_value(val, source_type='generic', field_name='unknown'):
    """
    Transform numeric values to ensure SQL Server compatibility.
    
    Args:
        val: The value to process
        source_type: Source system ('redshift', 'salesforce', 'generic')
        field_name: Name of the field for better error messages
        
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
    
    # Handle numeric values
    if isinstance(val, (Decimal, float, int)):
        try:
            # Convert to float first for consistent handling
            if isinstance(val, Decimal):
                # Handle very large Decimal values that might cause float overflow
                str_val = str(val)
                if len(str_val.replace('.', '').replace('-', '')) > 15:
                    logging.warning(f"{source_type} field '{field_name}': Very large Decimal value {val}, setting to NULL")
                    return None
                float_val = float(val)
            elif isinstance(val, int):
                # Handle very large integers
                if abs(val) > 999999999999999:
                    logging.warning(f"{source_type} field '{field_name}': Very large integer {val}, setting to NULL")
                    return None
                float_val = float(val)
            else:
                float_val = val
            
            # Check for special float values
            if not (float_val == float_val):  # NaN check (NaN != NaN)
                logging.debug(f"{source_type} field '{field_name}': NaN value converted to NULL")
                return None
            if float_val == float('inf') or float_val == float('-inf'):
                logging.debug(f"{source_type} field '{field_name}': Infinite value converted to NULL")
                return None
            
            # Apply source-specific limits - made more conservative
            if source_type == 'redshift':
                # Redshift often has large financial amounts - but be more conservative
                max_value = 999999999999.99    # 12 digits before decimal (was 13)
            elif source_type == 'salesforce':
                # Salesforce typically has smaller currency values
                max_value = 999999999.99       # 9 digits before decimal
            else:
                # Generic/conservative limit
                max_value = 999999999999.99    # 12 digits before decimal
            
            min_value = -max_value
            
            if abs(float_val) > max_value:
                logging.warning(f"{source_type} field '{field_name}': Value {float_val} exceeds limits, setting to NULL")
                return None
            else:
                # Round to 2 decimal places for currency fields, 4 for rates
                if any(keyword in field_name.lower() for keyword in ['rate', 'split']):
                    # Rate fields - keep 4 decimal places
                    return round(float_val, 4)
                else:
                    # Currency fields - keep 2 decimal places
                    return round(float_val, 2)
                
        except (InvalidOperation, ValueError, OverflowError, TypeError) as e:
            logging.warning(f"{source_type} field '{field_name}': Error processing numeric value {val}: {str(e)}. Setting to NULL.")
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
            return transform_numeric_value(float_val, source_type, field_name)
        except (ValueError, TypeError):
            # Not a numeric string, return as-is
            return val
        
    # Return value as-is for other types
    return val


def transform_datetime_value(val, source_type='generic', field_name='unknown'):
    """
    Transform datetime values to ensure SQL Server compatibility.
    
    Args:
        val: The value to process
        source_type: Source system ('redshift', 'salesforce', 'generic')
        field_name: Name of the field for better error messages
        
    Returns:
        Processed datetime string or None
    """
    if val is None:
        return None
        
    # Handle pandas NaT
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    
    # Handle datetime objects
    if isinstance(val, (datetime, pd.Timestamp)):
        try:
            return val.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logging.warning(f"{source_type} field '{field_name}': Error formatting datetime {val}: {str(e)}. Setting to NULL.")
            return None
    
    # Handle string dates (particularly from Salesforce)
    elif isinstance(val, str):
        if source_type == 'salesforce':
            # Salesforce often returns ISO format dates
            try:
                # Try to parse ISO format
                if 'T' in val:
                    # Remove timezone info if present
                    clean_val = val.split('T')[0] + ' ' + val.split('T')[1].split('+')[0].split('Z')[0]
                    parsed_dt = datetime.fromisoformat(clean_val.replace('Z', ''))
                    return parsed_dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    # Assume it's already in a good format
                    return val
            except Exception as e:
                logging.warning(f"{source_type} field '{field_name}': Could not parse date string {val}: {str(e)}")
                return None
    
    return val


def transform_row_data(row_data, column_names, source_type='generic'):
    """
    Transform an entire row of data for SQL Server compatibility.
    
    Args:
        row_data: List of values representing a row
        column_names: List of column names corresponding to the values
        source_type: Source system ('redshift', 'salesforce', 'generic')
        
    Returns:
        Transformed list of values
    """
    if not row_data or not column_names:
        return row_data
    
    transformed_row = []
    
    for i, val in enumerate(row_data):
        field_name = column_names[i] if i < len(column_names) else f'column_{i}'
        
        # Apply appropriate transformation based on field name patterns
        if any(keyword in field_name.lower() for keyword in [
            'amount', 'commission', 'billed', 'payment', 'profit', 'deduction', 'override'
        ]):
            # Numeric/currency field
            transformed_val = transform_numeric_value(val, source_type, field_name)
        elif any(keyword in field_name.lower() for keyword in [
            'date', 'time', 'created', 'modified', 'opened', 'closed'
        ]):
            # Date/time field
            transformed_val = transform_datetime_value(val, source_type, field_name)
        elif any(keyword in field_name.lower() for keyword in ['rate', 'split']):
            # Rate field (keep more precision)
            transformed_val = transform_numeric_value(val, source_type, field_name)
        else:
            # For other fields, just handle basic numeric conversion
            if isinstance(val, (int, float, Decimal)) and val is not None:
                transformed_val = transform_numeric_value(val, source_type, field_name)
            else:
                transformed_val = val
        
        transformed_row.append(transformed_val)
    
    return transformed_row


def log_transformation_summary(original_count, transformed_count, source_type, query_name):
    """
    Log a summary of transformations applied.
    
    Args:
        original_count: Number of rows before transformation
        transformed_count: Number of rows after transformation
        source_type: Source system name
        query_name: Name of the query/table
    """
    if original_count != transformed_count:
        logging.warning(f"{source_type} {query_name}: Row count changed from {original_count} to {transformed_count} during transformation")
    else:
        logging.info(f"{source_type} {query_name}: Successfully transformed {transformed_count} rows")