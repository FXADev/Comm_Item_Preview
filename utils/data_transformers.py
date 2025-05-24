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
    Financial values are capped at limits instead of being set to NULL.
    
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
                if len(str_val.replace('.', '').replace('-', '')) > 18:
                    logging.warning(f"{source_type} field '{field_name}': Very large Decimal value {val}, capping to limit")
                    # Use string parsing to avoid float overflow
                    if any(keyword in field_name.lower() for keyword in ['rate', 'split']):
                        return 9999.9999 if not str_val.startswith('-') else -9999.9999
                    else:
                        return 999999999999999.99 if not str_val.startswith('-') else -999999999999999.99
                float_val = float(val)
            elif isinstance(val, int):
                # Handle very large integers
                if abs(val) > 999999999999999:  # More than 15 digits (decimal(18,2) before decimal limit)
                    logging.warning(f"{source_type} field '{field_name}': Very large integer {val}, capping to limit")
                    if any(keyword in field_name.lower() for keyword in ['rate', 'split']):
                        return 9999.9999 if val > 0 else -9999.9999
                    else:
                        return 999999999999999.99 if val > 0 else -999999999999999.99
                float_val = float(val)
            else:
                float_val = val
            
            # Check for special float values
            if not (float_val == float_val):  # NaN check (NaN != NaN)
                logging.debug(f"{source_type} field '{field_name}': NaN value converted to 0")
                return 0.0  # Convert NaN to 0 for financial data
            if float_val == float('inf'):
                logging.warning(f"{source_type} field '{field_name}': Positive infinity converted to maximum limit")
                if any(keyword in field_name.lower() for keyword in ['rate', 'split']):
                    return 9999.9999
                else:
                    return 999999999999999.99
            if float_val == float('-inf'):
                logging.warning(f"{source_type} field '{field_name}': Negative infinity converted to minimum limit")
                if any(keyword in field_name.lower() for keyword in ['rate', 'split']):
                    return -9999.9999
                else:
                    return -999999999999999.99
            
            # SQL Server decimal limits based on UPDATED table schema
            # Currency fields: decimal(18,2) - 16 digits before decimal, 2 after
            # Rate fields: decimal(8,4) - 4 digits before decimal, 4 after
            if any(keyword in field_name.lower() for keyword in ['rate', 'split']):
                # Rate fields are decimal(8,4) - max 4 digits before decimal
                max_value = 9999.9999
                min_value = -9999.9999
            else:
                # Currency fields are decimal(18,2) - max 16 digits before decimal
                # Much more capacity than the previous decimal(18,4)!
                max_value = 999999999999999.99  # 16 digits before decimal
                min_value = -999999999999999.99
            
            # Cap values at limits instead of setting to NULL
            if float_val > max_value:
                logging.warning(f"{source_type} field '{field_name}': Value {float_val} exceeds maximum (max: {max_value}), capping at limit")
                capped_val = max_value
            elif float_val < min_value:
                logging.warning(f"{source_type} field '{field_name}': Value {float_val} below minimum (min: {min_value}), capping at limit")
                capped_val = min_value
            else:
                capped_val = float_val
            
            # Round to appropriate decimal places based on field type
            if any(keyword in field_name.lower() for keyword in ['rate', 'split']):
                # Rate fields - keep 4 decimal places (decimal(8,4))
                result = round(capped_val, 4)
            else:
                # Currency fields - keep 2 decimal places (decimal(18,2))
                result = round(capped_val, 2)
            
            # Final safety check after rounding using new decimal(18,2) limits
            if any(keyword in field_name.lower() for keyword in ['rate', 'split']):
                final_max = 9999.9999
                final_min = -9999.9999
            else:
                final_max = 999999999999999.99  # decimal(18,2) limit
                final_min = -999999999999999.99
            
            if result > final_max:
                logging.warning(f"{source_type} field '{field_name}': Value {result} still exceeds maximum after rounding, final cap at {final_max}")
                return final_max
            elif result < final_min:
                logging.warning(f"{source_type} field '{field_name}': Value {result} still below minimum after rounding, final cap at {final_min}")
                return final_min
            
            return result
                
        except (InvalidOperation, ValueError, OverflowError, TypeError) as e:
            logging.warning(f"{source_type} field '{field_name}': Error processing numeric value {val} (type: {type(val)}): {str(e)}. Setting to 0.")
            return 0.0  # Return 0 instead of NULL for financial data
    
    # Handle string representations of numbers
    elif isinstance(val, str):
        # Check if it's a numeric string
        try:
            # Avoid processing very long strings that might be large numbers
            if len(val) > 25:
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
        tuple: (transformed_list_of_values, capped_value_count)
    """
    if not row_data or not column_names:
        return row_data, 0
    
    transformed_row = []
    capped_values = 0
    
    for i, val in enumerate(row_data):
        field_name = column_names[i] if i < len(column_names) else f'column_{i}'
        original_val = val
        
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
        
        # Count capped values for large numeric values (values that were changed due to limits)
        if (original_val is not None and 
            isinstance(original_val, (int, float, Decimal)) and 
            transformed_val is not None and
            isinstance(transformed_val, (int, float)) and
            abs(float(original_val)) > 1000 and  # Only count significant values
            abs(abs(float(original_val)) - abs(float(transformed_val))) > 0.01):  # Value was actually changed
            capped_values += 1
        
        transformed_row.append(transformed_val)
    
    return transformed_row, capped_values


def log_transformation_summary(original_count, transformed_count, source_type, query_name, capped_values=0):
    """
    Log a summary of transformations applied.
    
    Args:
        original_count: Number of rows before transformation
        transformed_count: Number of rows after transformation
        source_type: Source system name
        query_name: Name of the query/table
        capped_values: Number of values that were capped due to range limits
    """
    if original_count != transformed_count:
        logging.warning(f"{source_type} {query_name}: Row count changed from {original_count} to {transformed_count} during transformation")
    else:
        logging.info(f"{source_type} {query_name}: Successfully transformed {transformed_count} rows")
    
    if capped_values > 0:
        logging.warning(f"{source_type} {query_name}: Capped {capped_values} out-of-range values at SQL Server limits")