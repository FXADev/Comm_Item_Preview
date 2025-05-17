"""
Schema Validation Module for SharePoint Upload

This module provides schema validation functions to ensure data
integrity throughout the ETL process. It validates:
1. Required columns exist in extracted datasets
2. Data types match expected types
3. Reports schema issues in a clear, actionable format

These validations help prevent failures due to upstream schema changes.
"""

import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Union, Any, Set


def get_required_columns(dataset_type: str) -> List[str]:
    """
    Get the list of required columns for a specific dataset type.
    
    Args:
        dataset_type: Type of dataset ('commission', 'agency_folder', or 'agency_group')
        
    Returns:
        List of required column names
    """
    # Define required columns for each dataset type
    required_columns = {
        'commission': [
            'RPM_Payout_Agency_ID', 'Agency_Name', 'Customer_Name',
            'Commission_Amt', 'Commission_Type', 'Month_End_Date'
        ],
        'agency_folder': [
            'RPM_Agency_ID', 'Agency_Name', 'Agency_Commission_Folder'
        ],
        'agency_group': [
            'RPM_Agency_ID', 'Agency_Group'
        ]
    }
    
    return required_columns.get(dataset_type, [])


def get_expected_types(dataset_type: str) -> Dict[str, Any]:
    """
    Get the expected data types for columns in a specific dataset type.
    
    Args:
        dataset_type: Type of dataset ('commission', 'agency_folder', or 'agency_group')
        
    Returns:
        Dictionary mapping column names to expected types or list of acceptable types
    """
    # Define expected types for each dataset type
    # For numeric values, we accept multiple numeric types
    # For dates, we accept pandas Timestamp objects
    expected_types = {
        'commission': {
            'RPM_Payout_Agency_ID': str,
            'Agency_Name': str, 
            'Customer_Name': str,
            'Commission_Amt': [float, int, np.float64, np.int64],
            'Commission_Type': str,
            'Month_End_Date': pd.Timestamp
        },
        'agency_folder': {
            'RPM_Agency_ID': str,
            'Agency_Name': str,
            'Agency_Commission_Folder': str
        },
        'agency_group': {
            'RPM_Agency_ID': str,
            'Agency_Group': str
        }
    }
    
    return expected_types.get(dataset_type, {})


def validate_schema(df: pd.DataFrame, required_columns: List[str], 
                   expected_types: Dict[str, Any] = None) -> Tuple[bool, List[str], List[Tuple[str, Any, Any]]]:
    """
    Validate that a dataframe has the required columns and expected types.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        expected_types: Dictionary mapping column names to expected types
        
    Returns:
        Tuple containing:
        - Boolean indicating if the schema is valid
        - List of missing column names
        - List of tuples (column_name, expected_type, found_type) for type errors
    """
    missing_columns = []
    type_errors = []
    
    # Check for missing columns
    for col in required_columns:
        if col not in df.columns:
            missing_columns.append(col)
    
    # Check data types if no columns are missing and expected_types is provided
    if not missing_columns and expected_types:
        for col, expected_type in expected_types.items():
            # Skip columns that might be optional but have type requirements
            if col not in df.columns:
                continue
                
            # Handle case where multiple types are accepted (e.g., numerics)
            if isinstance(expected_type, list):
                # Check if any value in the column matches one of the expected types
                valid = False
                found_type = None
                
                # Try to get a non-null sample value for type checking
                sample_values = df[col].dropna()
                if not sample_values.empty:
                    sample_value = sample_values.iloc[0]
                    found_type = type(sample_value)
                    
                    # Check if the type matches any of the accepted types
                    for accepted_type in expected_type:
                        if isinstance(sample_value, accepted_type):
                            valid = True
                            break
                else:
                    # If all values are null, we can't check the type
                    valid = True
                    
                if not valid and found_type:
                    type_errors.append((col, str(expected_type), str(found_type)))
            else:
                # For single expected type
                # Try to get a non-null sample value
                sample_values = df[col].dropna()
                if not sample_values.empty:
                    sample_value = sample_values.iloc[0]
                    
                    # Special case for pandas Timestamp
                    if expected_type == pd.Timestamp and not isinstance(sample_value, pd.Timestamp):
                        type_errors.append((col, str(pd.Timestamp), str(type(sample_value))))
                    elif expected_type != pd.Timestamp and not isinstance(sample_value, expected_type):
                        type_errors.append((col, str(expected_type), str(type(sample_value))))
    
    # Schema is valid if there are no missing columns and no type errors
    is_valid = not missing_columns and not type_errors
    
    return is_valid, missing_columns, type_errors


def validate_commission_schema(df: pd.DataFrame) -> Tuple[bool, List[str], List[Tuple[str, Any, Any]]]:
    """
    Validate the schema of commission data.
    
    Args:
        df: DataFrame containing commission data
        
    Returns:
        Tuple containing:
        - Boolean indicating if the schema is valid
        - List of missing column names
        - List of tuples (column_name, expected_type, found_type) for type errors
    """
    required_columns = get_required_columns('commission')
    expected_types = get_expected_types('commission')
    
    return validate_schema(df, required_columns, expected_types)


def validate_agency_folder_schema(df: pd.DataFrame) -> Tuple[bool, List[str], List[Tuple[str, Any, Any]]]:
    """
    Validate the schema of agency folder data.
    
    Args:
        df: DataFrame containing agency folder data
        
    Returns:
        Tuple containing:
        - Boolean indicating if the schema is valid
        - List of missing column names
        - List of tuples (column_name, expected_type, found_type) for type errors
    """
    required_columns = get_required_columns('agency_folder')
    expected_types = get_expected_types('agency_folder')
    
    return validate_schema(df, required_columns, expected_types)


def validate_agency_group_schema(df: pd.DataFrame) -> Tuple[bool, List[str], List[Tuple[str, Any, Any]]]:
    """
    Validate the schema of agency group data.
    
    Args:
        df: DataFrame containing agency group data
        
    Returns:
        Tuple containing:
        - Boolean indicating if the schema is valid
        - List of missing column names
        - List of tuples (column_name, expected_type, found_type) for type errors
    """
    required_columns = get_required_columns('agency_group')
    expected_types = get_expected_types('agency_group')
    
    return validate_schema(df, required_columns, expected_types)


def create_schema_report(dataset_name: str, missing_columns: List[str], 
                        type_errors: List[Tuple[str, Any, Any]]) -> str:
    """
    Create a detailed report of schema validation errors.
    
    Args:
        dataset_name: Name of the dataset being validated
        missing_columns: List of missing column names
        type_errors: List of tuples (column_name, expected_type, found_type)
        
    Returns:
        Formatted report string
    """
    report = [f"Schema Validation Report for {dataset_name}"]
    report.append("=" * 50)
    
    if not missing_columns and not type_errors:
        report.append("✅ Schema validation passed. All required columns present with correct data types.")
        return "\n".join(report)
    
    # Report missing columns
    if missing_columns:
        report.append("\n❌ Missing Required Columns:")
        for col in missing_columns:
            report.append(f"  - {col}")
    
    # Report type errors
    if type_errors:
        report.append("\n❌ Data Type Errors:")
        for col, expected_type, found_type in type_errors:
            report.append(f"  - Column: {col}")
            report.append(f"    Expected type: {expected_type}")
            report.append(f"    Found type: {found_type}")
    
    # Provide actionable recommendations
    report.append("\nRECOMMENDED ACTIONS:")
    if missing_columns:
        report.append("1. Check if the source query is selecting all required columns")
        report.append("2. Verify column naming in the source database hasn't changed")
    if type_errors:
        report.append("3. Inspect data types in the source query")
        report.append("4. Consider adding explicit type casting in the extraction process")
    
    return "\n".join(report)


def validate_all_schemas(comm_data: pd.DataFrame, agency_folders: pd.DataFrame, 
                        agency_groups: pd.DataFrame) -> bool:
    """
    Validate schemas for all datasets and log detailed reports.
    
    Args:
        comm_data: DataFrame containing commission data
        agency_folders: DataFrame containing agency folder data
        agency_groups: DataFrame containing agency group data
        
    Returns:
        Boolean indicating if all schemas are valid
    """
    # Validate commission data schema
    comm_valid, comm_missing, comm_type_errors = validate_commission_schema(comm_data)
    if not comm_valid:
        logging.error(create_schema_report("Commission Data", comm_missing, comm_type_errors))
    
    # Validate agency folder data schema
    folder_valid, folder_missing, folder_type_errors = validate_agency_folder_schema(agency_folders)
    if not folder_valid:
        logging.error(create_schema_report("Agency Folder Data", folder_missing, folder_type_errors))
    
    # Validate agency group data schema
    group_valid, group_missing, group_type_errors = validate_agency_group_schema(agency_groups)
    if not group_valid:
        logging.error(create_schema_report("Agency Group Data", group_missing, group_type_errors))
    
    # All schemas are valid only if all individual schemas are valid
    return comm_valid and folder_valid and group_valid
