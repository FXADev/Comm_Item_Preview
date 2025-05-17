"""
SharePoint Upload Workflow Main Orchestrator

This module orchestrates the full SharePoint upload workflow:
- Extract agency folder information from Salesforce
- Extract commission data from SQL Server
- Merge data and build file paths
- Validate paths and log intent to write
- Confirm with user before writing
- Write Excel files or roll back pending files
- Validate outputs and generate summary
"""

import os
import sys
import logging
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path for relative imports
sys.path.insert(0, str(Path(__file__).parent.absolute()))
from sp_utils import load_config, setup_logging, generate_summary
from sp_extract_soql import connect_to_salesforce, extract_agency_folders
from sp_extract_sql import connect_to_sql, extract_commission_data, store_execution_notification
from sp_extract_redshift import extract_agency_groups
from sp_write_statements import (
    build_file_paths, validate_and_log_paths,
    group_and_prepare_files, write_excel_files
)
from sp_validate_outputs import validate_file_outputs, perform_rollback, generate_execution_summary


def setup_environment():
    """
    Setup the environment for the workflow.
    
    Returns:
        tuple: (config, log_file, run_ym)
    """
    # Load environment variables from project root .env file
    dotenv_path = Path(__file__).parent.parent / '.env'
    if dotenv_path.exists():
        load_dotenv(dotenv_path)
        logging.info(f"Loaded environment variables from {dotenv_path}")
    else:
        logging.warning(f"No .env file found at {dotenv_path}. Make sure environment variables are set.")
    
    # Load configuration
    config = load_config(Path(__file__).parent / 'sp_config.yml')
    
    # Get run_ym from config or command line args
    run_ym = config.get('run_ym')
    if not run_ym:
        raise ValueError("Run_YM not found in configuration")
    
    # Setup logging
    log_file = setup_logging(run_ym)
    logging.info(f"SharePoint Upload workflow started for Run_YM={run_ym}")
    
    return config, log_file, run_ym


def extract_data(run_ym):
    """
    Extract data from sources.
    
    Args:
        run_ym (str): Year and month for the current run (format: YYYYMM)
        
    Returns:
        tuple: (commission_data, agency_folders, agency_groups)
    """
    logging.info("Starting data extraction phase")
    
    # Connect to data sources
    sf = connect_to_salesforce()
    sql_conn = connect_to_sql()
    
    # Extract agency folders from Salesforce
    agency_folders = extract_agency_folders(sf)
    if agency_folders.empty:
        raise ValueError("No agency folders found in Salesforce")
    
    # Extract commission data from SQL Server
    commission_data = extract_commission_data(run_ym, sql_conn)
    if commission_data.empty:
        raise ValueError(f"No commission data found for Run_YM={run_ym}")
    
    # Extract agency groups from Redshift
    agency_groups = extract_agency_groups()
    if agency_groups.empty:
        logging.warning("No agency group data found from Redshift, folder mapping might be incomplete")
    
    # Close connections
    sql_conn.close()
    
    logging.info(f"Data extraction complete: {len(commission_data)} commission records, "
                f"{len(agency_folders)} agency folders, {len(agency_groups)} agency groups")
    
    return commission_data, agency_folders, agency_groups


def prepare_files(commission_data, agency_folders, agency_groups, config, run_ym, log_file):
    """
    Prepare files for writing.
    
    Args:
        commission_data (pandas.DataFrame): DataFrame containing commission data
        agency_folders (pandas.DataFrame): DataFrame containing agency folder information
        agency_groups (pandas.DataFrame): DataFrame containing agency group information from Redshift
        config (dict): Configuration dictionary
        run_ym (str): Year and month for the current run (format: YYYYMM)
        log_file (str): Path to the log file
        
    Returns:
        list: List of dictionaries containing grouped data and file info
    """
    logging.info("Starting file preparation phase")
    
    # Get output base path from config
    output_base_path = config['output_paths']['base_path']
    
    # Build file paths
    merged_df = build_file_paths(commission_data, agency_folders, agency_groups, run_ym, output_base_path)
    
    # Validate paths and log
    valid_df = validate_and_log_paths(merged_df, log_file)
    
    # Group data by agency and prepare files
    grouped_data = group_and_prepare_files(valid_df, run_ym, log_file)
    
    logging.info(f"File preparation complete: {len(grouped_data)} files ready")
    
    return grouped_data


def user_confirmation(grouped_data):
    """
    Get user confirmation before writing files.
    
    Args:
        grouped_data (list): List of dictionaries containing grouped data and file info
        
    Returns:
        bool: Whether the user confirmed the write operation
    """
    print("\n" + "="*80)
    print(f"SHAREPOINT UPLOAD: {len(grouped_data)} agency files are ready to be written")
    print("="*80)
    
    for i, group in enumerate(grouped_data[:5]):
        print(f"{i+1}. Agency: {group['agency_name']} ({group['agency_id']})")
        print(f"   Rows: {group['row_count']}")
        print(f"   Path: {group['file_path']}")
    
    if len(grouped_data) > 5:
        print(f"\n... and {len(grouped_data) - 5} more agencies")
    
    print("\n")
    confirm = input("Should I push these files to SharePoint? (yes/no): ").lower()
    
    if confirm in ['y', 'yes']:
        double_confirm = input("Are you sure? This will overwrite existing files. (yes/no): ").lower()
        return double_confirm in ['y', 'yes']
    
    return False


def main(args=None):
    """
    Main entry point for the SharePoint upload workflow.
    
    Args:
        args (argparse.Namespace, optional): Command line arguments
    """
    start_time = datetime.now()
    
    try:
        # Setup environment
        config, log_file, run_ym = setup_environment()
        
        # Parse command line arguments if provided
        if args and args.rollback:
            # Perform rollback for the configured run_ym
            rollback_result = perform_rollback(log_file, run_ym)
            print(f"Rollback result: {rollback_result['message']}")
            store_execution_notification(run_ym, 'Rollback', rollback_result['message'])
            return
        
        # Override run_ym if provided in args
        if args and args.run_ym:
            run_ym = args.run_ym
        
        # Extract data from sources
        commission_data, agency_folders, agency_groups = extract_data(run_ym)
        
        # Prepare files for writing
        grouped_data = prepare_files(commission_data, agency_folders, agency_groups, config, run_ym, log_file)
        
        # Get user confirmation
        user_confirmed = user_confirmation(grouped_data)
        
        if user_confirmed:
            logging.info("User confirmed write operation, writing files")
            
            # Write files
            success_count, failure_count = write_excel_files(grouped_data, log_file, user_confirmed)
            
            # Validate outputs
            validation_results = validate_file_outputs(grouped_data, log_file)
            
            # Generate summary
            summary_result = generate_execution_summary(log_file, run_ym, start_time)
            
            # Store notification in SQL
            store_execution_notification(
                run_ym,
                'Success' if failure_count == 0 else 'Partial',
                summary_result['message']
            )
            
            # Print summary
            print("\n" + "="*80)
            print(f"SHAREPOINT UPLOAD COMPLETE")
            print("="*80)
            print(summary_result['message'])
        
        else:
            logging.info("User cancelled write operation, performing rollback")
            
            # Perform rollback
            rollback_result = perform_rollback(log_file, run_ym)
            
            # Store notification in SQL
            store_execution_notification(run_ym, 'Cancelled', rollback_result['message'])
            
            # Print result
            print("\n" + "="*80)
            print(f"SHAREPOINT UPLOAD CANCELLED")
            print("="*80)
            print(rollback_result['message'])
    
    except Exception as e:
        error_message = f"SharePoint Upload workflow failed: {str(e)}"
        logging.error(error_message, exc_info=True)
        
        # Store notification in SQL
        try:
            store_execution_notification(run_ym, 'Failed', error_message)
        except:
            logging.error("Failed to store execution notification", exc_info=True)
        
        # Print error
        print("\n" + "="*80)
        print(f"SHAREPOINT UPLOAD FAILED")
        print("="*80)
        print(error_message)
        
        # Try to perform rollback if an error occurred
        try:
            rollback_result = perform_rollback(log_file, run_ym)
            print(f"Rollback result: {rollback_result['message']}")
        except:
            logging.error("Failed to perform rollback after error", exc_info=True)


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='SharePoint Upload Workflow')
    parser.add_argument('--run-ym', type=str, help='Year and month for the current run (format: YYYYMM)')
    parser.add_argument('--rollback', action='store_true', help='Perform rollback for the configured run_ym')
    args = parser.parse_args()
    
    main(args)
