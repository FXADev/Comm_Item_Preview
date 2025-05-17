"""
SharePoint Upload Utilities

This module provides shared helper functions for the SharePoint upload workflow.
"""

import os
import sys
import yaml
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

def load_config(config_path='config.yml'):
    """
    Load the configuration from the specified YAML file.
    
    Args:
        config_path (str): Path to the configuration file, defaults to 'config.yml'
        
    Returns:
        dict: Configuration dictionary
        
    Raises:
        SystemExit: If configuration loading fails
    """
    try:
        # Path handling for the SharePoint upload workflow
        module_dir = Path(__file__).parent.absolute()
        if os.path.exists(os.path.join(module_dir, config_path)):
            config_path = os.path.join(module_dir, config_path)
        else:
            # Fallback to current working directory
            config_path = config_path
            
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        logging.info(f"Configuration loaded successfully from {config_path}")
        return config
    except Exception as e:
        logging.error(f"Failed to load config from {config_path}: {e}")
        sys.exit(1)

def setup_logging(run_ym):
    """
    Setup logging configuration for the SharePoint upload workflow.
    
    Args:
        run_ym (str): Year and month for the current run (format: YYYYMM)
        
    Returns:
        str: Path to the log file
    """
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / f"{run_ym}_upload_log.csv"
    
    # Create log file with headers if it doesn't exist
    if not log_file.exists():
        df = pd.DataFrame(columns=[
            'Agency',
            'RPM_Payout_Agency_ID',
            'FullPath',
            'RowCount',
            'Checked_At',
            'Write_Intent',
            'Status'
        ])
        df.to_csv(log_file, index=False)
        logging.info(f"Created log file: {log_file}")
    
    return str(log_file)

def validate_path_length(path):
    """
    Validate if a path is under 255 characters (SharePoint limit).
    
    Args:
        path (str): Path to validate
        
    Returns:
        bool: True if the path is valid, False otherwise
    """
    return len(path) <= 255

def safe_write_file(df, path, sheet_name='Commission Data'):
    """
    Safely write a DataFrame to an Excel file with appropriate error handling.
    
    Args:
        df (pandas.DataFrame): DataFrame to write
        path (str): Full path to write the file to
        sheet_name (str): Name of the Excel sheet
        
    Returns:
        tuple: (success, error_message)
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        # Write Excel file
        df.to_excel(path, sheet_name=sheet_name, index=False)
        return True, None
    except Exception as e:
        error_message = f"Failed to write file {path}: {str(e)}"
        logging.error(error_message)
        return False, error_message

def delete_unconfirmed_files(log_file):
    """
    Delete any files marked as 'Pending' in the log file.
    Used for rollback operations when a run is interrupted.
    
    Args:
        log_file (str): Path to the log file
        
    Returns:
        list: List of files that were deleted
    """
    try:
        log_df = pd.read_csv(log_file)
        pending_files = log_df[log_df['Status'] == 'Pending']
        
        deleted_files = []
        for _, row in pending_files.iterrows():
            file_path = row['FullPath']
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    deleted_files.append(file_path)
                    # Update log with rolledback status
                    log_df.loc[log_df['FullPath'] == file_path, 'Status'] = 'RolledBack'
                    logging.info(f"Rolled back file: {file_path}")
            except Exception as e:
                logging.error(f"Failed to delete file {file_path}: {str(e)}")
        
        # Save updated log file
        log_df.to_csv(log_file, index=False)
        
        return deleted_files
    except Exception as e:
        logging.error(f"Failed to perform rollback: {str(e)}")
        return []

def update_log_entry(log_file, full_path, status):
    """
    Update the status of a file in the log.
    
    Args:
        log_file (str): Path to the log file
        full_path (str): Full path of the file to update
        status (str): New status ('Succeeded', 'Failed', 'Skipped', 'RolledBack')
        
    Returns:
        bool: True if the update was successful, False otherwise
    """
    try:
        log_df = pd.read_csv(log_file)
        log_df.loc[log_df['FullPath'] == full_path, 'Status'] = status
        log_df.to_csv(log_file, index=False)
        return True
    except Exception as e:
        logging.error(f"Failed to update log entry: {str(e)}")
        return False

def generate_summary(log_file, run_ym, start_time):
    """
    Generate a summary of the execution results.
    
    Args:
        log_file (str): Path to the log file
        run_ym (str): Year and month for the current run
        start_time (datetime): Start time of the run
        
    Returns:
        dict: Summary statistics
    """
    elapsed_time = (datetime.now() - start_time).total_seconds()
    
    try:
        log_df = pd.read_csv(log_file)
        summary = {
            'run_ym': run_ym,
            'total_files_attempted': len(log_df),
            'total_succeeded': len(log_df[log_df['Status'] == 'Succeeded']),
            'total_skipped': len(log_df[log_df['Status'] == 'Skipped']),
            'total_rolled_back': len(log_df[log_df['Status'] == 'RolledBack']),
            'total_failed': len(log_df[log_df['Status'] == 'Failed']),
            'elapsed_time_seconds': elapsed_time,
            'elapsed_time_formatted': f"{elapsed_time/60:.2f} minutes"
        }
        
        # Write summary to a separate file
        summary_path = os.path.join(os.path.dirname(log_file), f"{run_ym}_summary.txt")
        with open(summary_path, 'w') as f:
            for key, value in summary.items():
                f.write(f"{key}: {value}\n")
                
        logging.info(f"Summary generated: {summary}")
        return summary
    except Exception as e:
        logging.error(f"Failed to generate summary: {str(e)}")
        return None
