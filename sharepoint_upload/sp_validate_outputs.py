"""
Validate Outputs Module for SharePoint Upload

This module handles confirmation of file write success, updating log status,
and providing rollback capability for the SharePoint upload workflow.
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add parent directory to path for relative imports
sys.path.insert(0, str(Path(__file__).parent.absolute()))
from sp_utils import update_log_entry, delete_unconfirmed_files, generate_summary


def validate_file_outputs(grouped_data, log_file):
    """
    Validate that all files were written successfully and update their status in the log.
    
    Args:
        grouped_data (list): List of dictionaries containing grouped data and file info
        log_file (str): Path to the log file
        
    Returns:
        dict: Dictionary with validation results
    """
    results = {
        'total': len(grouped_data),
        'exists': 0,
        'missing': 0,
        'validated': []
    }
    
    for group in grouped_data:
        agency_id = group['agency_id']
        agency_name = group['agency_name']
        file_path = group['file_path']
        
        file_exists = os.path.exists(file_path)
        
        validation_result = {
            'agency_id': agency_id,
            'agency_name': agency_name,
            'file_path': file_path,
            'exists': file_exists,
            'checked_at': datetime.now()
        }
        
        if file_exists:
            results['exists'] += 1
            # Only update if the file exists - we don't want to change 'Pending' status
            # for files that weren't written yet
            update_log_entry(log_file, file_path, 'Succeeded')
        else:
            results['missing'] += 1
            logging.warning(f"File not found for agency {agency_id}: {file_path}")
            
        results['validated'].append(validation_result)
    
    logging.info(f"Validated {results['total']} files: {results['exists']} exist, {results['missing']} missing")
    return results


def perform_rollback(log_file, run_ym):
    """
    Delete any files marked as 'Pending' in the log file.
    
    Args:
        log_file (str): Path to the log file
        run_ym (str): Year and month for the current run (format: YYYYMM)
        
    Returns:
        dict: Dictionary with rollback results
    """
    logging.info(f"Initiating rollback for Run_YM={run_ym}")
    
    try:
        # Read the log file
        log_df = pd.read_csv(log_file)
        
        # Find pending files
        pending_files = log_df[log_df['Status'] == 'Pending']
        total_pending = len(pending_files)
        
        if total_pending == 0:
            logging.info("No pending files found to roll back")
            return {
                'status': 'success',
                'message': 'No pending files found to roll back',
                'files_rolled_back': 0
            }
        
        # Delete pending files
        deleted_files = delete_unconfirmed_files(log_file)
        
        result = {
            'status': 'success',
            'message': f'Successfully rolled back {len(deleted_files)} of {total_pending} pending files',
            'files_rolled_back': len(deleted_files),
            'files': deleted_files
        }
        
        logging.info(result['message'])
        return result
    
    except Exception as e:
        error_msg = f"Failed to perform rollback: {str(e)}"
        logging.error(error_msg)
        return {
            'status': 'error',
            'message': error_msg,
            'files_rolled_back': 0
        }


def generate_execution_summary(log_file, run_ym, start_time):
    """
    Generate a summary of the execution results.
    
    Args:
        log_file (str): Path to the log file
        run_ym (str): Year and month for the current run
        start_time (datetime): Start time of the run
        
    Returns:
        dict: Summary statistics
    """
    try:
        summary = generate_summary(log_file, run_ym, start_time)
        
        # Create a more user-friendly message
        message = (
            f"SharePoint Upload Summary for Run_YM={run_ym}\n"
            f"--------------------------------------------\n"
            f"Total files attempted: {summary['total_files_attempted']}\n"
            f"Successfully written: {summary['total_succeeded']}\n"
            f"Skipped: {summary['total_skipped']}\n"
            f"Failed: {summary['total_failed']}\n"
            f"Rolled back: {summary['total_rolled_back']}\n"
            f"Execution time: {summary['elapsed_time_formatted']}\n"
        )
        
        logging.info(f"Execution summary for Run_YM={run_ym}:\n{message}")
        
        return {
            'summary': summary,
            'message': message
        }
    except Exception as e:
        error_msg = f"Failed to generate execution summary: {str(e)}"
        logging.error(error_msg)
        return {
            'status': 'error',
            'message': error_msg
        }
