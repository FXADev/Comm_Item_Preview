"""
Logger Module

This module provides functions for setting up and configuring logging for the ETL process.
"""

import os
import logging
import datetime


def setup_logging(manual_mode=False):
    """
    Set up logging configuration for the ETL process.
    
    Args:
        manual_mode (bool): Whether the ETL is running in manual mode
        
    Returns:
        tuple: (log_filename, batch_id) - The log filename and batch ID for tracking
    """
    # Generate batch ID and timestamp for logging
    now = datetime.datetime.now()
    batch_id = now.strftime("%Y%m%d%H%M%S")
    
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Generate log filename with timestamp
    log_filename = f"logs/etl_log_{now.strftime('%Y%m%d_%H%M%S')}.txt"
    
    # Configure logging
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Add console handler for terminal output
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)
    
    # Log initial information
    logging.info(f"Logging to {log_filename}")
    logging.info(f"ETL process started with batch ID: {batch_id}")
    if manual_mode:
        logging.info("Running in MANUAL mode (simulation only)")
    
    return log_filename, batch_id
