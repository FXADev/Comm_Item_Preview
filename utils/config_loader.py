"""
Config Loader Module

This module provides functions for loading and validating the ETL configuration.
"""

import os
import sys
import yaml
import logging


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
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        logging.info(f"Configuration loaded successfully from {config_path}")
        return config
    except Exception as e:
        logging.error(f"Failed to load config from {config_path}: {e}")
        sys.exit(1)


def load_query_from_file(file_path):
    """
    Load a query from a file.
    
    Args:
        file_path (str): Path to the query file
        
    Returns:
        str or None: Query content if successful, None otherwise
    """
    try:
        with open(file_path, 'r') as f:
            query = f.read()
        logging.debug(f"Query loaded from {file_path}")
        return query
    except Exception as e:
        logging.error(f"Failed to load query from {file_path}: {e}")
        return None


def verify_credentials(manual_mode=False):
    """
    Verify that all required environment variables for database connections are present.
    
    Args:
        manual_mode (bool): Whether the ETL is running in manual mode
        
    Returns:
        bool: True if all credentials are available or in manual mode, False otherwise
    """
    missing = []
    
    # Check Redshift credentials
    for var in ['REDSHIFT_HOST', 'REDSHIFT_USER', 'REDSHIFT_PASSWORD', 'REDSHIFT_DATABASE']:
        if not os.getenv(var):
            missing.append(var)
    
    # Check Salesforce credentials
    for var in ['SF_USERNAME', 'SF_PASSWORD', 'SF_SECURITY_TOKEN']:
        if not os.getenv(var):
            missing.append(var)
    
    # Check SQL Server credentials (for staging table insertion)
    for var in ['SQL_SERVER', 'SQL_DATABASE', 'SQL_USERNAME', 'SQL_PASSWORD']:
        if not os.getenv(var):
            missing.append(var)
    
    if missing:
        logging.error(f"Missing required environment variables: {', '.join(missing)}")
        if not manual_mode:
            logging.error("Run 'python verify_credentials.py' to diagnose credential issues")
            logging.error("For testing without proper credentials, use --manual flag")
            return False
    
    logging.info("Credentials verified successfully")
    return True
