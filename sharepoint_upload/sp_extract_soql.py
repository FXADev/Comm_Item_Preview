"""
Salesforce SOQL Extractor for SharePoint Upload

This module handles extracting agency folder information from Salesforce
using SOQL queries defined in the configuration.
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from simple_salesforce import Salesforce

# Add parent directory to path for relative imports
sys.path.insert(0, str(Path(__file__).parent.absolute()))
from sp_utils import load_config

# Try to import the load_query_from_file function from the main project utils
try:
    sys.path.insert(0, str(Path(__file__).parent.parent.absolute()))
    from utils.config_loader import load_query_from_file as project_load_query
except ImportError:
    # If it fails, define our own version
    def project_load_query(file_path):
        """
        Load a query from a file.
        
        Args:
            file_path (str): Path to the query file
            
        Returns:
            str or None: Query content if successful, None otherwise
        """
        try:
            project_root = Path(__file__).parent.parent.absolute()
            absolute_path = os.path.join(project_root, file_path)
                
            with open(absolute_path, 'r') as f:
                query = f.read()
            logging.debug(f"Query loaded from {absolute_path}")
            return query
        except Exception as e:
            logging.error(f"Failed to load query from {file_path}: {e}")
            return None


def connect_to_salesforce():
    """
    Connect to Salesforce using credentials from environment variables.
    
    Returns:
        Salesforce: Salesforce connection object
    
    Raises:
        Exception: If connection fails
    """
    try:
        username = os.environ.get('SF_USERNAME')
        password = os.environ.get('SF_PASSWORD')
        security_token = os.environ.get('SF_SECURITY_TOKEN')
        domain = os.environ.get('SF_DOMAIN', 'login')
        
        if not all([username, password, security_token]):
            raise ValueError("Missing Salesforce credentials in environment variables")
        
        sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            domain=domain
        )
        logging.info("Connected to Salesforce successfully")
        return sf
    except Exception as e:
        logging.error(f"Failed to connect to Salesforce: {e}")
        raise


def extract_agency_folders(sf=None):
    """
    Extract agency folders from Salesforce using the query defined in config.
    
    Args:
        sf (Salesforce, optional): Salesforce connection object
        
    Returns:
        pandas.DataFrame: DataFrame containing agency folder information
    """
    try:
        # Load configuration
        config = load_config()
        
        # Connect to Salesforce if not provided
        if sf is None:
            sf = connect_to_salesforce()
        
        # Find agency folders query file path in config
        soql_query_file = None
        for query in config['salesforce']['queries']:
            if query['name'] == 'agency_folders':
                soql_query_file = query['file']
                break
        
        if not soql_query_file:
            raise ValueError("Agency folders query file not found in configuration")
            
        # Load the query from the file
        soql_query = project_load_query(soql_query_file)
        if not soql_query:
            raise ValueError(f"Failed to load SOQL query from file: {soql_query_file}")
        
        # Execute SOQL query
        logging.info("Executing SOQL query for agency folders")
        results = sf.query_all(soql_query)
        
        # Convert to DataFrame
        if not results['records']:
            logging.warning("No agency folder records returned from Salesforce")
            return pd.DataFrame()
            
        # Transform Salesforce results to DataFrame
        logging.info(f"Received {len(results['records'])} agency records from Salesforce")
        
        records = []
        for record in results['records']:
            row = {
                'Id': record.get('Id'),
                'Name': record.get('Name'),
                'RPM_Agency_ID__c': record.get('RPM_Agency_ID__c'),
                'Agency_Commission_Folder__c': record.get('Agency_Commission_Folder__c'),
                'ParentAgency': record.get('ParentAgency__r', {}).get('Name') if record.get('ParentAgency__r') else None
            }
            records.append(row)
        
        df = pd.DataFrame(records)
        
        # Rename columns to match our convention
        df = df.rename(columns={
            'RPM_Agency_ID__c': 'RPM_Agency_ID',
            'Agency_Commission_Folder__c': 'Agency_Commission_Folder'
        })
        
        return df
    except Exception as e:
        logging.error(f"Failed to extract agency folders: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    # For testing/development purposes
    logging.basicConfig(level=logging.INFO)
    try:
        sf = connect_to_salesforce()
        agencies_df = extract_agency_folders(sf)
        print(f"Retrieved {len(agencies_df)} agency records")
        print(agencies_df.head())
    except Exception as e:
        print(f"Error: {e}")
