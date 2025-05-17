"""
Unit Tests for SharePoint Upload Data Extractors

Tests the data extraction modules:
- sp_extract_sql.py
- sp_extract_soql.py
- sp_extract_redshift.py

Focus on connection handling, query loading, error handling,
and data transformation.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import pyodbc
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.absolute()))
from sharepoint_upload.sp_extract_sql import connect_to_sql, extract_commission_data, store_execution_notification
from sharepoint_upload.sp_extract_soql import connect_to_salesforce, extract_agency_folders
from sharepoint_upload.sp_extract_redshift import connect_to_redshift, extract_agency_groups


class TestSQLExtractor(unittest.TestCase):
    """Test SQL Server extractor functionality."""
    
    @patch.dict(os.environ, {
        'SQL_SERVER': 'test-server',
        'SQL_DATABASE': 'test-db',
        'SQL_USERNAME': 'test-user',
        'SQL_PASSWORD': 'test-pass',
        'SQL_DRIVER': '{ODBC Driver}'
    })
    @patch('pyodbc.connect')
    def test_connect_to_sql(self, mock_connect):
        """Test SQL Server connection with environment variables."""
        # Setup
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Execute
        conn = connect_to_sql()
        
        # Verify
        self.assertEqual(conn, mock_conn)
        mock_connect.assert_called_once()
        connection_string = mock_connect.call_args[0][0]
        self.assertIn('test-server', connection_string)
        self.assertIn('test-db', connection_string)
        self.assertIn('test-user', connection_string)
        self.assertIn('test-pass', connection_string)
    
    @patch.dict(os.environ, {'SQL_SERVER': ''})
    def test_connect_to_sql_missing_env_vars(self):
        """Test SQL Server connection with missing environment variables."""
        # Verify that it raises an exception
        with self.assertRaises(ValueError):
            connect_to_sql()
    
    @patch('sharepoint_upload.sp_extract_sql.connect_to_sql')
    @patch('sharepoint_upload.sp_extract_sql.project_load_query')
    @patch('pandas.read_sql')
    def test_extract_commission_data_with_chunks(self, mock_read_sql, mock_load_query, mock_connect):
        """Test extracting commission data with chunking."""
        # Setup
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Mock the query loading
        mock_load_query.return_value = "SELECT * FROM CommissionItems WHERE Run_YM = '${run_ym}'"
        
        # Create mock chunks
        chunk1 = pd.DataFrame({
            'RPM_Payout_Agency_ID': ['A001', 'A002'],
            'Agency_Name': ['Agency 1', 'Agency 2'],
            'Commission_Amt': [100, 200]
        })
        chunk2 = pd.DataFrame({
            'RPM_Payout_Agency_ID': ['A003'],
            'Agency_Name': ['Agency 3'],
            'Commission_Amt': [300]
        })
        
        # Set up mock_read_sql to return an iterator of chunks
        mock_read_sql.return_value = [chunk1, chunk2]
        
        # Execute
        result = extract_commission_data('202505', mock_conn, chunksize=2)
        
        # Verify
        self.assertEqual(len(result), 3)  # Combined data from both chunks
        self.assertEqual(mock_read_sql.call_count, 1)
    
    @patch('sharepoint_upload.sp_extract_sql.connect_to_sql')
    @patch('pyodbc.Connection')
    @patch('pyodbc.Cursor')
    def test_store_execution_notification(self, mock_cursor, mock_connection, mock_connect):
        """Test storing execution notifications in SQL."""
        # Setup
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Execute
        result = store_execution_notification('202505', 'Success', 'Test message', mock_conn)
        
        # Verify
        self.assertTrue(result)
        self.assertEqual(mock_cursor.execute.call_count, 2)  # Table check and insert
        self.assertEqual(mock_conn.commit.call_count, 2)


class TestSalesforceExtractor(unittest.TestCase):
    """Test Salesforce extractor functionality."""
    
    @patch.dict(os.environ, {
        'SF_USERNAME': 'test@example.com',
        'SF_PASSWORD': 'test-pass',
        'SF_SECURITY_TOKEN': 'test-token',
        'SF_DOMAIN': 'test'
    })
    @patch('simple_salesforce.Salesforce')
    def test_connect_to_salesforce(self, mock_sf):
        """Test Salesforce connection with environment variables."""
        # Setup
        mock_sf_instance = MagicMock()
        mock_sf.return_value = mock_sf_instance
        
        # Execute
        sf = connect_to_salesforce()
        
        # Verify
        self.assertEqual(sf, mock_sf_instance)
        mock_sf.assert_called_once_with(
            username='test@example.com',
            password='test-pass',
            security_token='test-token',
            domain='test'
        )
    
    @patch.dict(os.environ, {'SF_USERNAME': ''})
    def test_connect_to_salesforce_missing_env_vars(self):
        """Test Salesforce connection with missing environment variables."""
        # Verify that it raises an exception
        with self.assertRaises(ValueError):
            connect_to_salesforce()
    
    @patch('sharepoint_upload.sp_extract_soql.connect_to_salesforce')
    @patch('sharepoint_upload.sp_extract_soql.load_config')
    @patch('sharepoint_upload.sp_extract_soql.project_load_query')
    def test_extract_agency_folders(self, mock_load_query, mock_load_config, mock_connect):
        """Test extracting agency folders from Salesforce."""
        # Setup
        mock_sf = MagicMock()
        mock_connect.return_value = mock_sf
        
        # Mock config loading
        mock_config = {
            'salesforce': {
                'queries': [
                    {'name': 'agency_folders', 'file': 'soql/test.soql'}
                ]
            }
        }
        mock_load_config.return_value = mock_config
        
        # Mock query loading
        mock_load_query.return_value = "SELECT Id, Name FROM Account"
        
        # Mock Salesforce query results
        mock_sf.query_all.return_value = {
            'records': [
                {
                    'Id': 'id1',
                    'Name': 'Agency 1',
                    'RPM_Agency_ID__c': 'A001',
                    'Agency_Commission_Folder__c': 'Folder1',
                    'ParentAgency__r': {'Name': 'Parent 1'}
                },
                {
                    'Id': 'id2',
                    'Name': 'Agency 2',
                    'RPM_Agency_ID__c': 'A002',
                    'Agency_Commission_Folder__c': 'Folder2',
                    'ParentAgency__r': None
                }
            ]
        }
        
        # Execute
        result = extract_agency_folders(mock_sf)
        
        # Verify
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]['RPM_Agency_ID'], 'A001')
        self.assertEqual(result.iloc[0]['Agency_Commission_Folder'], 'Folder1')
        self.assertEqual(result.iloc[1]['RPM_Agency_ID'], 'A002')
        self.assertEqual(result.iloc[1]['Agency_Commission_Folder'], 'Folder2')


class TestRedshiftExtractor(unittest.TestCase):
    """Test Redshift extractor functionality."""
    
    @patch.dict(os.environ, {
        'REDSHIFT_HOST': 'test-host',
        'REDSHIFT_PORT': '5439',
        'REDSHIFT_DATABASE': 'test-db',
        'REDSHIFT_USER': 'test-user',
        'REDSHIFT_PASSWORD': 'test-pass',
        'REDSHIFT_SCHEMA': 'test-schema'
    })
    @patch('psycopg2.connect')
    def test_connect_to_redshift(self, mock_connect):
        """Test Redshift connection with environment variables."""
        # Setup
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Execute
        with patch('sharepoint_upload.sp_extract_redshift.psycopg2.connect', mock_connect):
            conn = connect_to_redshift()
        
        # Verify
        self.assertEqual(conn, mock_conn)
        mock_connect.assert_called_once()
        connection_string = mock_connect.call_args[0][0]
        self.assertIn('test-host', connection_string)
        self.assertIn('5439', connection_string)
        self.assertIn('test-db', connection_string)
        self.assertIn('test-user', connection_string)
        self.assertIn('test-pass', connection_string)
    
    @patch('sharepoint_upload.sp_extract_redshift.connect_to_redshift')
    @patch('sharepoint_upload.sp_extract_redshift.load_config')
    @patch('sharepoint_upload.sp_extract_redshift.project_load_query')
    @patch('pandas.read_sql')
    def test_extract_agency_groups_with_chunks(self, mock_read_sql, mock_load_query, mock_load_config, mock_connect):
        """Test extracting agency groups with chunking."""
        # Setup
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Mock config loading
        mock_config = {
            'redshift': {
                'queries': [
                    {'name': 'agency_groups', 'file': 'redshift/test.sql'}
                ]
            }
        }
        mock_load_config.return_value = mock_config
        
        # Mock query loading
        mock_load_query.return_value = "SELECT agency_id, agency_name, agency_group FROM agency"
        
        # Create mock chunks
        chunk1 = pd.DataFrame({
            'rpm_agency_id': [1, 2],
            'agency_name': ['Agency 1', 'Agency 2'],
            'agency_group': ['Group 1', 'Group 2']
        })
        chunk2 = pd.DataFrame({
            'rpm_agency_id': [3],
            'agency_name': ['Agency 3'],
            'agency_group': ['Group 3']
        })
        
        # Set up mock_read_sql to return an iterator of chunks
        mock_read_sql.return_value = [chunk1, chunk2]
        
        # Execute
        result = extract_agency_groups(chunksize=2)
        
        # Verify
        self.assertEqual(len(result), 3)  # Combined data from both chunks
        
        # Verify ID format conversion
        self.assertEqual(result.iloc[0]['RPM_Agency_ID'], 'bpt3__1')
        self.assertEqual(result.iloc[1]['RPM_Agency_ID'], 'bpt3__2')
        self.assertEqual(result.iloc[2]['RPM_Agency_ID'], 'bpt3__3')


if __name__ == '__main__':
    unittest.main()
