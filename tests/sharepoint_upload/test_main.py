"""
Integration Tests for SharePoint Upload Main Workflow

Tests the integration flow in sp_main.py including:
- Configuration loading
- Data extraction coordination
- File preparation logic
- Upload process
- User confirmation handling
- Error handling and rollback
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock, mock_open, call
import pandas as pd
import io
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.absolute()))
from sharepoint_upload.sp_main import (
    load_configuration, extract_data, prepare_files,
    process_upload, main
)


class TestMainIntegration(unittest.TestCase):
    """Test the main workflow integration."""
    
    @patch('sharepoint_upload.sp_main.load_config')
    def test_load_configuration(self, mock_load_config):
        """Test configuration loading."""
        # Setup
        mock_config = {
            'run_params': {'output_path': '/test/path'},
            'sql': {'queries': []},
            'salesforce': {'queries': []},
            'redshift': {'queries': []}
        }
        mock_load_config.return_value = mock_config
        
        # Execute
        config = load_configuration()
        
        # Verify
        self.assertEqual(config, mock_config)
        mock_load_config.assert_called_once()
    
    @patch('sharepoint_upload.sp_main.connect_to_sql')
    @patch('sharepoint_upload.sp_main.extract_commission_data')
    @patch('sharepoint_upload.sp_main.connect_to_salesforce')
    @patch('sharepoint_upload.sp_main.extract_agency_folders')
    @patch('sharepoint_upload.sp_main.connect_to_redshift')
    @patch('sharepoint_upload.sp_main.extract_agency_groups')
    def test_extract_data(self, mock_extract_groups, mock_connect_redshift, 
                          mock_extract_folders, mock_connect_sf, 
                          mock_extract_commission, mock_connect_sql):
        """Test data extraction coordination."""
        # Setup mocks
        mock_sql_conn = MagicMock()
        mock_connect_sql.return_value = mock_sql_conn
        
        mock_sf_conn = MagicMock()
        mock_connect_sf.return_value = mock_sf_conn
        
        mock_rs_conn = MagicMock()
        mock_connect_redshift.return_value = mock_rs_conn
        
        # Mock dataframes
        commission_df = pd.DataFrame({
            'RPM_Payout_Agency_ID': ['A001', 'A002'],
            'Agency_Name': ['Agency 1', 'Agency 2'],
            'Commission_Amt': [100, 200]
        })
        mock_extract_commission.return_value = commission_df
        
        folder_df = pd.DataFrame({
            'RPM_Agency_ID': ['A001', 'A002'],
            'Agency_Commission_Folder': ['Folder1', 'Folder2']
        })
        mock_extract_folders.return_value = folder_df
        
        group_df = pd.DataFrame({
            'RPM_Agency_ID': ['A001', 'A002'],
            'Agency_Group': ['Group1', 'Group2']
        })
        mock_extract_groups.return_value = group_df
        
        # Execute
        comm_data, agency_folders, agency_groups = extract_data('202505')
        
        # Verify
        self.assertEqual(len(comm_data), 2)
        self.assertEqual(len(agency_folders), 2)
        self.assertEqual(len(agency_groups), 2)
        
        mock_connect_sql.assert_called_once()
        mock_extract_commission.assert_called_once_with('202505', mock_sql_conn)
        mock_connect_sf.assert_called_once()
        mock_extract_folders.assert_called_once_with(mock_sf_conn)
        mock_connect_redshift.assert_called_once()
        mock_extract_groups.assert_called_once()
    
    @patch('sharepoint_upload.sp_main.build_file_paths')
    @patch('sharepoint_upload.sp_main.validate_and_log_paths')
    @patch('sharepoint_upload.sp_main.group_and_prepare_files')
    def test_prepare_files(self, mock_group_prepare, mock_validate, mock_build_paths):
        """Test file preparation logic."""
        # Setup mocks
        comm_data = pd.DataFrame({
            'RPM_Payout_Agency_ID': ['A001', 'A002'],
            'Agency_Name': ['Agency 1', 'Agency 2'],
            'Commission_Amt': [100, 200]
        })
        
        agency_folders = pd.DataFrame({
            'RPM_Agency_ID': ['A001', 'A002'],
            'Agency_Commission_Folder': ['Folder1', 'Folder2']
        })
        
        agency_groups = pd.DataFrame({
            'RPM_Agency_ID': ['A001', 'A002'],
            'Agency_Group': ['Group1', 'Group2']
        })
        
        merged_df = pd.DataFrame({
            'RPM_Payout_Agency_ID': ['A001', 'A002'],
            'Agency_Name': ['Agency 1', 'Agency 2'],
            'Agency_Commission_Folder': ['Folder1', 'Folder2'],
            'FullPath': ['/path/A001.xlsx', '/path/A002.xlsx']
        })
        mock_build_paths.return_value = merged_df
        mock_validate.return_value = merged_df
        
        expected_grouped_data = [
            {'agency_id': 'A001', 'file_path': '/path/A001.xlsx', 'data': pd.DataFrame()},
            {'agency_id': 'A002', 'file_path': '/path/A002.xlsx', 'data': pd.DataFrame()}
        ]
        mock_group_prepare.return_value = expected_grouped_data
        
        # Execute
        log_file = 'test_log.csv'
        base_path = '/test/path'
        run_ym = '202505'
        grouped_data = prepare_files(comm_data, agency_folders, agency_groups, log_file, base_path, run_ym)
        
        # Verify
        self.assertEqual(grouped_data, expected_grouped_data)
        mock_build_paths.assert_called_once()
        mock_validate.assert_called_once()
        mock_group_prepare.assert_called_once()
    
    @patch('sharepoint_upload.sp_main.input', return_value='y')
    @patch('sharepoint_upload.sp_main.write_excel_files')
    @patch('sharepoint_upload.sp_main.delete_unconfirmed_files')
    def test_process_upload_confirmed(self, mock_delete, mock_write, mock_input):
        """Test upload process with user confirmation."""
        # Setup
        grouped_data = [
            {'agency_id': 'A001', 'agency_name': 'Agency 1', 'file_path': '/path/A001.xlsx', 'data': pd.DataFrame()},
            {'agency_id': 'A002', 'agency_name': 'Agency 2', 'file_path': '/path/A002.xlsx', 'data': pd.DataFrame()}
        ]
        
        log_file = 'test_log.csv'
        mock_write.return_value = (2, 0)  # 2 successful, 0 failed
        
        # Execute
        result = process_upload(grouped_data, log_file)
        
        # Verify
        self.assertEqual(result, (2, 0))
        mock_input.assert_called_once()
        mock_write.assert_called_once_with(grouped_data, log_file, user_confirmed=True)
        mock_delete.assert_not_called()  # No deletion when confirmed
    
    @patch('sharepoint_upload.sp_main.input', return_value='n')
    @patch('sharepoint_upload.sp_main.write_excel_files')
    @patch('sharepoint_upload.sp_main.delete_unconfirmed_files')
    def test_process_upload_cancelled(self, mock_delete, mock_write, mock_input):
        """Test upload process with user cancellation."""
        # Setup
        grouped_data = [
            {'agency_id': 'A001', 'agency_name': 'Agency 1', 'file_path': '/path/A001.xlsx', 'data': pd.DataFrame()},
            {'agency_id': 'A002', 'agency_name': 'Agency 2', 'file_path': '/path/A002.xlsx', 'data': pd.DataFrame()}
        ]
        
        log_file = 'test_log.csv'
        mock_delete.return_value = ['/path/A001.xlsx', '/path/A002.xlsx']
        
        # Execute
        result = process_upload(grouped_data, log_file)
        
        # Verify
        self.assertEqual(result, (0, 0))
        mock_input.assert_called_once()
        mock_write.assert_not_called()
        mock_delete.assert_called_once_with(log_file)
    
    @patch('sharepoint_upload.sp_main.load_configuration')
    @patch('sharepoint_upload.sp_main.setup_logging')
    @patch('sharepoint_upload.sp_main.extract_data')
    @patch('sharepoint_upload.sp_main.prepare_files')
    @patch('sharepoint_upload.sp_main.process_upload')
    @patch('sharepoint_upload.sp_main.store_execution_notification')
    @patch('sharepoint_upload.sp_main.generate_summary')
    @patch('sharepoint_upload.sp_main.datetime')
    def test_main_success_flow(self, mock_datetime, mock_generate_summary,
                              mock_store_notification, mock_process_upload,
                              mock_prepare_files, mock_extract_data,
                              mock_setup_logging, mock_load_config):
        """Test main workflow with successful execution."""
        # Setup mocks
        mock_config = {
            'run_params': {
                'output_path': '/test/path',
                'run_ym': '202505'
            }
        }
        mock_load_config.return_value = mock_config
        mock_setup_logging.return_value = 'test_log.csv'
        
        # Mock data
        comm_data = pd.DataFrame({'RPM_Payout_Agency_ID': ['A001', 'A002']})
        agency_folders = pd.DataFrame({'RPM_Agency_ID': ['A001', 'A002']})
        agency_groups = pd.DataFrame({'RPM_Agency_ID': ['A001', 'A002']})
        mock_extract_data.return_value = (comm_data, agency_folders, agency_groups)
        
        # Mock file preparation
        grouped_data = [{'agency_id': 'A001'}, {'agency_id': 'A002'}]
        mock_prepare_files.return_value = grouped_data
        
        # Mock upload process
        mock_process_upload.return_value = (2, 0)  # 2 successful, 0 failed
        
        # Mock datetime
        mock_start_time = MagicMock()
        mock_datetime.now.return_value = mock_start_time
        
        # Mock summary
        mock_summary = {'run_ym': '202505', 'total_succeeded': 2}
        mock_generate_summary.return_value = mock_summary
        
        # Execute
        main()
        
        # Verify the entire workflow
        mock_load_config.assert_called_once()
        mock_setup_logging.assert_called_once()
        mock_extract_data.assert_called_once_with('202505')
        mock_prepare_files.assert_called_once_with(
            comm_data, agency_folders, agency_groups,
            'test_log.csv', '/test/path', '202505'
        )
        mock_process_upload.assert_called_once_with(grouped_data, 'test_log.csv')
        mock_store_notification.assert_called_once()
        mock_generate_summary.assert_called_once_with('test_log.csv', '202505', mock_start_time)


if __name__ == '__main__':
    unittest.main()
