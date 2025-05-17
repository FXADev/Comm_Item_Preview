"""
Unit Tests for SharePoint Upload Write Statements Module

Tests the functionality in sp_write_statements.py including:
- Folder name mappings
- File path construction
- Path validation
- File grouping and preparation
- File writing operations
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.absolute()))
from sharepoint_upload.sp_write_statements import (
    apply_folder_name_mappings, build_file_paths,
    validate_and_log_paths, group_and_prepare_files,
    write_excel_files
)


class TestFolderMappings(unittest.TestCase):
    """Test folder name mapping functionality."""
    
    def test_jett_enterprises_mapping(self):
        """Test the Jett Enterprises folder mapping logic."""
        # Create test dataframe
        df = pd.DataFrame({
            'Agency_Group': ['Jett Enterprises + Subs', 'Regular Agency'],
            'Agency_Commission_Folder': ['Original Folder', 'Regular Folder']
        })
        
        # Apply mappings
        result = apply_folder_name_mappings(df)
        
        # Verify mapping was applied correctly
        self.assertEqual(result.loc[0, 'Agency_Commission_Folder'], "Jett Enterprises, Inc. (ACH)")
        self.assertEqual(result.loc[1, 'Agency_Commission_Folder'], "Regular Folder")
    
    def test_complete_communications_mapping(self):
        """Test the Complete Communications folder mapping logic."""
        # Create test dataframe
        df = pd.DataFrame({
            'Agency_Group': ['Complete Communications Group', 'Another Agency'],
            'Agency_Commission_Folder': ['Original Folder', 'Another Folder']
        })
        
        # Apply mappings
        result = apply_folder_name_mappings(df)
        
        # Verify mapping was applied correctly
        self.assertEqual(result.loc[0, 'Agency_Commission_Folder'], "Complete Communications")
        self.assertEqual(result.loc[1, 'Agency_Commission_Folder'], "Another Folder")
    
    def test_multiple_mappings_in_one_dataframe(self):
        """Test multiple folder mappings in a single dataframe."""
        # Create test dataframe with both mapping cases
        df = pd.DataFrame({
            'Agency_Group': ['Jett Enterprises + Subs', 'Complete Communications Inc.', 'Regular Agency'],
            'Agency_Commission_Folder': ['Jett Original', 'CC Original', 'Regular Folder']
        })
        
        # Apply mappings
        result = apply_folder_name_mappings(df)
        
        # Verify mappings were applied correctly
        self.assertEqual(result.loc[0, 'Agency_Commission_Folder'], "Jett Enterprises, Inc. (ACH)")
        self.assertEqual(result.loc[1, 'Agency_Commission_Folder'], "Complete Communications")
        self.assertEqual(result.loc[2, 'Agency_Commission_Folder'], "Regular Folder")


class TestFilePathConstruction(unittest.TestCase):
    """Test file path construction functionality."""
    
    def setUp(self):
        """Set up common test data."""
        # Create test commission data
        self.commission_data = pd.DataFrame({
            'RPM_Payout_Agency_ID': ['A001', 'A002', 'A003'],
            'Agency_Name': ['Agency 1', 'Agency 2', 'Agency 3']
        })
        
        # Create test agency folders data
        self.agency_folders = pd.DataFrame({
            'RPM_Agency_ID': ['A001', 'A002', 'A003'],
            'Agency_Commission_Folder': ['Folder1', 'Folder2', 'Folder3']
        })
        
        # Create test agency groups data
        self.agency_groups = pd.DataFrame({
            'RPM_Agency_ID': ['A001', 'A002', 'A003'],
            'Agency_Group': ['Regular Group', 'Jett Enterprises + Subs', 'Complete Communications Group']
        })
        
        # Test parameters
        self.run_ym = '202505'
        self.output_base_path = 'C:/test/path'
    
    @patch('sharepoint_upload.sp_write_statements.logging')
    def test_build_file_paths_with_agency_groups(self, mock_logging):
        """Test building file paths with agency group data."""
        # Execute
        result = build_file_paths(
            self.commission_data,
            self.agency_folders,
            self.agency_groups,
            self.run_ym,
            self.output_base_path
        )
        
        # Verify paths were constructed correctly
        self.assertEqual(len(result), 3)
        self.assertTrue('FullPath' in result.columns)
        self.assertTrue('PathLength' in result.columns)
        
        # Check the mappings were applied
        paths = result['FullPath'].tolist()
        self.assertIn(os.path.join(self.output_base_path, 'Folder1', '2025', f"{self.run_ym}_A001.xlsx"), paths)
        self.assertIn(os.path.join(self.output_base_path, 'Jett Enterprises, Inc. (ACH)', '2025', f"{self.run_ym}_A002.xlsx"), paths)
        self.assertIn(os.path.join(self.output_base_path, 'Complete Communications', '2025', f"{self.run_ym}_A003.xlsx"), paths)
    
    @patch('sharepoint_upload.sp_write_statements.logging')
    def test_build_file_paths_without_agency_groups(self, mock_logging):
        """Test building file paths without agency group data."""
        # Execute
        result = build_file_paths(
            self.commission_data,
            self.agency_folders,
            pd.DataFrame(),  # Empty agency groups
            self.run_ym,
            self.output_base_path
        )
        
        # Verify paths were constructed (without mapping)
        self.assertEqual(len(result), 3)
        paths = result['FullPath'].tolist()
        for i in range(1, 4):
            expected_path = os.path.join(self.output_base_path, f'Folder{i}', '2025', f"{self.run_ym}_A00{i}.xlsx")
            self.assertIn(expected_path, paths)


class TestPathValidationAndLogging(unittest.TestCase):
    """Test path validation and logging functionality."""
    
    @patch('pandas.DataFrame.to_csv')
    def test_validate_and_log_paths_valid_paths(self, mock_to_csv):
        """Test validation with valid paths."""
        # Setup
        merged_df = pd.DataFrame({
            'RPM_Payout_Agency_ID': ['A001', 'A002'],
            'Agency_Name': ['Agency 1', 'Agency 2'],
            'Agency_Commission_Folder': ['Folder1', 'Folder2'],
            'FullPath': ['/path/file1.xlsx', '/path/file2.xlsx'],
            'PathLength': [16, 16]
        })
        log_file = 'test_log.csv'
        
        # Execute
        valid_df = validate_and_log_paths(merged_df, log_file)
        
        # Verify
        self.assertEqual(len(valid_df), 2)
        self.assertEqual(mock_to_csv.call_count, 0)  # No entries skipped, no CSV writes
    
    @patch('pandas.DataFrame.to_csv')
    def test_validate_and_log_paths_missing_folders(self, mock_to_csv):
        """Test validation with missing folders."""
        # Setup
        merged_df = pd.DataFrame({
            'RPM_Payout_Agency_ID': ['A001', 'A002'],
            'Agency_Name': ['Agency 1', 'Agency 2'],
            'Agency_Commission_Folder': ['Folder1', None],
            'FullPath': ['/path/file1.xlsx', None],
            'PathLength': [16, 0]
        })
        log_file = 'test_log.csv'
        
        # Execute
        valid_df = validate_and_log_paths(merged_df, log_file)
        
        # Verify
        self.assertEqual(len(valid_df), 1)  # Only one valid entry
        self.assertEqual(mock_to_csv.call_count, 1)  # One CSV write for skipped entry
    
    @patch('pandas.DataFrame.to_csv')
    def test_validate_and_log_paths_long_paths(self, mock_to_csv):
        """Test validation with long paths."""
        # Setup
        merged_df = pd.DataFrame({
            'RPM_Payout_Agency_ID': ['A001', 'A002'],
            'Agency_Name': ['Agency 1', 'Agency 2'],
            'Agency_Commission_Folder': ['Folder1', 'Folder2'],
            'FullPath': ['/path/file1.xlsx', '/path/' + 'x' * 256],
            'PathLength': [16, 262]
        })
        log_file = 'test_log.csv'
        
        # Execute
        valid_df = validate_and_log_paths(merged_df, log_file)
        
        # Verify
        self.assertEqual(len(valid_df), 1)  # Only one valid entry
        self.assertEqual(mock_to_csv.call_count, 1)  # One CSV write for skipped entry


class TestGroupingAndFilePreperation(unittest.TestCase):
    """Test grouping and file preparation functionality."""
    
    @patch('pandas.DataFrame.to_csv')
    def test_group_and_prepare_files(self, mock_to_csv):
        """Test grouping and preparing files."""
        # Setup
        valid_df = pd.DataFrame({
            'RPM_Payout_Agency_ID': ['A001', 'A001', 'A002'],
            'Agency_Name': ['Agency 1', 'Agency 1', 'Agency 2'],
            'FullPath': ['/path/A001.xlsx', '/path/A001.xlsx', '/path/A002.xlsx'],
            'Customer_Name': ['Customer 1', 'Customer 2', 'Customer 3'],
            'Commission_Amt': [100, 200, 300]
        })
        run_ym = '202505'
        log_file = 'test_log.csv'
        
        # Execute
        grouped_data = group_and_prepare_files(valid_df, run_ym, log_file)
        
        # Verify
        self.assertEqual(len(grouped_data), 2)  # Two unique agencies
        self.assertEqual(grouped_data[0]['agency_id'], 'A001')
        self.assertEqual(grouped_data[1]['agency_id'], 'A002')
        self.assertEqual(len(grouped_data[0]['data']), 2)  # Two rows for Agency 1
        self.assertEqual(len(grouped_data[1]['data']), 1)  # One row for Agency 2
        self.assertEqual(mock_to_csv.call_count, 2)  # Two log entries created


class TestFileWriting(unittest.TestCase):
    """Test file writing functionality."""
    
    @patch('os.makedirs')
    @patch('pandas.DataFrame.to_excel')
    @patch('sharepoint_upload.sp_write_statements.update_log_entry')
    def test_write_excel_files_small_dataset(self, mock_update_log, mock_to_excel, mock_makedirs):
        """Test writing small Excel files."""
        # Setup
        data1 = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        data2 = pd.DataFrame({'col1': [3], 'col2': ['c']})
        
        grouped_data = [
            {'agency_id': 'A001', 'agency_name': 'Agency 1', 'file_path': '/path/A001.xlsx', 'data': data1, 'row_count': 2},
            {'agency_id': 'A002', 'agency_name': 'Agency 2', 'file_path': '/path/A002.xlsx', 'data': data2, 'row_count': 1}
        ]
        
        # Execute
        success_count, failure_count = write_excel_files(grouped_data, 'test_log.csv', user_confirmed=True, chunk_size=10)
        
        # Verify
        self.assertEqual(success_count, 2)
        self.assertEqual(failure_count, 0)
        self.assertEqual(mock_makedirs.call_count, 2)
        self.assertEqual(mock_to_excel.call_count, 2)
        self.assertEqual(mock_update_log.call_count, 2)
    
    @patch('os.makedirs')
    @patch('pandas.ExcelWriter')
    @patch('sharepoint_upload.sp_write_statements.update_log_entry')
    @patch('gc.collect')
    def test_write_excel_files_large_dataset(self, mock_gc, mock_update_log, mock_excel_writer, mock_makedirs):
        """Test writing large Excel files with chunking."""
        # Setup - create a large dataframe
        large_data = pd.DataFrame({'col1': list(range(15000)), 'col2': ['x'] * 15000})
        
        grouped_data = [
            {'agency_id': 'A001', 'agency_name': 'Agency 1', 'file_path': '/path/A001.xlsx', 'data': large_data, 'row_count': 15000}
        ]
        
        # Mock ExcelWriter context manager
        mock_writer = MagicMock()
        mock_excel_writer.return_value.__enter__.return_value = mock_writer
        mock_writer.sheets = {'Commission Data': MagicMock()}
        
        # Execute - with chunk_size smaller than data size to trigger chunking
        success_count, failure_count = write_excel_files(grouped_data, 'test_log.csv', user_confirmed=True, chunk_size=5000)
        
        # Verify
        self.assertEqual(success_count, 1)
        self.assertEqual(failure_count, 0)
        self.assertEqual(mock_excel_writer.call_count, 1)
        self.assertTrue(mock_gc.called)  # Garbage collection should be called
    
    @patch('os.makedirs')
    @patch('pandas.DataFrame.to_excel', side_effect=Exception("Write error"))
    @patch('sharepoint_upload.sp_write_statements.update_log_entry')
    def test_write_excel_files_with_error(self, mock_update_log, mock_to_excel, mock_makedirs):
        """Test handling errors when writing Excel files."""
        # Setup
        data = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        
        grouped_data = [
            {'agency_id': 'A001', 'agency_name': 'Agency 1', 'file_path': '/path/A001.xlsx', 'data': data, 'row_count': 2}
        ]
        
        # Execute
        success_count, failure_count = write_excel_files(grouped_data, 'test_log.csv', user_confirmed=True)
        
        # Verify
        self.assertEqual(success_count, 0)
        self.assertEqual(failure_count, 1)
        mock_update_log.assert_called_with('test_log.csv', '/path/A001.xlsx', 'Failed')


if __name__ == '__main__':
    unittest.main()
