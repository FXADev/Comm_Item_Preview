"""
Unit Tests for SharePoint Upload Utilities

Tests the utility functions in sp_utils.py including:
- Configuration loading
- Path validation
- Log file operations
- File operations
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock, mock_open
import tempfile
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.absolute()))
from sharepoint_upload.sp_utils import (
    load_config, validate_path_length, setup_logging,
    safe_write_file, update_log_entry, delete_unconfirmed_files,
    generate_summary
)


class TestConfigLoading(unittest.TestCase):
    """Test configuration loading functionality."""
    
    @patch('yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open, read_data="key: value")
    def test_load_config(self, mock_file, mock_yaml_load):
        """Test that load_config properly loads and returns config."""
        # Setup
        expected_config = {"key": "value"}
        mock_yaml_load.return_value = expected_config
        
        # Execute
        result = load_config("dummy_path.yml")
        
        # Verify
        mock_file.assert_called_once()
        mock_yaml_load.assert_called_once()
        self.assertEqual(result, expected_config)

    @patch('builtins.open', side_effect=Exception("File not found"))
    @patch('logging.error')
    @patch('sys.exit')
    def test_load_config_failure(self, mock_exit, mock_logging, mock_file):
        """Test load_config handles failures properly."""
        # Execute
        load_config("nonexistent_file.yml")
        
        # Verify
        mock_logging.assert_called_once()
        mock_exit.assert_called_once_with(1)


class TestPathValidation(unittest.TestCase):
    """Test path validation functionality."""
    
    def test_valid_path_length(self):
        """Test that paths under 256 characters are valid."""
        # Test short path
        self.assertTrue(validate_path_length("C:/short_path.txt"))
        
        # Test path at exactly 255 characters
        path_at_limit = "C:/" + "a" * 248 + ".txt"  # 255 chars total: 3 + 248 + 4 = 255
        self.assertTrue(validate_path_length(path_at_limit))
    
    def test_invalid_path_length(self):
        """Test that paths over 255 characters are invalid."""
        # Test path exceeding limit
        path_over_limit = "C:/" + "a" * 249 + ".txt"  # 256 chars total: 3 + 249 + 4 = 256
        self.assertFalse(validate_path_length(path_over_limit))


class TestLoggingFunctions(unittest.TestCase):
    """Test logging setup and operations."""
    
    @patch('pandas.DataFrame.to_csv')
    @patch('pathlib.Path.mkdir')
    def test_setup_logging(self, mock_mkdir, mock_to_csv):
        """Test that setup_logging creates log directory and file."""
        # Execute
        log_file = setup_logging("202505")
        
        # Verify
        mock_mkdir.assert_called_once()
        mock_to_csv.assert_called_once()
        self.assertIn("202505_upload_log.csv", log_file)


class TestFileOperations(unittest.TestCase):
    """Test file operations."""
    
    @patch('os.makedirs')
    @patch('pandas.DataFrame.to_excel')
    def test_safe_write_file(self, mock_to_excel, mock_makedirs):
        """Test safe file writing."""
        # Setup
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        
        # Execute
        success, error = safe_write_file(df, "/path/to/file.xlsx")
        
        # Verify
        mock_makedirs.assert_called_once()
        mock_to_excel.assert_called_once()
        self.assertTrue(success)
        self.assertIsNone(error)
    
    @patch('os.makedirs', side_effect=Exception("Permission denied"))
    def test_safe_write_file_failure(self, mock_makedirs):
        """Test safe file writing handles errors."""
        # Setup
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        
        # Execute
        success, error = safe_write_file(df, "/path/to/file.xlsx")
        
        # Verify
        self.assertFalse(success)
        self.assertIsNotNone(error)
        self.assertIn("Failed to write file", error)


class TestLogEntryManagement(unittest.TestCase):
    """Test log entry management."""
    
    @patch('pandas.read_csv')
    @patch('pandas.DataFrame.to_csv')
    def test_update_log_entry(self, mock_to_csv, mock_read_csv):
        """Test updating log entries."""
        # Setup
        mock_df = pd.DataFrame({
            'FullPath': ['/path/file1.xlsx', '/path/file2.xlsx'],
            'Status': ['Pending', 'Pending']
        })
        mock_read_csv.return_value = mock_df
        
        # Execute
        result = update_log_entry('log.csv', '/path/file1.xlsx', 'Succeeded')
        
        # Verify
        self.assertTrue(result)
        mock_read_csv.assert_called_once()
        mock_to_csv.assert_called_once()
    
    @patch('pandas.read_csv', side_effect=Exception("File not found"))
    def test_update_log_entry_failure(self, mock_read_csv):
        """Test update_log_entry handles errors."""
        # Execute
        result = update_log_entry('log.csv', '/path/file.xlsx', 'Succeeded')
        
        # Verify
        self.assertFalse(result)
        mock_read_csv.assert_called_once()


class TestRollbackFunctionality(unittest.TestCase):
    """Test rollback functionality."""
    
    @patch('os.path.exists', return_value=True)
    @patch('os.remove')
    @patch('pandas.read_csv')
    @patch('pandas.DataFrame.to_csv')
    def test_delete_unconfirmed_files(self, mock_to_csv, mock_read_csv, mock_remove, mock_exists):
        """Test deleting unconfirmed files."""
        # Setup
        mock_df = pd.DataFrame({
            'FullPath': ['/path/file1.xlsx', '/path/file2.xlsx'],
            'Status': ['Pending', 'Succeeded']
        })
        mock_read_csv.return_value = mock_df
        
        # Execute
        deleted_files = delete_unconfirmed_files('log.csv')
        
        # Verify
        self.assertEqual(len(deleted_files), 1)
        self.assertEqual(deleted_files[0], '/path/file1.xlsx')
        mock_remove.assert_called_once_with('/path/file1.xlsx')


class TestReportingFunctions(unittest.TestCase):
    """Test reporting functionality."""
    
    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open)
    def test_generate_summary(self, mock_file, mock_read_csv):
        """Test generating execution summary."""
        # Setup
        mock_df = pd.DataFrame({
            'Status': ['Succeeded', 'Failed', 'Skipped', 'RolledBack', 'Succeeded']
        })
        mock_read_csv.return_value = mock_df
        start_time = datetime.now()
        
        # Execute
        summary = generate_summary('log.csv', '202505', start_time)
        
        # Verify
        self.assertEqual(summary['run_ym'], '202505')
        self.assertEqual(summary['total_files_attempted'], 5)
        self.assertEqual(summary['total_succeeded'], 2)
        self.assertEqual(summary['total_failed'], 1)
        self.assertEqual(summary['total_skipped'], 1)
        self.assertEqual(summary['total_rolled_back'], 1)


if __name__ == '__main__':
    unittest.main()
