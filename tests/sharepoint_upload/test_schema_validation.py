"""
Tests for Schema Validation in SharePoint Upload Workflow

These tests ensure that the data schema validation functions work correctly
and protect against unexpected schema changes in the source databases that
could break the ETL process.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.absolute()))

# Import the module we'll create for schema validation
from sharepoint_upload.sp_schema_validator import (
    validate_commission_schema, validate_agency_folder_schema,
    validate_agency_group_schema, get_required_columns,
    create_schema_report
)


class TestSchemaValidation(unittest.TestCase):
    """Test the schema validation functionality."""
    
    def setUp(self):
        """Set up test data."""
        # Define the expected schema for commission data
        self.expected_commission_cols = [
            'RPM_Payout_Agency_ID', 'Agency_Name', 'Customer_Name',
            'Commission_Amt', 'Commission_Type', 'Month_End_Date'
        ]
        
        # Define the expected schema for agency folders
        self.expected_agency_folder_cols = [
            'RPM_Agency_ID', 'Agency_Name', 'Agency_Commission_Folder'
        ]
        
        # Define the expected schema for agency groups
        self.expected_agency_group_cols = [
            'RPM_Agency_ID', 'Agency_Group'
        ]
        
        # Mock dataframes with correct schema
        self.valid_commission_df = pd.DataFrame({
            'RPM_Payout_Agency_ID': ['A001', 'A002'],
            'Agency_Name': ['Agency 1', 'Agency 2'],
            'Customer_Name': ['Customer 1', 'Customer 2'],
            'Commission_Amt': [100.0, 200.0],
            'Commission_Type': ['Type A', 'Type B'],
            'Month_End_Date': pd.to_datetime(['2025-05-31', '2025-05-31'])
        })
        
        self.valid_folder_df = pd.DataFrame({
            'RPM_Agency_ID': ['A001', 'A002'],
            'Agency_Name': ['Agency 1', 'Agency 2'],
            'Agency_Commission_Folder': ['Folder1', 'Folder2']
        })
        
        self.valid_group_df = pd.DataFrame({
            'RPM_Agency_ID': ['A001', 'A002'],
            'Agency_Group': ['Group1', 'Group2']
        })
        
        # Mock dataframes with invalid schema
        self.invalid_commission_df = pd.DataFrame({
            'RPM_Payout_Agency_ID': ['A001', 'A002'],
            'Agency_Name': ['Agency 1', 'Agency 2'],
            # Missing Commission_Amt and other required columns
        })
        
        self.invalid_folder_df = pd.DataFrame({
            'RPM_Agency_ID': ['A001', 'A002'],
            # Missing Agency_Commission_Folder
        })
        
        self.invalid_group_df = pd.DataFrame({
            'RPM_Agency_ID': ['A001', 'A002'],
            # Missing Agency_Group
        })
    
    @patch('sharepoint_upload.sp_schema_validator.get_required_columns')
    def test_validate_commission_schema_valid(self, mock_get_cols):
        """Test validation with valid commission schema."""
        # Setup
        mock_get_cols.return_value = self.expected_commission_cols
        
        # Execute
        is_valid, missing_cols, type_errors = validate_commission_schema(self.valid_commission_df)
        
        # Verify
        self.assertTrue(is_valid)
        self.assertEqual(len(missing_cols), 0)
        self.assertEqual(len(type_errors), 0)
    
    @patch('sharepoint_upload.sp_schema_validator.get_required_columns')
    def test_validate_commission_schema_invalid(self, mock_get_cols):
        """Test validation with invalid commission schema."""
        # Setup
        mock_get_cols.return_value = self.expected_commission_cols
        
        # Execute
        is_valid, missing_cols, type_errors = validate_commission_schema(self.invalid_commission_df)
        
        # Verify
        self.assertFalse(is_valid)
        self.assertGreater(len(missing_cols), 0)
        self.assertIn('Commission_Amt', missing_cols)
    
    @patch('sharepoint_upload.sp_schema_validator.get_required_columns')
    def test_validate_agency_folder_schema_valid(self, mock_get_cols):
        """Test validation with valid agency folder schema."""
        # Setup
        mock_get_cols.return_value = self.expected_agency_folder_cols
        
        # Execute
        is_valid, missing_cols, type_errors = validate_agency_folder_schema(self.valid_folder_df)
        
        # Verify
        self.assertTrue(is_valid)
        self.assertEqual(len(missing_cols), 0)
        self.assertEqual(len(type_errors), 0)
    
    @patch('sharepoint_upload.sp_schema_validator.get_required_columns')
    def test_validate_agency_folder_schema_invalid(self, mock_get_cols):
        """Test validation with invalid agency folder schema."""
        # Setup
        mock_get_cols.return_value = self.expected_agency_folder_cols
        
        # Execute
        is_valid, missing_cols, type_errors = validate_agency_folder_schema(self.invalid_folder_df)
        
        # Verify
        self.assertFalse(is_valid)
        self.assertGreater(len(missing_cols), 0)
        self.assertIn('Agency_Commission_Folder', missing_cols)
    
    @patch('sharepoint_upload.sp_schema_validator.get_required_columns')
    def test_validate_agency_group_schema_valid(self, mock_get_cols):
        """Test validation with valid agency group schema."""
        # Setup
        mock_get_cols.return_value = self.expected_agency_group_cols
        
        # Execute
        is_valid, missing_cols, type_errors = validate_agency_group_schema(self.valid_group_df)
        
        # Verify
        self.assertTrue(is_valid)
        self.assertEqual(len(missing_cols), 0)
        self.assertEqual(len(type_errors), 0)
    
    @patch('sharepoint_upload.sp_schema_validator.get_required_columns')
    def test_validate_agency_group_schema_invalid(self, mock_get_cols):
        """Test validation with invalid agency group schema."""
        # Setup
        mock_get_cols.return_value = self.expected_agency_group_cols
        
        # Execute
        is_valid, missing_cols, type_errors = validate_agency_group_schema(self.invalid_group_df)
        
        # Verify
        self.assertFalse(is_valid)
        self.assertGreater(len(missing_cols), 0)
        self.assertIn('Agency_Group', missing_cols)
    
    def test_type_validation(self):
        """Test data type validation."""
        # Setup - dataframe with type issues
        df_with_type_issues = pd.DataFrame({
            'RPM_Payout_Agency_ID': ['A001', 'A002'],
            'Agency_Name': ['Agency 1', 'Agency 2'],
            'Customer_Name': ['Customer 1', 'Customer 2'],
            'Commission_Amt': ['invalid', 'not_a_number'],  # Should be numeric
            'Commission_Type': ['Type A', 'Type B'],
            'Month_End_Date': ['not-a-date', '2025-05-31']  # First value is not a valid date
        })
        
        # Mock the expected types
        expected_types = {
            'RPM_Payout_Agency_ID': str,
            'Agency_Name': str,
            'Customer_Name': str,
            'Commission_Amt': [float, int, np.float64, np.int64],  # Accept multiple numeric types
            'Commission_Type': str,
            'Month_End_Date': pd.Timestamp
        }
        
        # Execute with patched function to use our expected types
        with patch('sharepoint_upload.sp_schema_validator.get_expected_types', return_value=expected_types):
            is_valid, missing_cols, type_errors = validate_commission_schema(df_with_type_issues)
        
        # Verify
        self.assertFalse(is_valid)
        self.assertEqual(len(missing_cols), 0)  # All columns present
        self.assertEqual(len(type_errors), 2)  # Two type errors
        self.assertIn('Commission_Amt', [err[0] for err in type_errors])
        self.assertIn('Month_End_Date', [err[0] for err in type_errors])
    
    def test_create_schema_report(self):
        """Test creating a schema validation report."""
        # Setup
        missing_cols = ['Commission_Amt', 'Month_End_Date']
        type_errors = [
            ('Customer_Name', str, int),
            ('Agency_Group', str, list)
        ]
        
        # Execute
        report = create_schema_report('Test Dataset', missing_cols, type_errors)
        
        # Verify
        self.assertIn('Test Dataset', report)
        self.assertIn('Commission_Amt', report)
        self.assertIn('Month_End_Date', report)
        self.assertIn('Customer_Name', report)
        self.assertIn('Agency_Group', report)
        self.assertIn('Expected type', report)
        self.assertIn('Found type', report)


if __name__ == '__main__':
    unittest.main()
