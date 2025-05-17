"""
Test Runner Script for SharePoint Upload Tests

This script runs all SharePoint Upload tests and logs the results to a file.
It provides a simple way to run tests with detailed logging.
"""

import unittest
import logging
import sys
import os
import datetime
from pathlib import Path

# Create logs directory if it doesn't exist
logs_dir = Path('test_logs')
logs_dir.mkdir(exist_ok=True)

# Configure logging
timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = logs_dir / f"test_run_{timestamp}.log"

# Set up logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

# Custom test result class to log test results
class LoggingTestResult(unittest.TextTestResult):
    def startTest(self, test):
        super().startTest(test)
        logging.info(f"Starting test: {test.id()}")
    
    def addSuccess(self, test):
        super().addSuccess(test)
        logging.info(f"✅ Test passed: {test.id()}")
    
    def addFailure(self, test, err):
        super().addFailure(test, err)
        logging.error(f"❌ Test failed: {test.id()}")
        logging.error(f"Error: {err[1]}")
    
    def addError(self, test, err):
        super().addError(test, err)
        logging.error(f"⚠️ Test error: {test.id()}")
        logging.error(f"Error: {err[1]}")
    
    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        logging.info(f"⏩ Test skipped: {test.id()} - Reason: {reason}")


class LoggingTestRunner(unittest.TextTestRunner):
    def __init__(self, **kwargs):
        kwargs['resultclass'] = LoggingTestResult
        super().__init__(**kwargs)


def run_tests():
    """Run all SharePoint Upload tests with logging."""
    logging.info("=" * 70)
    logging.info(f"Starting test run at {datetime.datetime.now().isoformat()}")
    logging.info("=" * 70)
    
    # Discover and run tests
    start_dir = 'tests/sharepoint_upload'
    loader = unittest.TestLoader()
    
    # Try to discover tests
    try:
        suite = loader.discover(start_dir, pattern="test_*.py")
        test_count = suite.countTestCases()
        logging.info(f"Discovered {test_count} tests in {start_dir}")
        
        # Run the tests with our custom runner
        runner = LoggingTestRunner(verbosity=2)
        result = runner.run(suite)
        
        # Log summary
        logging.info("=" * 70)
        logging.info(f"Test run completed at {datetime.datetime.now().isoformat()}")
        logging.info(f"Ran {result.testsRun} tests")
        logging.info(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
        logging.info(f"Failures: {len(result.failures)}")
        logging.info(f"Errors: {len(result.errors)}")
        logging.info("=" * 70)
        
        # Return True if successful, False otherwise
        return len(result.failures) == 0 and len(result.errors) == 0
        
    except Exception as e:
        logging.error(f"Error discovering or running tests: {e}")
        return False


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
