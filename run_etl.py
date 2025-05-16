#!/usr/bin/env python
"""
Bridgepointe Commission Preview ETL - Backward Compatibility Entry Point

This script maintains backward compatibility while using the new modular architecture.
It serves as the entry point that redirects to the modularized implementation.

The original monolithic implementation has been refactored into:
1. extractors/ - Data extraction modules for Redshift and Salesforce
2. loaders/ - Data loading modules for SQL Server
3. utils/ - Shared utilities for configuration, logging, etc.
4. etl_main.py - Main orchestration module

Usage:
  python run_etl.py [--manual]
"""

import sys
import logging

# Import the main orchestration function from our modular implementation
from etl_main import main

# Simply pass control to the modularized implementation
if __name__ == "__main__":
    # Display a brief migration notice
    print("Using modularized ETL implementation")
    print("See docs/README_MODULAR_REFACTOR.md for details about the new architecture")
    
    # Call the main function from the etl_main module
    main()
