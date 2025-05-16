#!/usr/bin/env python
"""
Bridgepointe Commission Preview ETL - Backward Compatibility Wrapper

This script serves as a backward compatibility layer to maintain the existing
CLI interface while using the new modular architecture. It simply imports and
calls the main function from the etl_main module.

Usage:
  python run_etl.py [--manual]
"""

import sys
from etl_main import main

# Maintain the same entry point behavior
if __name__ == "__main__":
    # Simply pass through to the new main function
    main()
