#!/usr/bin/env python
"""
ETL Metrics Email Generator

This script reads the latest ETL metrics and generates an HTML table
suitable for inclusion in email notifications. It searches for the latest
metrics file in the metrics directory.

Usage:
  python generate_email_metrics.py [--output FILENAME]

Options:
  --output FILENAME    Optional output file for the HTML content [default: email_metrics.html]
"""

import os
import sys
import json
import argparse
from datetime import datetime
import pandas as pd

# Add parent directory to path so we can import utils
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from utils.notification_helper import read_latest_metrics, generate_metrics_table_html


def main():
    """Main function to generate email metrics HTML"""
    parser = argparse.ArgumentParser(description='Generate ETL metrics for email notifications')
    parser.add_argument('--output', default='email_metrics.html', help='Output file for HTML content')
    args = parser.parse_args()
    
    # Read the latest metrics
    metrics = read_latest_metrics()
    if not metrics:
        print("ERROR: No metrics files found")
        with open(args.output, 'w') as f:
            f.write("<p>No ETL metrics available</p>")
        return 1
    
    # Generate HTML table
    html_content = generate_metrics_table_html(metrics)
    
    # Save to output file
    with open(args.output, 'w') as f:
        f.write(html_content)
    
    print(f"Metrics HTML generated successfully: {args.output}")
    # Only print these in non-GitHub environments to avoid duplication
    if not os.environ.get('GITHUB_ENV'):
        print(f"Total rows queried: {metrics['summary']['total_queried']}")
        print(f"Total rows inserted: {metrics['summary']['total_inserted']}")
    else:
        print("Metrics written to GitHub environment file.")
    
    # Return data suitable for GitHub Actions environment files
    # These environment variables can be used to include key metrics in email subject lines
    # Using new environment files approach instead of deprecated set-output
    github_env = os.environ.get('GITHUB_ENV')
    if github_env:
        with open(github_env, 'a') as f:
            f.write(f"total_queried={metrics['summary']['total_queried']}\n")
            f.write(f"total_inserted={metrics['summary']['total_inserted']}\n")
            f.write(f"metrics_timestamp={metrics['timestamp']}\n")
            f.write(f"metrics_batch_id={metrics['batch_id']}\n")
    else:
        # For local running, just print metrics
        print(f"Total rows queried: {metrics['summary']['total_queried']}")
        print(f"Total rows inserted: {metrics['summary']['total_inserted']}")
        print(f"Timestamp: {metrics['timestamp']}")
        print(f"Batch ID: {metrics['batch_id']}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
