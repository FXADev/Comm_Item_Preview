#!/usr/bin/env python
"""
ETL Metrics Email Generator with AI-Powered Commission Insights

This script reads the latest ETL metrics and generates an HTML report
with AI-powered insights specifically focused on commission data analysis.

Usage:
  python generate_email_metrics.py [--output FILENAME] [--with-ai-insights]

Options:
  --output FILENAME       Output file for the HTML content [default: email_metrics.html]
  --with-ai-insights     Include AI-powered insights in the report (requires OPENAI_API_KEY)
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

from utils.notification_helper import (
    read_latest_metrics, 
    generate_metrics_table_html,
    analyze_commission_trends
)


def generate_executive_summary(metrics, analysis):
    """
    Generate an executive summary section for the email.
    
    Args:
        metrics (dict): ETL metrics
        analysis (dict): Commission trend analysis
        
    Returns:
        str: HTML for executive summary
    """
    commission_volume = analysis.get('current_volume', 0)
    quality_score = analysis.get('quality_score', 0)
    referral_ratio = analysis.get('referral_ratio', 0)
    
    # Determine health status
    if quality_score >= 99:
        health_status = "🟢 Excellent"
        health_color = "#28a745"
    elif quality_score >= 95:
        health_status = "🟡 Good"
        health_color = "#ffc107"
    else:
        health_status = "🔴 Needs Attention"
        health_color = "#dc3545"
    
    summary_html = f"""
    <div style="background-color: #f8f9fa; padding: 20px; border-radius: 10px; margin-bottom: 20px;">
        <h2 style="margin-top: 0;">📈 Commission ETL Executive Summary</h2>
        
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px;">
            <div style="background-color: white; padding: 15px; border-radius: 5px; text-align: center;">
                <h3 style="margin: 0; color: #2196F3;">Commission Volume</h3>
                <p style="font-size: 24px; font-weight: bold; margin: 10px 0;">{commission_volume:,}</p>
                <p style="margin: 0; color: #666;">Total Records</p>
            </div>
            
            <div style="background-color: white; padding: 15px; border-radius: 5px; text-align: center;">
                <h3 style="margin: 0; color: {health_color};">Data Quality</h3>
                <p style="font-size: 24px; font-weight: bold; margin: 10px 0;">{quality_score:.1f}%</p>
                <p style="margin: 0; color: #666;">{health_status}</p>
            </div>
            
            <div style="background-color: white; padding: 15px; border-radius: 5px; text-align: center;">
                <h3 style="margin: 0; color: #ff9800;">Referral Rate</h3>
                <p style="font-size: 24px; font-weight: bold; margin: 10px 0;">{referral_ratio:.1f}%</p>
                <p style="margin: 0; color: #666;">Of Commissions</p>
            </div>
        </div>
    </div>
    """
    
    return summary_html


def main():
    """Main function to generate enhanced email metrics HTML"""
    parser = argparse.ArgumentParser(description='Generate ETL metrics for email notifications')
    parser.add_argument('--output', default='email_metrics.html', help='Output file for HTML content')
    parser.add_argument('--with-ai-insights', action='store_true', help='Include AI-powered insights')
    args = parser.parse_args()
    
    # Read the latest metrics
    metrics = read_latest_metrics()
    if not metrics:
        print("ERROR: No metrics files found")
        with open(args.output, 'w') as f:
            f.write("<p>No ETL metrics available</p>")
        return 1
    
    # Analyze commission trends
    analysis = analyze_commission_trends(metrics)
    
    # Start building complete HTML document
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Commission ETL Report</title>
    </head>
    <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
    """
    
    # Add executive summary
    html_content += generate_executive_summary(metrics, analysis)
    
    # Add detailed metrics table (this includes AI insights if API key is available)
    html_content += generate_metrics_table_html(metrics)
    
    # Add commission-specific analysis section
    html_content += """
    <h3>📊 Commission Data Breakdown</h3>
    <div style="background-color: #e8f5e9; padding: 15px; border-radius: 5px; margin-top: 20px;">
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="padding: 10px; border-right: 1px solid #ccc;">
                    <strong>Commission Items:</strong><br>
                    Primary commission records representing sales transactions
                </td>
                <td style="padding: 10px; border-right: 1px solid #ccc;">
                    <strong>Referral Payments:</strong><br>
                    Secondary payments to referral partners
                </td>
                <td style="padding: 10px;">
                    <strong>Adjustments:</strong><br>
                    Manual corrections and special cases
                </td>
            </tr>
        </table>
    </div>
    """
    
    # Close HTML document
    html_content += """
    </body>
    </html>
    """
    
    # Save to output file
    with open(args.output, 'w') as f:
        f.write(html_content)
    
    print(f"Enhanced metrics HTML generated successfully: {args.output}")
    
    # Handle GitHub Actions environment variables
    github_env = os.environ.get('GITHUB_ENV')
    if github_env:
        with open(github_env, 'a') as f:
            f.write(f"total_queried={metrics['summary']['total_queried']}\n")
            f.write(f"total_inserted={metrics['summary']['total_inserted']}\n")
            f.write(f"commission_volume={analysis['current_volume']}\n")
            f.write(f"data_quality_score={analysis['quality_score']:.1f}\n")
            f.write(f"referral_ratio={analysis['referral_ratio']:.1f}\n")
            f.write(f"metrics_timestamp={metrics['timestamp']}\n")
            f.write(f"metrics_batch_id={metrics['batch_id']}\n")
        print("Metrics written to GitHub environment file.")
    else:
        # For local running, print summary
        print("\n=== COMMISSION ETL SUMMARY ===")
        print(f"Total rows processed: {metrics['summary']['total_queried']:,}")
        print(f"Commission records: {analysis['current_volume']:,}")
        print(f"Data quality score: {analysis['quality_score']:.1f}%")
        print(f"Referral ratio: {analysis['referral_ratio']:.1f}%")
        print(f"Adjustment ratio: {analysis['adjustment_ratio']:.1f}%")
        print(f"Timestamp: {metrics['timestamp']}")
        print(f"Batch ID: {metrics['batch_id']}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())