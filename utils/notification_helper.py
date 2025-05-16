"""
Notification Helper Module

This module provides utilities for generating notification content,
including metrics tables and summaries for email notifications.
"""

import os
import logging
import json
from datetime import datetime
import pandas as pd


def save_etl_metrics(metrics, batch_id):
    """
    Save ETL metrics to a JSON file for use in notifications.
    
    Args:
        metrics (dict): Dictionary containing source and destination row counts
        batch_id (str): Batch ID for the ETL run
    
    Returns:
        str: Path to the metrics file
    """
    try:
        # Create metrics directory if it doesn't exist
        metrics_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'metrics')
        os.makedirs(metrics_dir, exist_ok=True)
        
        # Add timestamp to metrics
        metrics['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        metrics['batch_id'] = batch_id
        
        # Save metrics to file
        metrics_file = os.path.join(metrics_dir, f'etl_metrics_{batch_id}.json')
        with open(metrics_file, 'w') as f:
            json.dump(metrics, f, indent=2)
        
        logging.info(f"ETL metrics saved to {metrics_file}")
        return metrics_file
    
    except Exception as e:
        logging.error(f"Failed to save ETL metrics: {e}")
        return None


def generate_metrics_table_html(metrics):
    """
    Generate an HTML table from ETL metrics.
    
    Args:
        metrics (dict): Dictionary containing source and destination row counts
    
    Returns:
        str: HTML table representation of the metrics
    """
    if not metrics or 'sources' not in metrics:
        return "<p>No metrics available for this ETL run.</p>"
    
    # Create DataFrame for better table formatting
    rows = []
    for source_name, source_data in metrics['sources'].items():
        for query_name, query_data in source_data.items():
            rows.append({
                'Source': source_name,
                'Object/Query': query_name,
                'Rows Queried': query_data.get('rows_queried', 0),
                'Rows Inserted': query_data.get('rows_inserted', 0)
            })
    
    # If no rows, return message
    if not rows:
        return "<p>No data processed in this ETL run.</p>"
    
    # Create DataFrame and generate HTML
    df = pd.DataFrame(rows)
    
    # Add totals row
    totals = {
        'Source': 'TOTAL',
        'Object/Query': '',
        'Rows Queried': df['Rows Queried'].sum(),
        'Rows Inserted': df['Rows Inserted'].sum()
    }
    df = pd.concat([df, pd.DataFrame([totals])], ignore_index=True)
    
    # Convert DataFrame to HTML with styling
    html_table = df.to_html(index=False, border=1)
    
    # Apply some basic styling to the table
    styled_table = f"""
    <style>
        table {{
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 20px;
        }}
        th, td {{
            padding: 8px;
            text-align: left;
            border: 1px solid #ddd;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        tr:last-child {{
            font-weight: bold;
            background-color: #e6e6e6;
        }}
    </style>
    
    <h3>ETL Process Metrics</h3>
    {html_table}
    """
    
    return styled_table


def generate_metrics_table_markdown(metrics):
    """
    Generate a markdown table from ETL metrics.
    
    Args:
        metrics (dict): Dictionary containing source and destination row counts
    
    Returns:
        str: Markdown table representation of the metrics
    """
    if not metrics or 'sources' not in metrics:
        return "No metrics available for this ETL run."
    
    # Create table header
    markdown = "## ETL Process Metrics\n\n"
    markdown += "| Source | Object/Query | Rows Queried | Rows Inserted |\n"
    markdown += "|--------|-------------|--------------|---------------|\n"
    
    # Add rows
    total_queried = 0
    total_inserted = 0
    
    for source_name, source_data in metrics['sources'].items():
        for query_name, query_data in source_data.items():
            rows_queried = query_data.get('rows_queried', 0)
            rows_inserted = query_data.get('rows_inserted', 0)
            
            total_queried += rows_queried
            total_inserted += rows_inserted
            
            markdown += f"| {source_name} | {query_name} | {rows_queried:,} | {rows_inserted:,} |\n"
    
    # Add totals row
    markdown += f"| **TOTAL** | | **{total_queried:,}** | **{total_inserted:,}** |\n"
    
    return markdown


def read_latest_metrics():
    """
    Read the latest ETL metrics file.
    
    Returns:
        dict: Metrics dictionary or None if not found
    """
    try:
        metrics_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'metrics')
        if not os.path.exists(metrics_dir):
            return None
        
        # Find the latest metrics file
        metric_files = [f for f in os.listdir(metrics_dir) if f.startswith('etl_metrics_')]
        if not metric_files:
            return None
        
        # Sort by modification time (most recent first)
        latest_file = sorted(
            metric_files,
            key=lambda x: os.path.getmtime(os.path.join(metrics_dir, x)),
            reverse=True
        )[0]
        
        # Read the metrics file
        with open(os.path.join(metrics_dir, latest_file), 'r') as f:
            metrics = json.load(f)
        
        return metrics
    
    except Exception as e:
        logging.error(f"Failed to read ETL metrics: {e}")
        return None
