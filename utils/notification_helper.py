"""
Notification Helper Module

This module provides functions for generating ETL metrics reports
and AI-powered insights for commission data analysis.
"""

import os
import json
import logging
from datetime import datetime
import pandas as pd


def save_etl_metrics(metrics, batch_id):
    """
    Save ETL metrics to a JSON file for later reporting.
    
    Args:
        metrics (dict): Dictionary containing ETL metrics
        batch_id (str): Batch ID for the ETL run
        
    Returns:
        str: Path to the saved metrics file, or None if save failed
    """
    try:
        # Create metrics directory if it doesn't exist
        if not os.path.exists('metrics'):
            os.makedirs('metrics')
        
        # Add timestamp and batch_id to metrics
        metrics['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        metrics['batch_id'] = batch_id
        
        # Save to JSON file
        filename = f"metrics/etl_metrics_{batch_id}.json"
        with open(filename, 'w') as f:
            json.dump(metrics, f, indent=2)
        
        logging.info(f"ETL metrics saved to {filename}")
        return filename
    except Exception as e:
        logging.error(f"Failed to save ETL metrics: {e}")
        return None


def read_latest_metrics():
    """
    Read the latest ETL metrics file from the metrics directory.
    
    Returns:
        dict: Latest metrics data, or None if not found
    """
    try:
        if not os.path.exists('metrics'):
            logging.warning("Metrics directory not found")
            return None
        
        # Find all metrics files
        metrics_files = [f for f in os.listdir('metrics') if f.startswith('etl_metrics_') and f.endswith('.json')]
        if not metrics_files:
            logging.warning("No metrics files found")
            return None
        
        # Get the latest file (based on filename timestamp)
        latest_file = sorted(metrics_files)[-1]
        
        # Read and return the metrics
        with open(os.path.join('metrics', latest_file), 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to read metrics: {e}")
        return None


def generate_metrics_table_html(metrics):
    """
    Generate an HTML table from ETL metrics suitable for email.
    
    Args:
        metrics (dict): ETL metrics dictionary
        
    Returns:
        str: HTML table content
    """
    if not metrics:
        return "<p>No metrics available</p>"
    
    # Start building HTML
    html = """
    <style>
        table { border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; }
        th { background-color: #4CAF50; color: white; padding: 12px; text-align: left; }
        td { padding: 8px; border-bottom: 1px solid #ddd; }
        tr:hover { background-color: #f5f5f5; }
        .source-header { background-color: #2196F3; color: white; font-weight: bold; }
        .summary-row { background-color: #FFF3CD; font-weight: bold; }
    </style>
    """
    
    # Create summary section
    html += f"""
    <h3>ETL Execution Summary</h3>
    <p><strong>Batch ID:</strong> {metrics.get('batch_id', 'N/A')}</p>
    <p><strong>Timestamp:</strong> {metrics.get('timestamp', 'N/A')}</p>
    <p><strong>Total Rows Queried:</strong> {metrics['summary']['total_queried']:,}</p>
    <p><strong>Total Rows Inserted:</strong> {metrics['summary']['total_inserted']:,}</p>
    """
    
    # Create detailed table
    html += """
    <h3>Detailed Metrics by Source</h3>
    <table>
        <tr>
            <th>Source</th>
            <th>Query/Table</th>
            <th>Rows Queried</th>
            <th>Rows Inserted</th>
            <th>Success Rate</th>
        </tr>
    """
    
    # Add rows for each source
    for source_name, source_data in metrics['sources'].items():
        if source_data:
            # Add source header row
            html += f"""
            <tr>
                <td colspan="5" class="source-header">{source_name.upper()}</td>
            </tr>
            """
            
            source_total_queried = 0
            source_total_inserted = 0
            
            # Add rows for each query
            for query_name, query_data in source_data.items():
                rows_queried = query_data.get('rows_queried', 0)
                rows_inserted = query_data.get('rows_inserted', 0)
                success_rate = (rows_inserted / rows_queried * 100) if rows_queried > 0 else 0
                
                source_total_queried += rows_queried
                source_total_inserted += rows_inserted
                
                html += f"""
                <tr>
                    <td></td>
                    <td>{query_name}</td>
                    <td>{rows_queried:,}</td>
                    <td>{rows_inserted:,}</td>
                    <td>{success_rate:.1f}%</td>
                </tr>
                """
            
            # Add source subtotal
            source_success_rate = (source_total_inserted / source_total_queried * 100) if source_total_queried > 0 else 0
            html += f"""
            <tr class="summary-row">
                <td></td>
                <td><strong>{source_name.upper()} Total</strong></td>
                <td><strong>{source_total_queried:,}</strong></td>
                <td><strong>{source_total_inserted:,}</strong></td>
                <td><strong>{source_success_rate:.1f}%</strong></td>
            </tr>
            """
    
    html += "</table>"
    
    # Add AI insights if available
    html = add_ai_insights_to_email(html, metrics)
    
    return html


def generate_metrics_table_markdown(metrics):
    """
    Generate a markdown table from ETL metrics for console display.
    
    Args:
        metrics (dict): ETL metrics dictionary
        
    Returns:
        str: Markdown table content
    """
    if not metrics:
        return "No metrics available"
    
    # Build markdown table
    md = f"""
Total Rows Queried: {metrics['summary']['total_queried']:,}
Total Rows Inserted: {metrics['summary']['total_inserted']:,}

| Source | Query/Table | Rows Queried | Rows Inserted | Success Rate |
|--------|-------------|--------------|---------------|--------------|
"""
    
    # Add rows for each source
    for source_name, source_data in metrics['sources'].items():
        if source_data:
            for query_name, query_data in source_data.items():
                rows_queried = query_data.get('rows_queried', 0)
                rows_inserted = query_data.get('rows_inserted', 0)
                success_rate = (rows_inserted / rows_queried * 100) if rows_queried > 0 else 0
                
                md += f"| {source_name} | {query_name} | {rows_queried:,} | {rows_inserted:,} | {success_rate:.1f}% |\n"
    
    return md


def generate_ai_insights(metrics):
    """
    Generate AI insights specifically for commission items data.
    
    Args:
        metrics (dict): ETL metrics dictionary
        
    Returns:
        str: AI-generated insights text
    """
    try:
        import openai
        
        # Check if OpenAI API key is available
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logging.info("OpenAI API key not found, skipping AI insights")
            return ""
        
        openai.api_key = api_key
        
        # Focus on commission items metrics
        commission_metrics = metrics['sources'].get('redshift', {}).get('commission_items', {})
        if not commission_metrics:
            return ""
        
        # Prepare context for AI analysis
        rows_queried = commission_metrics.get('rows_queried', 0)
        rows_inserted = commission_metrics.get('rows_inserted', 0)
        success_rate = (rows_inserted / rows_queried * 100) if rows_queried > 0 else 0
        
        # Get additional context from other tables for comparison
        referral_metrics = metrics['sources'].get('redshift', {}).get('referral_payments', {})
        adjustment_metrics = metrics['sources'].get('redshift', {}).get('adjustment_items', {})
        
        prompt = f"""
        Analyze the following commission ETL metrics and provide 3-4 key business insights:
        
        Commission Items:
        - Rows processed: {rows_queried:,}
        - Success rate: {success_rate:.1f}%
        - This represents commission transactions for the current open period
        
        Related data:
        - Referral payments: {referral_metrics.get('rows_queried', 0):,} rows
        - Adjustments: {adjustment_metrics.get('rows_queried', 0):,} rows
        
        Provide insights about:
        1. Commission volume trends (is this typical, high, or low?)
        2. Data quality observations based on success rates
        3. Referral payment ratio to commissions
        4. Any notable patterns or recommendations
        
        Keep insights concise, business-focused, and actionable. Format as bullet points.
        """
        
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a financial data analyst specializing in commission analysis. Provide concise, actionable insights."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=300,
            temperature=0.7
        )
        
        insights = response.choices[0].message.content
        return insights
        
    except Exception as e:
        logging.warning(f"Failed to generate AI insights: {e}")
        return ""


def add_ai_insights_to_email(metrics_html, metrics):
    """
    Add AI-generated insights to email report with focus on commission data.
    
    Args:
        metrics_html (str): Existing HTML content
        metrics (dict): ETL metrics dictionary
        
    Returns:
        str: Enhanced HTML with AI insights
    """
    if not os.getenv('OPENAI_API_KEY'):
        return metrics_html  # Gracefully skip if no API key
    
    try:
        # Generate insights
        insights = generate_ai_insights(metrics)
        
        if not insights:
            return metrics_html
        
        # Convert insights to HTML with proper formatting
        insights_lines = insights.strip().split('\n')
        formatted_insights = ""
        for line in insights_lines:
            if line.strip():
                if line.strip().startswith(('•', '-', '*')):
                    formatted_insights += f"<li>{line.strip()[1:].strip()}</li>"
                elif line.strip().startswith(('1.', '2.', '3.', '4.')):
                    formatted_insights += f"<li>{line.strip()[2:].strip()}</li>"
                else:
                    formatted_insights += f"<p>{line.strip()}</p>"
        
        # Add to HTML with enhanced styling
        insights_html = f"""
        <h3>📊 AI-Powered Commission Insights</h3>
        <div style="background-color: #f0f8ff; padding: 15px; border-radius: 5px; border-left: 4px solid #2196F3;">
            <ul style="margin: 0; padding-left: 20px;">
                {formatted_insights}
            </ul>
        </div>
        
        <h3>💡 Commission Data Analysis</h3>
        <div style="background-color: #fff3cd; padding: 15px; border-radius: 5px; border-left: 4px solid #ffc107;">
            <p><strong>Commission Items Performance:</strong></p>
            <ul style="margin: 0; padding-left: 20px;">
                <li>Total commission records: {metrics['sources'].get('redshift', {}).get('commission_items', {}).get('rows_queried', 0):,}</li>
                <li>Data integrity: {(metrics['sources'].get('redshift', {}).get('commission_items', {}).get('rows_inserted', 0) / metrics['sources'].get('redshift', {}).get('commission_items', {}).get('rows_queried', 1) * 100):.1f}% success rate</li>
                <li>Referral ratio: {(metrics['sources'].get('redshift', {}).get('referral_payments', {}).get('rows_queried', 0) / max(metrics['sources'].get('redshift', {}).get('commission_items', {}).get('rows_queried', 1), 1) * 100):.1f}% of commissions have referrals</li>
            </ul>
        </div>
        """
        
        return metrics_html + insights_html
        
    except Exception as e:
        logging.warning(f"Failed to add AI insights to email: {e}")
        return metrics_html  # Return original on failure


def analyze_commission_trends(metrics, historical_data=None):
    """
    Analyze commission data trends over time if historical data is available.
    
    Args:
        metrics (dict): Current ETL metrics
        historical_data (list): Optional list of previous metrics for trend analysis
        
    Returns:
        dict: Trend analysis results
    """
    analysis = {
        'current_volume': metrics['sources'].get('redshift', {}).get('commission_items', {}).get('rows_queried', 0),
        'quality_score': 0,
        'referral_ratio': 0,
        'adjustment_ratio': 0
    }
    
    # Calculate quality score
    commission_data = metrics['sources'].get('redshift', {}).get('commission_items', {})
    if commission_data.get('rows_queried', 0) > 0:
        analysis['quality_score'] = (commission_data.get('rows_inserted', 0) / commission_data.get('rows_queried', 1)) * 100
    
    # Calculate referral ratio
    referral_data = metrics['sources'].get('redshift', {}).get('referral_payments', {})
    if commission_data.get('rows_queried', 0) > 0:
        analysis['referral_ratio'] = (referral_data.get('rows_queried', 0) / commission_data.get('rows_queried', 1)) * 100
    
    # Calculate adjustment ratio
    adjustment_data = metrics['sources'].get('redshift', {}).get('adjustment_items', {})
    if commission_data.get('rows_queried', 0) > 0:
        analysis['adjustment_ratio'] = (adjustment_data.get('rows_queried', 0) / commission_data.get('rows_queried', 1)) * 100
    
    # Add historical trend analysis if data available
    if historical_data and len(historical_data) > 0:
        volumes = [h['sources'].get('redshift', {}).get('commission_items', {}).get('rows_queried', 0) for h in historical_data]
        analysis['average_volume'] = sum(volumes) / len(volumes)
        analysis['volume_trend'] = 'increasing' if analysis['current_volume'] > analysis['average_volume'] else 'decreasing'
        analysis['volume_change_pct'] = ((analysis['current_volume'] - analysis['average_volume']) / analysis['average_volume']) * 100
    
    return analysis