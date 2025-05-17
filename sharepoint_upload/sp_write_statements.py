"""
Write Statements Module for SharePoint Upload

This module handles grouping commission data by agency, path construction,
validation, and write-intent logging.
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add parent directory to path for relative imports
sys.path.insert(0, str(Path(__file__).parent.absolute()))
from sp_utils import load_config, validate_path_length, setup_logging, safe_write_file, update_log_entry


def apply_folder_name_mappings(df):
    """
    Apply folder name mapping overrides according to business rules.
    
    Args:
        df (pandas.DataFrame): DataFrame containing agency information
        
    Returns:
        pandas.DataFrame: Updated DataFrame with folder mappings applied
    """
    # Make a copy to avoid modifying the original
    df_mapped = df.copy()
    
    # Apply Jett Enterprises mapping
    jett_mask = df_mapped['Agency_Group'] == "Jett Enterprises + Subs"
    if jett_mask.any():
        df_mapped.loc[jett_mask, 'Agency_Commission_Folder'] = "Jett Enterprises, Inc. (ACH)"
        logging.info(f"Applied Jett Enterprises mapping to {jett_mask.sum()} records")
    
    # Apply Complete Communications mapping
    cc_mask = df_mapped['Agency_Group'].str.contains("Complete Communications", na=False)
    if cc_mask.any():
        df_mapped.loc[cc_mask, 'Agency_Commission_Folder'] = "Complete Communications"
        logging.info(f"Applied Complete Communications mapping to {cc_mask.sum()} records")
    
    return df_mapped


def build_file_paths(commission_data, agency_folders, agency_groups, run_ym, output_base_path):
    """
    Build file paths for each agency and validate them.
    
    Args:
        commission_data (pandas.DataFrame): DataFrame containing commission data
        agency_folders (pandas.DataFrame): DataFrame containing agency folder information
        agency_groups (pandas.DataFrame): DataFrame containing agency group information from Redshift
        run_ym (str): Year and month for the current run (format: YYYYMM)
        output_base_path (str): Base path for SharePoint-synced folder
        
    Returns:
        pandas.DataFrame: Merged DataFrame with file paths
    """
    try:
        # If agency_groups DataFrame is not empty, merge with commission_data first
        if not agency_groups.empty:
            logging.info("Merging commission data with agency groups from Redshift")
            # Merge commission data with agency groups to get Agency_Group column
            enriched_commission_data = pd.merge(
                commission_data,
                agency_groups[['RPM_Agency_ID', 'Agency_Group']],
                on='RPM_Agency_ID',
                how='left'
            )
            logging.info(f"Merged {len(enriched_commission_data)} commission records with agency groups")
        else:
            # If no agency groups available, just use the original commission data
            enriched_commission_data = commission_data.copy()
            logging.warning("No agency groups available from Redshift, folder mapping might be incomplete")
        
        # Merge enriched commission data with agency folders
        merged_df = pd.merge(
            enriched_commission_data,
            agency_folders[['RPM_Agency_ID', 'Agency_Commission_Folder']],
            on='RPM_Agency_ID',
            how='left'
        )
        
        logging.info(f"Merged {len(merged_df)} commission records with agency folders")
        
        # Apply folder name mappings only if Agency_Group column exists
        if 'Agency_Group' in merged_df.columns:
            merged_df = apply_folder_name_mappings(merged_df)
        else:
            logging.warning("Agency_Group column not available, skipping folder name mappings")
        
        # Extract year from run_ym
        year = run_ym[:4]
        
        # Build file paths
        merged_df['FullPath'] = merged_df.apply(
            lambda row: os.path.join(
                output_base_path,
                row['Agency_Commission_Folder'] if pd.notnull(row['Agency_Commission_Folder']) else '',
                year,
                f"{run_ym}_{row['RPM_Payout_Agency_ID']}.xlsx"
            ) if pd.notnull(row['Agency_Commission_Folder']) else None,
            axis=1
        )
        
        # Calculate path length
        merged_df['PathLength'] = merged_df['FullPath'].apply(
            lambda path: len(path) if pd.notnull(path) else 0
        )
        
        return merged_df
    
    except Exception as e:
        logging.error(f"Failed to build file paths: {e}")
        raise


def validate_and_log_paths(merged_df, log_file):
    """
    Validate file paths and log them.
    
    Args:
        merged_df (pandas.DataFrame): Merged DataFrame with file paths
        log_file (str): Path to the log file
        
    Returns:
        pandas.DataFrame: Validated DataFrame with only valid entries
    """
    # Filter out entries with missing Agency_Commission_Folder
    missing_folder_mask = pd.isnull(merged_df['Agency_Commission_Folder'])
    if missing_folder_mask.any():
        missing_agencies = merged_df[missing_folder_mask]['RPM_Payout_Agency_ID'].unique()
        logging.warning(f"Skipping {len(missing_agencies)} agencies with missing Agency_Commission_Folder: {', '.join(missing_agencies)}")
        
        # Log skipped entries
        for _, row in merged_df[missing_folder_mask].iterrows():
            log_entry = pd.DataFrame({
                'Agency': [row.get('Agency_Name', 'Unknown')],
                'RPM_Payout_Agency_ID': [row['RPM_Payout_Agency_ID']],
                'FullPath': [None],
                'RowCount': [0],
                'Checked_At': [datetime.now()],
                'Write_Intent': [False],
                'Status': ['Skipped']
            })
            log_entry.to_csv(log_file, mode='a', header=False, index=False)
    
    # Filter out entries with path length > 255
    long_path_mask = merged_df['PathLength'] > 255
    if long_path_mask.any():
        long_path_agencies = merged_df[long_path_mask]['RPM_Payout_Agency_ID'].unique()
        logging.warning(f"Skipping {len(long_path_agencies)} agencies with path length > 255: {', '.join(long_path_agencies)}")
        
        # Log skipped entries
        for _, row in merged_df[long_path_mask].iterrows():
            log_entry = pd.DataFrame({
                'Agency': [row.get('Agency_Name', 'Unknown')],
                'RPM_Payout_Agency_ID': [row['RPM_Payout_Agency_ID']],
                'FullPath': [row['FullPath']],
                'RowCount': [0],
                'Checked_At': [datetime.now()],
                'Write_Intent': [False],
                'Status': ['Skipped']
            })
            log_entry.to_csv(log_file, mode='a', header=False, index=False)
    
    # Keep only valid entries
    valid_df = merged_df[~(missing_folder_mask | long_path_mask)]
    
    logging.info(f"Validated {len(valid_df)} commission records with valid paths")
    return valid_df


def group_and_prepare_files(valid_df, run_ym, log_file):
    """
    Group commission data by agency and prepare files for writing.
    
    Args:
        valid_df (pandas.DataFrame): Validated DataFrame with only valid entries
        run_ym (str): Year and month for the current run (format: YYYYMM)
        log_file (str): Path to the log file
        
    Returns:
        list: List of dictionaries containing grouped data and file info
    """
    # Group by agency ID
    grouped_data = []
    
    # Get unique agencies
    unique_agencies = valid_df[['RPM_Payout_Agency_ID', 'Agency_Name', 'FullPath']].drop_duplicates()
    
    for _, agency_info in unique_agencies.iterrows():
        agency_id = agency_info['RPM_Payout_Agency_ID']
        agency_name = agency_info['Agency_Name']
        file_path = agency_info['FullPath']
        
        # Filter data for this agency
        agency_data = valid_df[valid_df['RPM_Payout_Agency_ID'] == agency_id]
        row_count = len(agency_data)
        
        # Log the intent to write
        log_entry = pd.DataFrame({
            'Agency': [agency_name],
            'RPM_Payout_Agency_ID': [agency_id],
            'FullPath': [file_path],
            'RowCount': [row_count],
            'Checked_At': [datetime.now()],
            'Write_Intent': [True],
            'Status': ['Pending']
        })
        log_entry.to_csv(log_file, mode='a', header=False, index=False)
        
        # Store grouped data
        grouped_data.append({
            'agency_id': agency_id,
            'agency_name': agency_name,
            'file_path': file_path,
            'data': agency_data,
            'row_count': row_count
        })
    
    logging.info(f"Grouped data for {len(grouped_data)} agencies")
    return grouped_data


def write_excel_files(grouped_data, log_file, user_confirmed=False, chunk_size=10000):
    """
    Write grouped commission data to Excel files with memory-efficient chunking.
    
    Args:
        grouped_data (list): List of dictionaries containing grouped data and file info
        log_file (str): Path to the log file
        user_confirmed (bool): Whether the user has confirmed the write operation
        chunk_size (int): Maximum number of rows to process at once (to manage memory)
        
    Returns:
        tuple: (success_count, failure_count)
    """
    if not user_confirmed:
        logging.warning("User confirmation required to write files")
        return 0, 0
    
    success_count = 0
    failure_count = 0
    
    for group in grouped_data:
        agency_id = group['agency_id']
        file_path = group['file_path']
        data = group['data']
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        try:
            # Get row count for logging memory usage
            row_count = len(data)
            logging.info(f"Processing {row_count} rows for agency {agency_id}")
            
            # For larger datasets, use chunks to reduce memory usage
            if row_count > chunk_size:
                logging.info(f"Large dataset detected ({row_count} rows), using memory-efficient chunking")
                
                # Use ExcelWriter with 'openpyxl' engine for more memory-efficient write
                with pd.ExcelWriter(file_path, engine='openpyxl', mode='w') as writer:
                    # Process in chunks
                    total_chunks = (row_count // chunk_size) + (1 if row_count % chunk_size > 0 else 0)
                    
                    for chunk_num, start_idx in enumerate(range(0, row_count, chunk_size)):
                        # Calculate end index for this chunk
                        end_idx = min(start_idx + chunk_size, row_count)
                        
                        # Log chunk progress
                        chunk_msg = f"Writing chunk {chunk_num+1}/{total_chunks} (rows {start_idx+1}-{end_idx})" 
                        logging.info(chunk_msg)
                        
                        # Get chunk of data
                        chunk = data.iloc[start_idx:end_idx]
                        
                        # Write chunk to Excel (first chunk creates sheet, others append)
                        if chunk_num == 0:
                            chunk.to_excel(writer, sheet_name='Commission Data', index=False)
                        else:
                            # For subsequent chunks, we append rows to existing sheet
                            sheet = writer.sheets['Commission Data']
                            for r_idx, row in enumerate(chunk.values):
                                for c_idx, value in enumerate(row):
                                    sheet.cell(row=r_idx + start_idx + 2, column=c_idx + 1, value=value)
                        
                        # Force garbage collection between chunks to free memory
                        import gc
                        del chunk
                        gc.collect()
            else:
                # For smaller datasets, use standard write method
                data.to_excel(file_path, sheet_name='Commission Data', index=False)
            
            # Update log status
            update_log_entry(log_file, file_path, 'Succeeded')
            
            success_count += 1
            logging.info(f"Successfully wrote file for agency {agency_id}: {file_path}")
        
        except Exception as e:
            # Update log status
            update_log_entry(log_file, file_path, 'Failed')
            
            failure_count += 1
            logging.error(f"Failed to write file for agency {agency_id}: {e}")
    
    logging.info(f"Write operation completed. Success: {success_count}, Failure: {failure_count}")
    return success_count, failure_count
