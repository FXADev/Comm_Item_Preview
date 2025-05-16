# Bridgepointe Commission Preview ETL

## Overview
This ETL pipeline extracts commission data from Redshift and Salesforce, loads it into SQL Server staging tables, and prepares it for reporting and analysis. The system provides comprehensive logging, error handling, and execution tracking.

## Data Flow
1. **Redshift → SQL Server**: Extracts commission items, referral payments, and adjustments
2. **Salesforce → SQL Server**: Extracts configured objects (agencies, accounts, companies, etc.)
3. **Staging refresh**: Automatically truncates and reloads all staging tables
4. **Data tracking**: Adds `etl_batch_id` and `extracted_at` to all records for lineage
5. **Metrics collection**: Tracks row counts for ETL process monitoring and reporting
6. **Email notifications**: Sends detailed execution reports with metrics tables
7. **Credential security**: Sources all connection parameters from `.env` file

## Project Structure

### Core Files
- **`run_etl.py`**: Backward compatibility wrapper for the modular ETL implementation
- **`run_etl.bat`**: Windows batch wrapper for easy execution
- **`etl_main.py`**: Main orchestration module for the ETL workflow

### Modular Components
- **`extractors/`**: Data extraction modules
  - **`redshift_extractor.py`**: Handles data extraction from Redshift
  - **`salesforce_extractor.py`**: Handles data extraction from Salesforce
- **`loaders/`**: Data loading modules
  - **`sql_server_loader.py`**: Manages data loading into SQL Server
- **`utils/`**: Utility modules
  - **`config_loader.py`**: Manages configuration loading and validation
  - **`logger.py`**: Sets up logging
  - **`common.py`**: Contains shared functions
  - **`notification_helper.py`**: Generates metrics reports and tables
- **`scripts/`**: Utility scripts
  - **`generate_email_metrics.py`**: Creates HTML metrics tables for email reports

### Configuration
- **`config/`**: Configuration directory
  - **`config.yml`**: Configuration file defining data sources and queries
- **`redshift/`**: SQL queries for Redshift extraction
- **`soql/`**: SOQL queries for Salesforce extraction
- **`.github/workflows/`**: GitHub Actions workflow configurations
  - **`nightly_etl.yml`**: Automated ETL execution and notifications
- **`.env`**: Environment file containing all credentials (not in repo)
- **`.gitignore`**: Prevents sensitive files from being committed
- **`.env.example`**: Template for required environment variables

### Documentation
- **`docs/`**: Documentation directory
  - **`sql_reference/`**: SQL reference files
    - **`views/`**: SQL views for formatting extracted data
  - **`README_MODULAR_REFACTOR.md`**: Detailed documentation about the modularization
  - **`CONTRIBUTING.md`**: Guidelines for contributing to the project

## Execution Options

### Main Execution Script
```bash
python run_etl.py                # Standard execution
python run_etl.py --manual       # Test mode without connections
```

### Windows Batch Execution
```
run_etl.bat                      # Double-click or schedule this file
```

### Legacy Execution (Alternative)
```bash
python etl.py                    # Simpler version with less error handling
```

## Getting Started
1. Clone the repository
2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   venv\Scripts\activate       # Windows
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Create `.env` file with required credentials:
   - Redshift: `REDSHIFT_HOST`, `REDSHIFT_USER`, `REDSHIFT_PASSWORD`, `REDSHIFT_DATABASE`
   - Salesforce: `SF_USERNAME`, `SF_PASSWORD`, `SF_SECURITY_TOKEN`
   - SQL Server: `SQL_SERVER`, `SQL_DATABASE`, `SQL_USERNAME`, `SQL_PASSWORD`

## Scheduling Recommendations
*Windows Task Scheduler:*
- Program: `cmd.exe`
- Arguments: `/c "C:\path\to\run_etl.bat"`
- Working directory: `C:\path\to\project`

*SQL Server Agent:*
1. Step 1: `cmd /c "cd C:\path\to\project && run_etl.bat"`
2. Step 2: `EXEC dbo.sp_commission_preview_transform` (optional SQL transformation)

*GitHub Actions:*
- Automated nightly runs via the `.github/workflows/nightly_etl.yml` configuration
- Sends email notifications with execution metrics to configured recipients

## Configuration

### Adding New Data Sources
*Redshift:* Add new query entry to `redshift.queries` section in `config/config.yml`:
```yaml
- name: new_table_name
  file: redshift/new_query.sql
```

*Salesforce:* Add new object entry to `salesforce.queries` section:
```yaml
- name: new_object
  file: soql/new_object.soql
```

## Troubleshooting
- **Logs**: Check the `logs/` directory for detailed execution logs (older logs in `logs/archive/`)
- **Execution tracing**: Each ETL run generates a unique `etl_batch_id` timestamp for data lineage
- **Credential issues**: Run with `--manual` flag to test without connections
- **Data verification**: Query staging tables with `etl_batch_id` to trace data lineage
- **Metrics reports**: Check email notifications for row counts and processing statistics
- **Row count verification**: Check JSON files in the `metrics/` directory for detailed metrics
- **Modular architecture**: See `docs/README_MODULAR_REFACTOR.md` for detailed implementation information
