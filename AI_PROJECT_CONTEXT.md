# Commission Preview ETL Project Overview

*Last Updated: 2025-05-15*

## Project Purpose

This ETL (Extract, Transform, Load) application automates the extraction of commission data from multiple sources and loads it into an AWS Redshift database to create a unified commission preview system. The goal is to provide stakeholders with accurate, timely commission information through a consolidated view.

## Project Structure

```
Comm_Item_Preview_New/
├── .github/workflows/       # GitHub Actions workflow definitions
│   ├── ci.yml               # Continuous Integration workflow
│   └── nightly_etl.yml      # Automated nightly ETL process
├── config/                  # Configuration files
├── docs/                    # Documentation files
├── extractors/              # Source data extraction modules
├── loaders/                 # Data loading components
├── logs/                    # ETL process logs
├── redshift/                # Redshift database utilities and schema
├── soql/                    # Salesforce SOQL queries
├── tests/                   # Test suite
├── utils/                   # Utility functions
├── worklog/                 # Work log entries tracking changes
├── .env                     # Environment variables (not committed)
├── .env.example             # Example environment file template
├── etl_main.py              # Main ETL orchestration
├── run_etl.py               # ETL process entry point
├── run_etl.bat              # Windows batch script for running ETL
├── requirements.txt         # Python dependencies
└── requirements.lock        # Locked dependencies for reproducibility
```

## Key Components

### Data Sources
- **Salesforce**: Primary source for commission-related data
- **SQL Server**: Additional commission and internal business data
- **[Other Data Sources]**: Any additional data sources integrated into the ETL flow

### ETL Pipeline
- **Extractors**: Modules that pull data from source systems
- **Transformers**: Data transformation and business logic (embedded in main ETL)
- **Loaders**: Components that load processed data into Redshift

### Infrastructure
- **AWS Redshift**: Target data warehouse
- **GitHub Actions**: CI/CD and scheduled ETL execution
- **Email Notifications**: Automated success/failure alerts

## Workflow

1. **Scheduled Execution**: The ETL process runs nightly at 5:00 AM EST (9:00 UTC) via GitHub Actions
2. **Data Extraction**: Data is pulled from multiple sources using appropriate APIs and connectors
3. **Data Transformation**: Raw data is transformed into a unified schema
4. **Data Loading**: Processed data is loaded into Redshift
5. **Notifications**: Email notifications are sent upon completion (success or failure)
6. **Monitoring**: Logs are generated for troubleshooting and auditing

## Configuration

The application uses environment variables stored in a `.env` file locally and GitHub Secrets for the automated workflow. Key configuration includes:

- **Redshift Connection**: Host, port, database, credentials, schema
- **Salesforce Connection**: Username, password, security token
- **SQL Server Connection**: Server, database, credentials, driver
- **Email Notifications**: SMTP configuration and recipient list

## Running the ETL Process

### Locally
```bash
# Windows
run_etl.bat

# Unix/Linux
python run_etl.py
```

### GitHub Actions
- **Scheduled**: Runs automatically at 5:00 AM EST daily
- **Manual**: Can be triggered manually from the GitHub Actions tab

## Maintenance and Updates

When making changes to the project:
1. Update documentation in this file
2. Record significant changes in the worklog
3. Keep environment variable examples updated in `.env.example`
4. Ensure tests are updated to reflect changes in functionality

## Monitoring and Troubleshooting

- Check the `logs/` directory for detailed execution logs
- GitHub Actions provides execution history and logs
- Email notifications contain execution details for quick reference

---

*This document should be maintained and updated as the project evolves to provide comprehensive context for AI assistants and team members.*
