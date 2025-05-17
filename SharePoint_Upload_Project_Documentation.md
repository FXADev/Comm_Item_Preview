# SharePoint Commission Statement Upload Project

## Project Overview
This project implements an automated workflow for extracting commission data from multiple sources and writing Excel statement files to SharePoint-synced folders. It replaces the previous Alteryx-based workflow with a modular Python solution, improving maintainability, memory efficiency, and error handling.

## Architecture

### Core Components

1. **Data Extraction**
   - `sp_extract_sql.py`: Extracts commission data from SQL Server
   - `sp_extract_soql.py`: Extracts agency folder information from Salesforce
   - `sp_extract_redshift.py`: Extracts agency group information from Redshift

2. **Data Processing & File Generation**
   - `sp_write_statements.py`: Handles file path construction, data grouping, and Excel file generation
   - `sp_schema_validator.py`: Validates data schemas to ensure upstream changes don't break the process

3. **System Integration**
   - `sp_utils.py`: Provides shared utilities for logging, config loading, and file operations
   - `sp_main.py`: Coordinates the entire workflow and handles user interaction

4. **Testing & Quality Assurance**
   - `tests/sharepoint_upload/`: Comprehensive test suite following TDD principles
   - `run_tests.py`: Custom test runner with detailed logging

## Implementation Details

### Memory Efficiency Improvements
- Implemented chunking in data extraction from SQL and Redshift
- Added memory-efficient processing for large Excel files
- Explicit garbage collection during high-memory operations

### Data Integrity Protection
- Schema validation to catch upstream data structure changes
- Type checking to ensure data consistency
- Clear error reporting for validation failures

### Error Handling
- Graceful error handling for database connections
- Transaction management for database operations
- File operation error handling with proper logging

### User Workflow
- Confirmation prompt before writing files
- Rollback capability if process is interrupted
- Detailed logging of all operations for auditability

## Current Status

### Completed Features
- Data extraction from all required sources (SQL, Salesforce, Redshift)
- Agency group integration for proper folder mapping
- Memory-efficient file writing with chunking
- Comprehensive test framework for all components
- Schema validation to protect against upstream changes

### Testing Status
- Unit tests implemented for all components
- Schema validation tests passing
- File operation tests passing
- Test runner implemented with detailed logging

## Next Steps

### Immediate Tasks
1. **End-to-End Integration Testing**
   - Test with realistic data volume
   - Validate folder structure creation
   - Verify agency group mapping logic

2. **Performance Optimization**
   - Benchmark and tune chunk sizes for optimal performance
   - Consider parallel processing for file writing operations
   - Optimize memory usage during peak operations

3. **Deployment Preparation**
   - Update GitHub Actions workflow to include the new SharePoint process
   - Set up proper error notification channels
   - Configure the correct schedule (5:00 AM EST / 10:00 UTC)

### Future Improvements
1. **Monitoring & Analytics**
   - Add detailed metrics on process duration and resource usage
   - Implement proactive alerts for schema changes
   - Track historical performance metrics

2. **User Interface Enhancements**
   - Consider a web UI for triggering and monitoring uploads
   - Implement a dashboard for process statistics
   - Add email notifications for successful/failed runs

3. **Advanced Features**
   - Support for incremental uploads
   - Automated validation of generated Excel files
   - Checksum verification for data integrity

## Technical Reference

### Environment Variables
The following environment variables are required:

```
# SQL Server Configuration
SQL_SERVER=server_name
SQL_DATABASE=database_name
SQL_USERNAME=username
SQL_PASSWORD=password
SQL_DRIVER={ODBC Driver}

# Salesforce Configuration
SF_USERNAME=username@example.com
SF_PASSWORD=password
SF_SECURITY_TOKEN=security_token
SF_DOMAIN=domain

# Redshift Configuration
REDSHIFT_HOST=host
REDSHIFT_PORT=port
REDSHIFT_DATABASE=database
REDSHIFT_USER=user
REDSHIFT_PASSWORD=password
REDSHIFT_SCHEMA=schema
```

### Example Usage

```python
# Run the SharePoint upload process for a specific period
python -m sharepoint_upload.sp_main --run-ym 202505
```

### Configuration

The system is configured through `sp_config.yml`, which defines:

- Output paths for SharePoint-synced folders
- SQL query file references
- SOQL query file references
- Redshift query file references
- Runtime parameters

## Conclusion

The SharePoint Upload module provides a robust, memory-efficient solution for generating and uploading commission statements to SharePoint. Its modular design, comprehensive testing, and focus on data integrity ensure reliable operation even with large datasets and changing upstream systems. The system is designed following SOLID principles and emphasizes maintainability and error resilience.
