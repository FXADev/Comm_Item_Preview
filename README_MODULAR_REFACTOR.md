# ETL Modularization Project

## Overview

This project refactors the Commission Preview ETL process from a monolithic script into a modular architecture. The refactoring improves maintainability, testability, and extensibility without changing the functional behavior of the ETL pipeline.

## New Architecture

The refactored codebase follows a clean, modular structure:

```
Commission_Item_Preview/
├── extractors/               # Data extraction modules
│   ├── __init__.py
│   ├── redshift_extractor.py # Redshift extraction logic
│   └── salesforce_extractor.py # Salesforce extraction logic
├── loaders/                  # Data loading modules  
│   ├── __init__.py
│   └── sql_server_loader.py  # SQL Server loading logic
├── utils/                    # Shared utilities
│   ├── __init__.py
│   ├── config_loader.py      # Configuration management
│   ├── common.py             # Shared functions
│   └── logger.py             # Logging setup
├── etl_main.py               # Main orchestration module
└── run_etl.py                # Original entry point (wrapper for backward compatibility)
```

## Key Improvements

1. **Single Responsibility**: Each module has a clear, focused purpose
2. **Improved Testability**: Components can be tested independently
3. **Better Error Handling**: Errors are isolated to specific components
4. **Maintainability**: Easier to understand, modify and extend specific parts
5. **Consistent Interface**: Same CLI usage as before

## Running the ETL

No changes to usage - the same command will work as before:

```
python run_etl.py [--manual]
```

## Original vs New Structure Mapping

| Original Function | New Module Location |
|-------------------|---------------------|
| `verify_credentials()` | `utils/config_loader.py` |
| `load_config()` | `utils/config_loader.py` |
| `load_query_from_file()` | `utils/config_loader.py` |
| `get_sql_connection()` | `utils/common.py` |
| `prepare_staging_tables()` | `utils/common.py` |
| `insert_to_sql_table()` | `loaders/sql_server_loader.py` |
| `execute_redshift_queries()` | `extractors/redshift_extractor.py` |
| `execute_salesforce_queries()` | `extractors/salesforce_extractor.py` |
| `main()` | `etl_main.py` |

## Extension Points

The modular architecture makes it easy to:

1. Add new data sources by creating additional extractor modules
2. Support new target databases by adding new loader modules
3. Add pre/post processing steps without affecting other components
4. Implement unit tests for each module independently
