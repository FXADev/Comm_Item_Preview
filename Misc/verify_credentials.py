#!/usr/bin/env python
"""
Credential Verification Script
Checks if all required credentials are properly loaded from .env
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
print("Loading .env file...")
load_dotenv()

def check_variable(var_name, is_secret=True, required=True):
    """Check if a variable is set and print status"""
    value = os.getenv(var_name)
    if value:
        if is_secret:
            display_value = f"{'*' * min(8, len(value))} [Set - {len(value)} chars]"
        else:
            display_value = value
        print(f"✅ {var_name}: {display_value}")
        return True
    else:
        status = "MISSING - REQUIRED" if required else "MISSING - OPTIONAL"
        print(f"❌ {var_name}: {status}")
        return False

def main():
    """Check all required credentials"""
    print("\n=== CREDENTIAL VERIFICATION ===\n")
    
    print("--- Redshift Credentials ---")
    redshift_ok = all([
        check_variable("REDSHIFT_HOST", is_secret=False),
        check_variable("REDSHIFT_USER", is_secret=False),
        check_variable("REDSHIFT_PASSWORD"),
        check_variable("REDSHIFT_DATABASE", is_secret=False),
        check_variable("REDSHIFT_PORT", is_secret=False, required=False)
    ])
    
    print("\n--- Salesforce Credentials ---")
    salesforce_ok = all([
        check_variable("SF_USERNAME", is_secret=False),
        check_variable("SF_PASSWORD"),
        check_variable("SF_SECURITY_TOKEN"),
        check_variable("SF_DOMAIN", is_secret=False, required=False)
    ])
    
    print("\n--- Azure SQL Server Credentials ---")
    sql_server_ok = all([
        check_variable("SQL_SERVER", is_secret=False),
        check_variable("SQL_DATABASE", is_secret=False),
        check_variable("SQL_USERNAME", is_secret=False),
        check_variable("SQL_PASSWORD")
    ])
    
    print("\n=== SUMMARY ===")
    print(f"Redshift: {'READY' if redshift_ok else 'CONFIGURATION INCOMPLETE'}")
    print(f"Salesforce: {'READY' if salesforce_ok else 'CONFIGURATION INCOMPLETE'}")
    print(f"SQL Server: {'READY' if sql_server_ok else 'CONFIGURATION INCOMPLETE'}")
    
    print("\nSuggested .env format:")
    print("""
# Redshift connection
REDSHIFT_HOST=your-redshift-cluster.region.redshift.amazonaws.com
REDSHIFT_USER=your_redshift_username
REDSHIFT_PASSWORD=your_redshift_password
REDSHIFT_DATABASE=your_database_name
REDSHIFT_PORT=5439

# Salesforce connection
SF_USERNAME=your_salesforce_email@example.com
SF_PASSWORD=your_salesforce_password
SF_SECURITY_TOKEN=your_salesforce_security_token
SF_DOMAIN=login

# Azure SQL Server connection
SQL_SERVER=your-server.database.windows.net
SQL_DATABASE=your_database_name
SQL_USERNAME=your_sql_username
SQL_PASSWORD=your_sql_password
    """)
    
    print("Make sure to check your actual .env file against this template.")
    print("Note: For security, password values are masked in this output.")

if __name__ == "__main__":
    main()
