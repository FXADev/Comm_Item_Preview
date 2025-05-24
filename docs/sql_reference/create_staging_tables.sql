/* Staging tables for Bridgepointe Commission Preview ETL */
/* Updated: 2025-05-24 - Changed currency fields to decimal(18,2) from decimal(18,4) */
/* Rate fields remain decimal(8,4) for precision requirements */

-- Redshift staging tables

-- Commission Items
CREATE TABLE dbo.stg_commission_items (
    run_id varchar(50),
    run_ym varchar(7),
    run_month_name varchar(20),
    run_year int,
    run_is_closed bit,
    run_first_day_of_month date,
    run_last_day_of_month date,
    run_date_opened datetime,
    run_date_closed datetime,
    
    primary_rep_name varchar(255),
    primary_rep_id varchar(50),
    primary_rep_agency_id varchar(50),
    primary_rep_agency_name varchar(255),
    primary_rep_agency_group varchar(100),
    
    referral_agency_key varchar(50),
    referral_agency_id varchar(50),
    referral_agency_name varchar(255),
    referral_agency_group varchar(100),
    
    account_id varchar(50),
    account_number varchar(100),
    account_customer_id varchar(50),
    account_supplier_id varchar(50),
    account_supplier_name varchar(255),
    account_customer_name varchar(255),
    
    product_id varchar(50),
    product_name varchar(255),
    product_supplier_name varchar(255),
    product_supplier_id varchar(50),
    
    rep_key varchar(50),
    account_key varchar(50),
    run_key varchar(50),
    account_group_key varchar(50),
    product_key varchar(50),
    commission_item_id varchar(50),
    commission_item_created_from_item_name varchar(255),
    
    -- Currency fields: decimal(18,2) - 16 digits before decimal, 2 after
    commission_item_net_billed decimal(18,2),
    commission_item_gross_commission decimal(18,2),
    commission_item_sales_commission decimal(18,2),
    commission_item_referral_override decimal(18,2),
    commission_item_referral_deduction decimal(18,2),
    commission_item_gross_profit decimal(18,2),
    commission_item_raw_sales_commission decimal(18,2),
    
    -- Rate fields: decimal(8,4) - 4 digits before decimal, 4 after
    commission_item_split_rate decimal(8,4),
    commission_item_sales_commission_rate decimal(8,4),
    
    commission_item_is_adjusted_rate bit,
    commission_item_date_added datetime,
    commission_item_note_for_agents varchar(max),
    commission_item_note_for_staff varchar(max),
    commission_item_variable_2471_bill_month varchar(100),
    commission_item_variable_4641_master_supplier varchar(100),
    
    -- ETL audit columns
    etl_batch_id varchar(20),
    extracted_at datetime
);

-- Referral Payments
CREATE TABLE dbo.stg_referral_payments (
    run_id varchar(50),
    run_ym varchar(7),
    run_month_name varchar(20),
    run_year int,
    run_is_closed bit,
    run_first_day_of_month date,
    run_last_day_of_month date,
    run_date_opened datetime,
    run_date_closed datetime,
    
    primary_rep_name varchar(255),
    primary_rep_id varchar(50),
    primary_rep_agency_id varchar(50),
    primary_rep_agency_name varchar(255),
    primary_rep_agency_group varchar(100),
    
    referral_agency_key varchar(50),
    referral_agency_id varchar(50),
    referral_agency_name varchar(255),
    referral_agency_group varchar(100),
    
    account_id varchar(50),
    account_number varchar(100),
    account_customer_id varchar(50),
    account_supplier_id varchar(50),
    account_customer_name varchar(255),
    account_supplier_name varchar(255),
    
    product_id varchar(50),
    product_name varchar(255),
    product_supplier_name varchar(255),
    product_supplier_id varchar(50),
    
    commission_rep_key varchar(50),
    commission_item_id varchar(50),
    commission_item_created_from_item_name varchar(255),
    
    -- Currency fields: decimal(18,2) - 16 digits before decimal, 2 after
    commission_item_net_billed decimal(18,2),
    commission_item_gross_commission decimal(18,2),
    commission_item_sales_commission decimal(18,2),
    commission_item_referral_override decimal(18,2),
    commission_item_referral_deduction decimal(18,2),
    commission_item_gross_profit decimal(18,2),
    commission_item_raw_sales_commission decimal(18,2),
    referral_payment_amount decimal(18,2),
    
    -- Rate fields: decimal(8,4) - 4 digits before decimal, 4 after
    commission_item_split_rate decimal(8,4),
    commission_item_sales_commission_rate decimal(8,4),
    
    commission_item_is_adjusted_rate bit,
    commission_item_date_added datetime,
    commission_item_note_for_agents varchar(max),
    commission_item_note_for_staff varchar(max),
    commission_item_variable_2471_bill_month varchar(100),
    commission_item_variable_4641_master_supplier varchar(100),
    
    referral_payment_referral_id varchar(50),
    referral_payment_commission_item_id varchar(50),
    run_key varchar(50),
    account_key varchar(50),
    product_key varchar(50),
    rep_key varchar(50),
    pay_to_agency_key varchar(50),
    
    -- ETL audit columns
    etl_batch_id varchar(20),
    extracted_at datetime
);

-- Adjustment Items
CREATE TABLE dbo.stg_adjustment_items (
    run_id varchar(50),
    run_ym varchar(7),
    run_month_name varchar(20),
    run_year int,
    run_is_closed bit,
    run_first_day_of_month date,
    run_last_day_of_month date,
    run_date_opened datetime,
    run_date_closed datetime,
    
    agency_id varchar(50),
    agency_name varchar(255),
    agency_group varchar(100),
    
    supplier_id varchar(50),
    supplier_name varchar(255),
    
    commission_adjustment_id varchar(50),
    run_key varchar(50),
    supplier_key varchar(50),
    agency_key varchar(50),
    
    -- Currency fields: decimal(18,2) - 16 digits before decimal, 2 after
    commission_adjustment_net_billed decimal(18,2),
    commission_adjustment_gross_commission decimal(18,2),
    commission_adjustment_sales_commission decimal(18,2),
    
    commission_adjustment_note_for_staff varchar(max),
    commission_adjustment_note varchar(max),
    commission_adjustment_last_modified datetime,
    
    commission_adjustment_type_key varchar(50),
    commission_adjustment_type_name varchar(100),
    
    -- ETL audit columns
    etl_batch_id varchar(20),
    extracted_at datetime
);

-- Salesforce staging tables (unchanged - no decimal precision issues)

-- Agency
CREATE TABLE dbo.stg_sf_agency (
    Id varchar(18) PRIMARY KEY,
    Name varchar(255),
    Agency_ID__c varchar(50),
    RPM_Agency_Name__c varchar(255),
    etl_batch_id varchar(20),
    extracted_at datetime
);

-- Agency Company Identifier
CREATE TABLE dbo.stg_sf_agency_company_identifier (
    Id varchar(18) PRIMARY KEY,
    Company__c varchar(18),
    Agency__c varchar(18),
    Agency_ID__c varchar(50),
    Company_ID__c varchar(50),
    etl_batch_id varchar(20),
    extracted_at datetime
);

-- Commission Account
CREATE TABLE dbo.stg_sf_commission_account (
    Id varchar(18) PRIMARY KEY,
    Name varchar(255),
    Account_ID__c varchar(50),
    Supplier__c varchar(18),
    Primary_Agency__c varchar(18),
    Primary_Company__c varchar(18),
    Full_Account_Number__c varchar(100),
    etl_batch_id varchar(20),
    extracted_at datetime
);

-- Company
CREATE TABLE dbo.stg_sf_company (
    Id varchar(18) PRIMARY KEY,
    Company_ID__c varchar(50),
    etl_batch_id varchar(20),
    extracted_at datetime
);

-- Strategist (Sales Rep)
CREATE TABLE dbo.stg_sf_strategist (
    Id varchar(18) PRIMARY KEY,
    Sales_Rep_ID__c varchar(50),
    Agency_Id__c varchar(18),
    etl_batch_id varchar(20),
    extracted_at datetime
);

-- Supplier
CREATE TABLE dbo.stg_sf_supplier (
    Id varchar(18) PRIMARY KEY,
    Supplier_ID__c varchar(50),
    etl_batch_id varchar(20),
    extracted_at datetime
);

-- Account (if used)
CREATE TABLE dbo.stg_sf_account (
    Id varchar(18) PRIMARY KEY,
    Name varchar(255),
    ParentId varchar(18),
    Division__c varchar(100),
    SystemModstamp datetime,
    etl_batch_id varchar(20),
    extracted_at datetime
);

-- Table to store ETL execution history for auditing
CREATE TABLE dbo.etl_execution_log (
    execution_id varchar(20) PRIMARY KEY,
    start_time datetime NOT NULL,
    end_time datetime NULL,
    status varchar(20) NOT NULL,
    records_processed int,
    error_message varchar(1000)
);

-- Indexes for performance (optional - add as needed)
/*
-- Primary staging table indexes
CREATE INDEX IX_stg_commission_items_batch_id ON dbo.stg_commission_items(etl_batch_id);
CREATE INDEX IX_stg_referral_payments_batch_id ON dbo.stg_referral_payments(etl_batch_id);
CREATE INDEX IX_stg_adjustment_items_batch_id ON dbo.stg_adjustment_items(etl_batch_id);

-- Salesforce staging table indexes  
CREATE INDEX IX_stg_sf_agency_batch_id ON dbo.stg_sf_agency(etl_batch_id);
CREATE INDEX IX_stg_sf_commission_account_batch_id ON dbo.stg_sf_commission_account(etl_batch_id);
*/

GO

-- Comments and Documentation
/*
DECIMAL PRECISION NOTES:
======================

Currency Fields (decimal(18,2)):
- Maximum value: ±999,999,999,999,999.99
- 16 digits before decimal point
- 2 digits after decimal point
- Standard for financial amounts (dollars and cents)
- Used for: net_billed, gross_commission, sales_commission, etc.

Rate Fields (decimal(8,4)):  
- Maximum value: ±9,999.9999
- 4 digits before decimal point
- 4 digits after decimal point
- Higher precision needed for percentage calculations
- Used for: commission_rate, split_rate

ETL PROCESS NOTES:
==================
- All tables truncated before each ETL run
- etl_batch_id tracks each ETL execution (format: YYYYMMDDHHMMSS)
- extracted_at timestamp shows when data was loaded
- Tables designed for bulk insert performance
- No foreign key constraints to allow fast loading

SCHEMA CHANGE HISTORY:
=====================
2025-05-24: Changed currency fields from decimal(18,4) to decimal(18,2)
           - Eliminates "Numeric value out of range" errors
           - Provides 10x larger capacity for financial amounts
           - Aligns with financial industry standards
           - Rate fields remain decimal(8,4) for precision requirements
*/