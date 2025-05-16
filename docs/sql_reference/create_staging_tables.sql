/* Staging tables for Bridgepointe Commission Preview ETL */

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
    commission_item_net_billed decimal(18,2),
    commission_item_gross_commission decimal(18,2),
    commission_item_sales_commission decimal(18,2),
    commission_item_referral_override decimal(18,2),
    commission_item_referral_deduction decimal(18,2),
    commission_item_gross_profit decimal(18,2),
    commission_item_is_adjusted_rate bit,
    commission_item_split_rate decimal(8,4),
    commission_item_sales_commission_rate decimal(8,4),
    commission_item_raw_sales_commission decimal(18,2),
    commission_item_date_added datetime,
    commission_item_note_for_agents varchar(max),
    commission_item_note_for_staff varchar(max),
    commission_item_variable_2471_bill_month varchar(100),
    commission_item_variable_4641_master_supplier varchar(100),
    
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
    commission_item_net_billed decimal(18,2),
    commission_item_gross_commission decimal(18,2),
    commission_item_sales_commission decimal(18,2),
    commission_item_referral_override decimal(18,2),
    commission_item_referral_deduction decimal(18,2),
    commission_item_gross_profit decimal(18,2),
    commission_item_is_adjusted_rate bit,
    commission_item_split_rate decimal(8,4),
    commission_item_sales_commission_rate decimal(8,4),
    commission_item_raw_sales_commission decimal(18,2),
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
    referral_payment_amount decimal(18,2),
    
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
    commission_adjustment_net_billed decimal(18,2),
    commission_adjustment_gross_commission decimal(18,2),
    commission_adjustment_sales_commission decimal(18,2),
    commission_adjustment_note_for_staff varchar(max),
    commission_adjustment_note varchar(max),
    commission_adjustment_last_modified datetime,
    
    commission_adjustment_type_key varchar(50),
    commission_adjustment_type_name varchar(100),
    
    etl_batch_id varchar(20),
    extracted_at datetime
);

-- Salesforce staging tables

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
GO
