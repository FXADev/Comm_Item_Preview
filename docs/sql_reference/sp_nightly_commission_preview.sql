/* 
 * Bridgepointe Commission Preview - Nightly Refresh Procedure
 * Refreshes all staging tables with the latest data from Redshift and Salesforce
 * Scheduled to run at 5:00 AM EST daily
 */
CREATE OR ALTER PROC dbo.sp_nightly_commission_preview AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @batch_id VARCHAR(20) = CONVERT(VARCHAR(20), GETDATE(), 112) + RIGHT('000000' + CONVERT(VARCHAR(6), DATEPART(HOUR, GETDATE()) * 10000 + DATEPART(MINUTE, GETDATE()) * 100 + DATEPART(SECOND, GETDATE())), 6);
    DECLARE @extract_time DATETIME = GETDATE();
    
    -- Log start of process
    PRINT 'Starting nightly commission preview refresh at ' + CONVERT(VARCHAR(30), @extract_time, 120);
    PRINT 'Batch ID: ' + @batch_id;
    
    -- Clear Redshift staging tables
    TRUNCATE TABLE dbo.stg_commission_items;
    TRUNCATE TABLE dbo.stg_referral_payments;
    TRUNCATE TABLE dbo.stg_adjustment_items;
    
    -- Clear Salesforce staging tables
    TRUNCATE TABLE dbo.stg_sf_agency;
    TRUNCATE TABLE dbo.stg_sf_agency_company_identifier;
    TRUNCATE TABLE dbo.stg_sf_commission_account;
    TRUNCATE TABLE dbo.stg_sf_company;
    TRUNCATE TABLE dbo.stg_sf_strategist;
    TRUNCATE TABLE dbo.stg_sf_supplier;
    
    -- ETL process will then load the tables with fresh data
    -- The ETL script reads from config.yml and executes the SQL/SOQL files
    -- This stored procedure doesn't need to handle the actual extraction
    -- It just prepares the staging environment and can do post-processing
    
    -- Log completion
    PRINT 'Tables truncated and ready for refresh at ' + CONVERT(VARCHAR(30), GETDATE(), 120);
    PRINT 'ETL process will now load fresh data with batch ID: ' + @batch_id;
    
    -- Additional post-processing logic can be added here as needed
    -- Such as data validation, metrics collection, or notification triggers
END
GO
