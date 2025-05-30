SELECT
    --[run_id],
    [run_ym],
    --[run_month_name],
    --[run_year],
    [run_is_closed],
   -- [run_first_day_of_month],
   -- [run_last_day_of_month],
    [run_date_opened],
    [run_date_closed],
    [primary_rep_name] as [Primary_Rep_Name],
    --[primary_rep_id] as [RPM_Primary_Rep_ID],
    --[primary_rep_agency_id] as [RPM_Primary_Rep_Agency_ID],
    [primary_rep_agency_name] as [Primary_Rep_Agency_Name],
    [primary_rep_agency_group] as [Primary_Rep_Agency_Group],
    --[referral_agency_key],
    --[referral_agency_id] as [RPM_Referral_Agency_ID],
    [referral_agency_name] as [Referral_Agency_Name],
    [referral_agency_group] as [Referral_Agency_Group],
    [account_id] as [RPM_Account_ID],
    [account_number] as [RPM_Account_Number],
    --[account_customer_id] as [RPM_Customer_ID],
   -- [account_supplier_id] as [RPM_Supplier_ID],
    [account_supplier_name] as [RPM_Supplier_Name],
    [account_customer_name] as [RPM_Customer_Name],
    --[product_id] as [RPM_Product_ID],
    [product_name] as [RPM_Product_Name],
    --[product_supplier_name],
    --[product_supplier_id],
    --[rep_key],
    --[account_key],
    --[run_key],
    --[account_group_key],
    --[product_key],
    [commission_item_id] as [Item_ID],
    [commission_item_created_from_item_name] as [Original_Item_Name],
    [commission_item_net_billed] as [Net_Billed],
    [commission_item_gross_commission] as [Gross_Commission],
    [commission_item_sales_commission] as [Sales_Commission],
    [commission_item_referral_override] as [Referral_Override],
    [commission_item_referral_deduction] as [Referral_Deduction],
    [commission_item_gross_profit] as [Gross_Profit],
    [commission_item_is_adjusted_rate] as [Is_Adjusted_Rate],
    [commission_item_split_rate] as [Split_Rate],
    [commission_item_sales_commission_rate] as [Sales_Commission_Rate],
    [commission_item_raw_sales_commission] as [Raw_Sales_Commission],
    [commission_item_date_added] as [Item_Date_Added],
    [commission_item_note_for_agents] as [Note_for_Agents],
    [commission_item_note_for_staff] as [Note_for_Staff],
    [commission_item_variable_2471_bill_month] as [Bill_Month],
    [commission_item_variable_4641_master_supplier] as [Master_Supplier],
    [etl_batch_id] as [ETL_Batch_ID],
    [extracted_at] as [Extracted_At],
    CONCAT('BPT3__', CAST(ci.account_customer_id AS varchar(50))) AS RPM_Customer_ID,
    CONCAT('BPT3__', CAST(ci.account_supplier_id AS varchar(50))) AS RPM_Supplier_ID,
    CONCAT('BPT3__', CAST(ci.primary_rep_id AS varchar(50))) AS RPM_Primary_Rep_ID,
    CONCAT('BPT3__', CAST(ci.primary_rep_agency_id AS varchar(50))) AS RPM_Primary_Agency_ID,
    CONCAT('BPT3__', CAST(ci.product_id AS varchar(50))) AS RPM_Product_ID,
    'Item' AS Item_Type
FROM 
    [dbo].[stg_commission_items] ci