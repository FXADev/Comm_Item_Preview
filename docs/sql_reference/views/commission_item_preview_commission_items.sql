/*-----------------------------------------------------------
  Commission Items Preview Query – formatted
-----------------------------------------------------------*/
SELECT
    /*------------- Run metadata -------------*/
    ci.run_id,
    ci.run_ym,
    ci.run_month_name,
    ci.run_year,
    ci.run_is_closed,
    ci.run_first_day_of_month,
    ci.run_last_day_of_month,
    ci.run_date_opened,
    ci.run_date_closed,

    /*------------- Primary rep -------------*/
    ci.primary_rep_name,
    ci.primary_rep_id,
    ci.primary_rep_agency_id,
    ci.primary_rep_agency_name,
    ci.primary_rep_agency_group,

    /*------------- Referral agency -------------*/
    ci.referral_agency_key,
    ci.referral_agency_id,
    ci.referral_agency_name,
    ci.referral_agency_group,

    /*------------- Account & customer -------------*/
    ci.account_id                              AS RPM_Account_ID,
    ci.account_number,
    ci.account_customer_id,
    ci.account_supplier_id,
    ci.account_supplier_name,
    ci.account_customer_name,

    /*------------- Product -------------*/
    ci.product_id,
    ci.product_name,
    ci.product_supplier_name,
    ci.product_supplier_id,

    /*------------- Keys -------------*/
    ci.rep_key,
    ci.account_key,
    ci.run_key,
    ci.account_group_key,
    ci.product_key,
    ci.commission_item_id,

    /*------------- Financials -------------*/
    ci.commission_item_created_from_item_name,
    ci.commission_item_net_billed,
    ci.commission_item_gross_commission,
    ci.commission_item_sales_commission,
    ci.commission_item_referral_override,
    ci.commission_item_referral_deduction,
    ci.commission_item_gross_profit,
    ci.commission_item_is_adjusted_rate,
    ci.commission_item_split_rate,
    ci.commission_item_sales_commission_rate,
    ci.commission_item_raw_sales_commission,

    /*------------- Dates / notes -------------*/
    ci.commission_item_date_added,
    ci.commission_item_note_for_agents,
    ci.commission_item_note_for_staff,

    /*------------- Variables -------------*/
    ci.commission_item_variable_2471_bill_month,
    ci.commission_item_variable_4641_master_supplier,

    /*------------- ETL metadata -------------*/
    ci.etl_batch_id,
    ci.extracted_at,

    /*------------- Derived RPM IDs -------------*/
    CONCAT('BPT3__', CAST(ci.account_customer_id   AS varchar(50))) AS RPM_Customer_ID,
    CONCAT('BPT3__', CAST(ci.account_supplier_id   AS varchar(50))) AS RPM_Supplier_ID,
    CONCAT('BPT3__', CAST(ci.primary_rep_id        AS varchar(50))) AS RPM_Primary_Rep_ID,
    CONCAT('BPT3__', CAST(ci.primary_rep_agency_id AS varchar(50))) AS RPM_Primary_Agency_ID,
    CONCAT('BPT3__', CAST(ci.product_id            AS varchar(50))) AS RPM_Product_ID,

    /*------------- Flags -------------*/
    'Item' AS Item_Type,

    /*------------- Salesforce IDs -------------*/
    aci.Company_ID__c  AS SF_Company_ID,
    ag.Agency_ID__c    AS SF_Agency_ID,
    sup.Supplier_ID__c AS SF_Supplier_ID,
    strat.sales_rep_id__c     AS SF_Rep_ID,
    ac.account_id__c AS SF_Account_ID

FROM
    dbo.stg_commission_items               ci
LEFT JOIN dbo.stg_sf_commission_account ac ON ci.account_id = ac.account_id__c            -- account match
LEFT JOIN dbo.stg_sf_agency_company_identifier aci ON ci.account_customer_id     = aci.Company_ID__c             -- customer match
LEFT JOIN dbo.stg_sf_agency               ag ON ci.primary_rep_agency_id  = ag.Agency_ID__c                -- agency match
LEFT JOIN dbo.stg_sf_supplier             sup ON ci.account_supplier_id     = sup.Supplier_ID__c            -- supplier match
LEFT JOIN dbo.stg_sf_strategist           strat ON ci.primary_rep_agency_id = strat.sales_rep_id__c         -- rep match
LEFT JOIN dbo.stg_sf_company              comp ON ci.account_customer_id     = comp.Company_ID__c;           -- company match
