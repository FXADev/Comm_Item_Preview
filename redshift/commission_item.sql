SELECT
    run.run_id,
    run.run_ym,
    run.run_month_name,
    run.run_year,
    run.run_is_closed,
    run.run_first_day_of_month,
    run.run_last_day_of_month,
    run.run_date_opened,
    run.run_date_closed,

    rep.rep_name as primary_rep_name,
    rep.rep_id as primary_rep_id,
    rep.rep_agency_id as primary_rep_agency_id,
    rep.rep_agency_name as primary_rep_agency_name,
    rep.rep_agency_group as primary_rep_agency_group,

    NULL AS referral_agency_key,
    NULL AS referral_agency_id,
    NULL AS referral_agency_name,
    NULL AS referral_agency_group,

    acct.account_id,
    acct.account_number,
    acct.account_customer_id,
    acct.account_supplier_id,
    acct.account_supplier_name,
    acct.account_customer_name,

    product.product_id,
    product.product_name,
    product.product_supplier_name,
    product.product_supplier_id,

    ci.rep_key,
    ci.account_key,
    ci.run_key,
    ci.account_group_key,
    ci.product_key,
    ci.commission_item_id,
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
    ci.commission_item_date_added,
    ci.commission_item_note_for_agents,
    ci.commission_item_note_for_staff,
    ci.commission_item_variable_2471_bill_month,
    ci.commission_item_variable_4641_master_supplier

FROM bridge."public"."commission_item" ci
INNER JOIN bridge."public"."run" run ON ci.run_key = run.run_key
INNER JOIN bridge."public"."account" acct ON ci.account_key = acct.account_key
INNER JOIN bridge."public"."rep" rep ON rep.rep_key = ci.rep_key
INNER JOIN bridge."public"."agency" agency ON rep.rep_agency_id = agency.agency_id
INNER JOIN bridge."public"."product" product ON product.product_key = ci.product_key
WHERE run.run_is_closed = false
   OR (run.run_is_closed = true AND run.run_date_closed >= DATEADD(day, -4, CURRENT_DATE))