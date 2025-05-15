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

    agency.agency_id,
    agency.agency_name,
    agency.agency_group,

    supplier.supplier_id,
    supplier.supplier_name,

    adj.commission_adjustment_id,
    adj.run_key,
    adj.supplier_key,
    adj.agency_key,
    adj.commission_adjustment_net_billed,
    adj.commission_adjustment_gross_commission,
    adj.commission_adjustment_sales_commission,
    adj.commission_adjustment_note_for_staff,
    adj.commission_adjustment_note,
    adj.commission_adjustment_last_modified,
    
    adj_type.commission_adjustment_type_key,
    adj_type.commission_adjustment_type_name
    
FROM bridge."public"."commission_adjustment" adj
INNER JOIN bridge."public"."run" run ON run.run_key = adj.run_key
INNER JOIN bridge."public"."agency" agency ON agency.agency_key = adj.agency_key
INNER JOIN bridge."public"."supplier" supplier ON supplier.supplier_key = adj.supplier_key
INNER JOIN bridge."public"."commission_adjustment_type" adj_type ON adj_type.commission_adjustment_type_key = adj.commission_adjustment_type_key
WHERE run.run_is_closed = false
   OR (run.run_is_closed = true AND run.run_date_closed >= DATEADD(day, -4, CURRENT_DATE))
   