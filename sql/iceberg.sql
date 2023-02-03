create schema if not exists iceberg.iceberg_test with (LOCATION = 's3a://demo/iceberg_test.db');
use iceberg.iceberg_test;

create table if not exists iceberg.iceberg_test.call_center
    with (FORMAT = 'PARQUET')
    as select
        cc_call_center_sk,
        cast(cc_call_center_id as varchar(16)) as cc_call_center_id,
        cc_rec_start_date,
        cc_rec_end_date,
        cc_closed_date_sk,
        cc_open_date_sk,
        cc_name,
        cc_class,
        cc_employees,
        cc_sq_ft,
        cast(cc_hours as varchar(20)) as cc_hours,
        cc_manager,
        cc_mkt_id,
        cast(cc_mkt_class as varchar(50)) as cc_mkt_class,
        cc_mkt_desc,
        cc_market_manager,
        cc_division,
        cc_division_name,
        cc_company,
        cast(cc_company_name as varchar(50)) as cc_company_name,
        cast(cc_street_number as varchar(10)) as cc_street_number,
        cc_street_name,
        cast(cc_street_type as varchar(15)) as cc_street_type,
        cast(cc_suite_number as varchar(10)) as cc_suite_number,
        cc_city,
        cc_county,
        cast(cc_state as varchar(2)) as cc_state,
        cast(cc_zip as varchar(10)) as cc_zip,
        cc_country,
        cc_gmt_offset,
        cc_tax_percentage
    from hivehdfs.tpcds.call_center;

create table if not exists iceberg.iceberg_test.catalog_page
    with (FORMAT = 'PARQUET')
    as select
        cp_catalog_page_sk,
        cast(cp_catalog_page_id as varchar(16)) as cp_catalog_page_id,
        cp_start_date_sk,
        cp_end_date_sk,
        cp_department,
        cp_catalog_number,
        cp_catalog_page_number,
        cp_description,
        cp_type
    from hivehdfs.tpcds.catalog_page;

create table if not exists iceberg.iceberg_test.catalog_returns
    with (FORMAT = 'PARQUET')
    as select * from hivehdfs.tpcds.catalog_returns;

create table if not exists iceberg.iceberg_test.catalog_sales
    with (FORMAT = 'PARQUET')
    as select * from hivehdfs.tpcds.catalog_sales;

create table if not exists iceberg.iceberg_test.customer
    with (FORMAT = 'PARQUET')
    as select
        c_customer_sk,
        cast(c_customer_id as varchar(16)) as c_customer_id,
        c_current_cdemo_sk,
        c_current_hdemo_sk,
        c_current_addr_sk,
        c_first_shipto_date_sk,
        c_first_sales_date_sk,
        cast(c_salutation as varchar(10)) as c_salutation,
        cast(c_first_name as varchar(20)) as c_first_name,
        cast(c_last_name as varchar(30)) as c_last_name,
        cast(c_preferred_cust_flag as varchar(1)) as c_preferred_cust_flag,
        c_birth_day,
        c_birth_month,
        c_birth_year,
        c_birth_country,
        cast(c_login as varchar(13)) as c_login,
        cast(c_email_address as varchar(50)) as c_email_address,
        c_last_review_date_sk
    from hivehdfs.tpcds.customer;

create table if not exists iceberg.iceberg_test.customer_address
    with (FORMAT = 'PARQUET')
    as select
        ca_address_sk,
        cast(ca_address_id as varchar(16)) as ca_address_id,
        cast(ca_street_number as varchar(10)) as ca_street_number,
        ca_street_name,
        cast(ca_street_type as varchar(15)) as ca_street_type,
        cast(ca_suite_number as varchar(10)) as ca_suite_number,
        ca_city,
        ca_county,
        cast(ca_state as varchar(2)) as ca_state,
        cast(ca_zip as varchar(10)) as ca_zip,
        ca_country,
        ca_gmt_offset,
        cast(ca_location_type as varchar(20)) as ca_location_type
    from hivehdfs.tpcds.customer_address;

create table if not exists iceberg.iceberg_test.customer_demographics
    with (FORMAT = 'PARQUET')
    as select
        cd_demo_sk,
        cast(cd_gender as varchar(1)) as cd_gender,
        cast(cd_marital_status as varchar(1)) as cd_marital_status,
        cast(cd_education_status as varchar(20)) as cd_education_status,
        cd_purchase_estimate,
        cast(cd_credit_rating as varchar(10)) as cd_credit_rating,
        cd_dep_count,
        cd_dep_employed_count,
        cd_dep_college_count
    from hivehdfs.tpcds.customer_demographics;

create table if not exists iceberg.iceberg_test.date_dim
    with (FORMAT = 'PARQUET')
    as select
        d_date_sk,
        cast(d_date_id as varchar(16)) as d_date_id,
        d_date,
        d_month_seq,
        d_week_seq,
        d_quarter_seq,
        d_year,
        d_dow,
        d_moy,
        d_dom,
        d_qoy,
        d_fy_year,
        d_fy_quarter_seq,
        d_fy_week_seq,
        cast(d_day_name as varchar(9)) as d_day_name,
        cast(d_quarter_name as varchar(6)) as d_quarter_name,
        cast(d_holiday as varchar(1)) as d_holiday,
        cast(d_weekend as varchar(1)) as d_weekend,
        cast(d_following_holiday as varchar(1)) as d_following_holiday,
        d_first_dom,
        d_last_dom,
        d_same_day_ly,
        d_same_day_lq,
        cast(d_current_day as varchar(1)) as d_current_day,
        cast(d_current_week as varchar(1)) as d_current_week,
        cast(d_current_month as varchar(1)) as d_current_month,
        cast(d_current_quarter as varchar(1)) as d_current_quarter,
        cast(d_current_year as varchar(1)) as d_current_year
    from hivehdfs.tpcds.date_dim;

create table if not exists iceberg.iceberg_test.dbgen_version
    with (FORMAT = 'PARQUET')
    as select * from hivehdfs.tpcds.dbgen_version;

create table if not exists iceberg.iceberg_test.household_demographics
    with (FORMAT = 'PARQUET')
    as select
        hd_demo_sk,
        hd_income_band_sk,
        cast(hd_buy_potential as varchar(15)) as hd_buy_potential,
        hd_dep_count,
        hd_vehicle_count
    from hivehdfs.tpcds.household_demographics;

create table if not exists iceberg.iceberg_test.income_band
    with (FORMAT = 'PARQUET')
    as select * from hivehdfs.tpcds.income_band;

create table if not exists iceberg.iceberg_test.inventory
    with (FORMAT = 'PARQUET')
    as select * from hivehdfs.tpcds.inventory;

create table if not exists iceberg.iceberg_test.item
    with (FORMAT = 'PARQUET')
    as select
        i_item_sk,
        cast(i_item_id as varchar(16)) as i_item_id,
        i_rec_start_date,
        i_rec_end_date,
        i_item_desc,
        i_current_price,
        i_wholesale_cost,
        i_brand_id,
        cast(i_brand as varchar(50)) as i_brand,
        i_class_id,
        cast(i_class as varchar(50)) as i_class,
        i_category_id,
        cast(i_category as varchar(50)) as i_category,
        i_manufact_id,
        cast(i_manufact as varchar(50)) as i_manufact,
        cast(i_size as varchar(20)) as i_size,
        cast(i_formulation as varchar(20)) as i_formulation,
        cast(i_color as varchar(20)) as i_color,
        cast(i_units as varchar(10)) as i_units,
        cast(i_container as varchar(10)) as i_container,
        i_manager_id,
        cast(i_product_name as varchar(20)) as i_product_name
    from hivehdfs.tpcds.item;


create table if not exists iceberg.iceberg_test.promotion
    with (FORMAT = 'PARQUET')
    as select
        p_promo_sk,
        cast(p_promo_id as varchar(16)) as p_promo_id,
        p_start_date_sk,
        p_end_date_sk,
        p_item_sk,
        p_cost,
        p_response_targe,
        cast(p_promo_name as varchar(50)) as p_promo_name,
        cast(p_channel_dmail as varchar(1)) as p_channel_dmail,
        cast(p_channel_email as varchar(1)) as p_channel_email,
        cast(p_channel_catalog as varchar(1)) as p_channel_catalog,
        cast(p_channel_tv as varchar(1)) as p_channel_tv,
        cast(p_channel_radio as varchar(1)) as  p_channel_radio,
        cast(p_channel_press as varchar(1)) as p_channel_press,
        cast(p_channel_event as varchar(1)) as p_channel_event,
        cast(p_channel_demo as varchar(1)) as p_channel_demo,
        p_channel_details,
        cast(p_purpose as varchar(15)) as p_purpose,
        cast(p_discount_active as varchar(1)) as p_discount_active
    from hivehdfs.tpcds.promotion;

create table if not exists iceberg.iceberg_test.reason
    with (FORMAT = 'PARQUET')
    as select
        r_reason_sk,
        cast(r_reason_id as varchar(16)) as r_reason_id,
        cast(r_reason_desc as varchar(100)) as r_reason_desc
    from hivehdfs.tpcds.reason;

create table if not exists iceberg.iceberg_test.ship_mode
    with (FORMAT = 'PARQUET')
    as select
        sm_ship_mode_sk,
        cast(sm_ship_mode_id as varchar(16)) as sm_ship_mode_id,
        cast(sm_type as varchar(30)) as sm_type,
        cast(sm_code as varchar(10)) as sm_code,
        cast(sm_carrier as varchar(20)) as sm_carrier,
        cast(sm_contract as varchar(20)) as sm_contract
    from hivehdfs.tpcds.ship_mode;

create table if not exists iceberg.iceberg_test.store
    with (FORMAT = 'PARQUET')
    as select
        s_store_sk,
        cast(s_store_id as varchar(16)) as s_store_id,
        s_rec_start_date,
        s_rec_end_date,
        s_closed_date_sk,
        cast(s_store_name as varchar(50)) as s_store_name,
        s_number_employees,
        s_floor_space,
        cast(s_hours as varchar(20)) as s_hours,
        s_manager,
        s_market_id,
        s_geography_class,
        s_market_desc,
        s_market_manager,
        s_division_id,
        s_division_name,
        s_company_id,
        s_company_name,
        s_street_number,
        s_street_name,
        cast(s_street_type as varchar(15)) as s_street_type,
        cast(s_suite_number as varchar(10)) as s_suite_number,
        s_city,
        s_county,
        cast(s_state as varchar(2)) as s_state,
        cast(s_zip as varchar(10)) as s_zip,
        s_country,
        s_gmt_offset,
        s_tax_precentage
    from hivehdfs.tpcds.store;

create table if not exists iceberg.iceberg_test.store_returns
    with (FORMAT = 'PARQUET')
    as select * from hivehdfs.tpcds.store_returns;

create table if not exists iceberg.iceberg_test.store_sales
    with (FORMAT = 'PARQUET')
    as select * from hivehdfs.tpcds.store_sales;

create table if not exists iceberg.iceberg_test.time_dim
    with (FORMAT = 'PARQUET')
    as select
        t_time_sk,
        cast(t_time_id as varchar(16)) as t_time_id,
        t_time,
        t_hour,
        t_minute,
        t_second,
        cast(t_am_pm as varchar(2)) as t_am_pm,
        cast(t_shift as varchar(20)) as t_shift,
        cast(t_sub_shift as varchar(20)) as t_sub_shift,
        cast(t_meal_time as varchar(20)) as t_meal_time
    from hivehdfs.tpcds.time_dim;

create table if not exists iceberg.iceberg_test.warehouse
    with (FORMAT = 'PARQUET')
    as select
        w_warehouse_sk,
        cast(w_warehouse_id as varchar(16)) as w_warehouse_id,
        w_warehouse_name,
        w_warehouse_sq_ft,
        cast(w_street_number as varchar(20)) as w_street_number,
        w_street_name,
        cast(w_street_type as varchar(15)) as w_street_type,
        cast(w_suite_number as varchar(10)) as w_suite_number,
        w_city,
        w_county,
        cast(w_state as varchar(2)) as w_state,
        cast(w_zip as varchar(10)) as w_zip,
        w_country,
        w_gmt_offset
    from hivehdfs.tpcds.warehouse;

create table if not exists iceberg.iceberg_test.web_page
    with (FORMAT = 'PARQUET')
    as select
        wp_web_page_sk,
        cast(wp_web_page_id as varchar(16)) as wp_web_page_id,
        wp_rec_start_date,
        wp_rec_end_date,
        wp_creation_date_sk,
        wp_access_date_sk,
        cast(wp_autogen_flag as varchar(1)) as wp_autogen_flag,
        wp_customer_sk,
        wp_url,
        cast(wp_type as varchar(50)) as wp_type,
        wp_char_count,
        wp_link_count,
        wp_image_count,
        wp_max_ad_count
    from hivehdfs.tpcds.web_page;

create table if not exists iceberg.iceberg_test.web_returns
    with (FORMAT = 'PARQUET')
    as select * from hivehdfs.tpcds.web_returns;

create table if not exists iceberg.iceberg_test.web_sales
    with (FORMAT = 'PARQUET')
    as select * from hivehdfs.tpcds.web_sales;

create table if not exists iceberg.iceberg_test.web_site
    with (FORMAT = 'PARQUET')
    as select
        web_site_sk,
        cast(web_site_id as varchar(16)) as web_site_id,
        web_rec_start_date,
        web_rec_end_date,
        web_name,
        web_open_date_sk,
        web_close_date_sk,
        web_class,
        web_manager,
        web_mkt_id,
        web_mkt_class,
        web_mkt_desc,
        web_market_manager,
        web_company_id,
        cast(web_company_name as varchar(50)) as web_company_name,
        cast(web_street_number as varchar(10)) as web_street_number,
        web_street_name,
        cast(web_street_type as varchar(15)) as web_street_type,
        cast(web_suite_number as varchar(10)) as web_suite_number,
        web_city,
        web_county,
        cast(web_state as varchar(2)) as web_state,
        cast(web_zip as varchar(10)) as web_zip,
        web_country,
        web_gmt_offset,
        web_tax_percentage
    from hivehdfs.tpcds.web_site;
