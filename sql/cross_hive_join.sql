create schema if not exists hives3.tps_reports with (LOCATION = 's3a://demo/tps_reports.db');

create table hives3.tps_reports.loyalty_by_state as
        select
            coalesce(ca.ca_state, 'STATE_UNKNOWN') as state,
            coalesce(c.c_preferred_cust_flag, 'PREFERRED_STATUS_UNKNOWN') as preferred,
            count(*) as "count"
        from hives3.tpcds.customer_address ca
        join hivehdfs.tpcds.customer c
                on ca_address_id = c_customer_id
        group by ca_state, c_preferred_cust_flag;