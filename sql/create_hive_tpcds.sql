create schema hivehdfs.tpcds with (LOCATION = 'hdfs://hdfs/user/hive/warehouse/tpcds.db');
use hivehdfs.tpcds;

create table hivehdfs.tpcds.call_center with (FORMAT = 'PARQUET') as select * from tpcds.sf1.call_center;
create table hivehdfs.tpcds.catalog_page with (FORMAT = 'PARQUET') as select * from tpcds.sf1.catalog_page;
create table hivehdfs.tpcds.catalog_returns with (FORMAT = 'PARQUET') as select * from tpcds.sf1.catalog_returns;
create table hivehdfs.tpcds.catalog_sales with (FORMAT = 'PARQUET') as select * from tpcds.sf1.catalog_sales;
create table hivehdfs.tpcds.customer with (FORMAT = 'PARQUET') as select * from tpcds.sf1.customer;
create table hivehdfs.tpcds.customer_address with (FORMAT = 'PARQUET') as select * from tpcds.sf1.customer_address;
create table hivehdfs.tpcds.customer_demographics with (FORMAT = 'PARQUET') as select * from tpcds.sf1.customer_demographics;
create table hivehdfs.tpcds.date_dim with (FORMAT = 'PARQUET') as select * from tpcds.sf1.date_dim;
create table hivehdfs.tpcds.dbgen_version with (FORMAT = 'PARQUET') as select
dv_version, dv_create_date, cast(dv_create_time as varchar) as dv_create_time , dv_cmdline_args from tpcds.sf1.dbgen_version;
create table hivehdfs.tpcds.household_demographics with (FORMAT = 'PARQUET') as select * from tpcds.sf1.household_demographics;
create table hivehdfs.tpcds.income_band with (FORMAT = 'PARQUET') as select * from tpcds.sf1.income_band;
create table hivehdfs.tpcds.inventory with (FORMAT = 'PARQUET') as select * from tpcds.sf1.inventory;
create table hivehdfs.tpcds.item with (FORMAT = 'PARQUET') as select * from tpcds.sf1.item;
create table hivehdfs.tpcds.promotion with (FORMAT = 'PARQUET') as select * from tpcds.sf1.promotion;
create table hivehdfs.tpcds.reason with (FORMAT = 'PARQUET') as select * from tpcds.sf1.reason;
create table hivehdfs.tpcds.ship_mode with (FORMAT = 'PARQUET') as select * from tpcds.sf1.ship_mode;
create table hivehdfs.tpcds.store with (FORMAT = 'PARQUET') as select * from tpcds.sf1.store;
create table hivehdfs.tpcds.store_returns with (FORMAT = 'PARQUET') as select * from tpcds.sf1.store_returns;
create table hivehdfs.tpcds.store_sales with (FORMAT = 'PARQUET') as select * from tpcds.sf1.store_sales;
create table hivehdfs.tpcds.time_dim with (FORMAT = 'PARQUET') as select * from tpcds.sf1.time_dim;
create table hivehdfs.tpcds.warehouse with (FORMAT = 'PARQUET') as select * from tpcds.sf1.warehouse;
create table hivehdfs.tpcds.web_page with (FORMAT = 'PARQUET') as select * from tpcds.sf1.web_page;
create table hivehdfs.tpcds.web_returns with (FORMAT = 'PARQUET') as select * from tpcds.sf1.web_returns;
create table hivehdfs.tpcds.web_sales with (FORMAT = 'PARQUET') as select * from tpcds.sf1.web_sales;
create table hivehdfs.tpcds.web_site with (FORMAT = 'PARQUET') as select * from tpcds.sf1.web_site;