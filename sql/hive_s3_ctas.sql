create schema hives3.tpcds with (LOCATION = 's3a://demo/tpcds.db');
use hives3.tpcds;

create table hives3.tpcds.call_center with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.call_center;
create table hives3.tpcds.catalog_page with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.catalog_page;
create table hives3.tpcds.catalog_returns with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.catalog_returns;
create table hives3.tpcds.catalog_sales with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.catalog_sales;
create table hives3.tpcds.customer with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.customer;
create table hives3.tpcds.customer_address with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.customer_address;
create table hives3.tpcds.customer_demographics with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.customer_demographics;
create table hives3.tpcds.date_dim with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.date_dim;
create table hives3.tpcds.dbgen_version with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.dbgen_version;
create table hives3.tpcds.household_demographics with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.household_demographics;
create table hives3.tpcds.income_band with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.income_band;
create table hives3.tpcds.inventory with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.inventory;
create table hives3.tpcds.item with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.item;
create table hives3.tpcds.promotion with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.promotion;
create table hives3.tpcds.reason with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.reason;
create table hives3.tpcds.ship_mode with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.ship_mode;
create table hives3.tpcds.store with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.store;
create table hives3.tpcds.store_returns with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.store_returns;
create table hives3.tpcds.store_sales with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.store_sales;
create table hives3.tpcds.time_dim with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.time_dim;
create table hives3.tpcds.warehouse with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.warehouse;
create table hives3.tpcds.web_page with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.web_page;
create table hives3.tpcds.web_returns with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.web_returns;
create table hives3.tpcds.web_sales with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.web_sales;
create table hives3.tpcds.web_site with (FORMAT = 'ORC', transactional = true) as select * from hivehdfs.tpcds.web_site;