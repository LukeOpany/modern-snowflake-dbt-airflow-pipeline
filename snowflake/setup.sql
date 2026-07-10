use role accountadmin;

create warehouse if not exists TRANSFORMING_WH
  warehouse_size = xsmall
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true;

create database if not exists ANALYTICS;

create schema if not exists ANALYTICS.DBT_DEV;

create role if not exists DBT_ROLE;

grant usage on warehouse TRANSFORMING_WH to role DBT_ROLE;

grant usage on database ANALYTICS to role DBT_ROLE;
grant usage on schema ANALYTICS.DBT_DEV to role DBT_ROLE;

grant create table on schema ANALYTICS.DBT_DEV to role DBT_ROLE;
grant create view on schema ANALYTICS.DBT_DEV to role DBT_ROLE;
grant create stage on schema ANALYTICS.DBT_DEV to role DBT_ROLE;
grant create file format on schema ANALYTICS.DBT_DEV to role DBT_ROLE;

grant select on all tables in schema ANALYTICS.DBT_DEV to role DBT_ROLE;
grant select on future tables in schema ANALYTICS.DBT_DEV to role DBT_ROLE;

grant select on all views in schema ANALYTICS.DBT_DEV to role DBT_ROLE;
grant select on future views in schema ANALYTICS.DBT_DEV to role DBT_ROLE;

grant imported privileges on database SNOWFLAKE_SAMPLE_DATA to role DBT_ROLE;

grant role DBT_ROLE to user LUKE07;