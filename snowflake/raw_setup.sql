use role ACCOUNTADMIN;

create schema if not exists ANALYTICS.RAW;

grant usage on database ANALYTICS to role DBT_ROLE;
grant usage on schema ANALYTICS.RAW to role DBT_ROLE;

grant create table on schema ANALYTICS.RAW to role DBT_ROLE;
grant create stage on schema ANALYTICS.RAW to role DBT_ROLE;
grant create file format on schema ANALYTICS.RAW to role DBT_ROLE;

grant select on all tables in schema ANALYTICS.RAW to role DBT_ROLE;
grant select on future tables in schema ANALYTICS.RAW to role DBT_ROLE;