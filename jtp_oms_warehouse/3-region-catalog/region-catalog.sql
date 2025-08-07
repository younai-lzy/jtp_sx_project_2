
DROP CATALOG IF EXISTS mysql_catalog;
CREATE CATALOG mysql_catalog PROPERTIES (
    'type' = 'jdbc',
    'user' = 'root',
    'password' = '123456',
    'jdbc_url' = 'jdbc:mysql://node103:3306',
    'driver_url' = 'mysql-connector-j-8.0.33.jar',
    'driver_class' = 'com.mysql.cj.jdbc.Driver'
);

use mysql_catalog.jtp_oms;
SHOW CATALOGS;
SHOW DATABASES;
SWITCH CATALOG mysql_catalog;
    use catalog postgresql_catalog;
DROP CATALOG IF EXISTS postgresql_catalog;
CREATE CATALOG postgresql_catalog PROPERTIES (
    'type' = 'jdbc',
    'user' = 'postgres',
    'password' = '123456',
    'jdbc_url' = 'jdbc:postgresql://node103:5432/postgres',
    'driver_url' = 'postgresql-42.5.6.jar',
    'driver_class' = 'org.postgresql.Driver'
);

use jtp_oms_catalog.public;

select *
from ads_coupon_daily_report;
USE postgresql_catalog.jtp_oms.ads_coupon_daily_report;
SELECT * FROM postgresql_catalog.jtp_oms.ads_coupon_daily_report;
show tables ;

drop catalog if exists jtp_oms_catalog;
CREATE CATALOG jtp_oms_catalog PROPERTIES (
    'type' = 'jdbc',
    'user' = 'postgres',
    'password' = '123456',
    'jdbc_url' = 'jdbc:postgresql://node103:5432/jtp_oms',
    'driver_url' = 'file:///opt/module/doris/fe/jdbc_drivers/postgresql-42.5.6.jar',
    'driver_class' = 'org.postgresql.Driver'
);
SHOW DATABASES FROM jtp_oms_catalog;


SELECT * FROM jtp_oms_catalog.public.ads_coupon_daily_report;

USE CATALOG mysql_catalog
SHOW PROC '/frontends';
select *
from ;