CREATE CATALOG IF NOT EXISTS hive_catalog PROPERTIES (
    'type'='hms',
    'hive.metastore.type' = 'hms',
    'hive.version' = '3.1.2',
    'fs.defaultFS' = 'hdfs://node101:8020',
    'hive.metastore.uris' = 'thrift://node101:9083'
);

# 切换catalog
switch hive_catalog;

SHOW DATABASES ;
REFRESH CATALOG hive_catalog;
SHOW TABLES IN jtp_ads_warehouse ;

SELECT * FROM jtp_ads_warehouse.ods_ads_log_inc WHERE dt = '2024-10-01' LIMIT 10 ;