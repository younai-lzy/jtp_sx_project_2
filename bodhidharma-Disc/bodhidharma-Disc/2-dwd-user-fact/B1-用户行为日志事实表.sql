DROP CATALOG IF EXISTS hive_catalog;
CREATE CATALOG IF NOT EXISTS hive_catalog PROPERTIES (
    'type'='hms', -- required
    'hive.metastore.type' = 'hms', -- optional
    'hive.version' = '3.1.2', -- optional
    'fs.defaultFS' = 'hdfs://node101:8020', -- optional
    'hive.metastore.uris' = 'thrift://node101:9083'
);

SWITCH hive_catalog;

SHOW CATALOGS;

SHOW DATABASES;

SHOW TABLES IN bodhidharma_disc;


CREATE DATABASE IF NOT EXISTS bodhidharma_disc;

DROP TABLE IF EXISTS bodhidharma_disc.dwd_user_behavior_log;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.dwd_user_behavior_log
(
    dt                 DATE COMMENT '数据日期',
    log_id             BIGINT COMMENT '日志唯一ID',
    user_id            BIGINT NOT NULL COMMENT '用户唯一ID',
    username           STRING COMMENT '用户名',
    user_birth_of_date STRING COMMENT '用户出生日期',
    user_weight        BIGINT COMMENT '用户体重',
    behavior_type      BIGINT COMMENT '行为类型:1-浏览,2-搜索,3-收藏,4-加购,5-购买',
    behavior_time      BIGINT COMMENT '行为时间戳',
    item_id            BIGINT COMMENT '商品ID',
    product_name       STRING COMMENT '商品名称',
    category_id        INT COMMENT '商品类目ID',
    category_name      STRING COMMENT '商品所属类目名称',
    brand_id           INT COMMENT '品牌ID',
    brand_name         STRING COMMENT '品牌名称',
    price              DOUBLE COMMENT '商品价格',
    quantity           INT COMMENT '购买数量',
    platform           STRING COMMENT '平台类型:iOS/Android/PC/MiniProgram',
    os_version         STRING COMMENT '操作系统版本',
    device_model       STRING COMMENT '设备型号',
    network_type       STRING COMMENT '网络类型:WIFI/4G/5G',
    ip_address         STRING COMMENT 'IP地址',
    province           STRING COMMENT '省份',
    city               STRING COMMENT '城市',
    district           STRING COMMENT '区县',
    channel            STRING COMMENT '渠道来源',
    referer            STRING COMMENT '来源页面URL',
    search_keyword     STRING COMMENT '搜索关键词',
    duration           INT COMMENT '页面停留时长(秒)'
) DUPLICATE KEY(dt,log_id)
COMMENT '用户行为事实表，用于流量分析，粒度为每次用户行为'
PARTITION BY RANGE (dt) (
    PARTITION p202510 VALUES LESS THAN ('2025-08-10'),
    PARTITION p202511 VALUES LESS THAN ('2025-08-11')
    -- 可根据实际需求添加更多分区
)
DISTRIBUTED BY HASH(log_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);

SHOW DATABASES;
-- 用户行为日志事实表
INSERT INTO bodhidharma_disc.dwd_user_behavior_log -- 直接引用Doris内部表
SELECT t1.dt
     , t1.log_id
     , t1.user_id
     , t2.username
     , t2.user_birth_of_date
     , t2.user_weight
     , t1.behavior_type
     , t1.behavior_time
     , t1.item_id
     , t3.product_name
     , t3.category_id
     , t3.category_name
     , t1.brand_id
     , t1.brand_name
     , t1.price
     , t1.quantity
     , t1.platform
     , t1.os_version
     , t1.device_model
     , t1.network_type
     , t1.ip_address
     , t1.province
     , t1.city
     , t1.district
     , t1.channel
     , t1.referer
     , t1.search_keyword
     , t1.duration
FROM hive_catalog.bodhidharma_disc.ods_user_behavior_log t1
         LEFT JOIN hive_catalog.bodhidharma_disc.user_info t2 ON t1.user_id = t2.user_id
         LEFT JOIN hive_catalog.bodhidharma_disc.ods_product_info t3 ON t1.item_id = t3.item_id
;