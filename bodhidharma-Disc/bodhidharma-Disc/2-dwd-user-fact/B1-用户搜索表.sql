DROP CATALOG IF EXISTS hive_catalogs;
CREATE CATALOG IF NOT EXISTS hive_catalogs PROPERTIES (
    'type'='hms', -- required
    'hive.metastore.type' = 'hms', -- optional
    'hive.version' = '3.1.2', -- optional
    'fs.defaultFS' = 'hdfs://node101:8020', -- optional
    'hive.metastore.uris' = 'thrift://node101:9083'
);

SWITCH hive_catalogs;

SHOW CATALOGS;

SHOW DATABASES;

SHOW TABLES IN bodhidharma_disc;


CREATE DATABASE IF NOT EXISTS bodhidharma_disc;
USE bodhidharma_disc;

DROP TABLE IF EXISTS bodhidharma_disc.dwd_user_search_log;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.dwd_user_search_log(
    dt                 DATE COMMENT '数据日期',
    log_id             BIGINT COMMENT '日志唯一ID',
    order_id           BIGINT COMMENT '订单ID',
    user_id            BIGINT COMMENT '用户唯一ID',
    username           VARCHAR(255) COMMENT '用户名',
    user_birth_of_date VARCHAR(255) COMMENT '用户出生日期',
    user_weight        BIGINT COMMENT '用户体重',
    user_height        BIGINT COMMENT '用户身高',
    behavior_type      BIGINT COMMENT '行为类型:1-浏览,2-搜索,3-收藏,4-加购,5-购买',
    behavior_time      BIGINT COMMENT '行为时间戳',
    item_id            BIGINT COMMENT '商品ID',
    product_name       VARCHAR(255) COMMENT '商品名称',
    category_id        INT COMMENT '商品类目ID',
    category_name      VARCHAR(255) COMMENT '商品所属类目名称',
    brand_id           INT COMMENT '品牌ID',
    brand_name         VARCHAR(255) COMMENT '品牌名称',
    price              DECIMAL(10, 2) COMMENT '商品价格',
    quantity           BIGINT COMMENT '购买数量',
    platform           VARCHAR(255) COMMENT '平台类型:iOS/Android/PC/MiniProgram',
    os_version         VARCHAR(255) COMMENT '操作系统版本',
    device_model       VARCHAR(255) COMMENT '设备型号',
    network_type       VARCHAR(255) COMMENT '网络类型:WIFI/4G/5G',
    ip_address         VARCHAR(255) COMMENT 'IP地址',
    province           VARCHAR(255) COMMENT '省份',
    city               VARCHAR(255) COMMENT '城市',
    district           VARCHAR(255) COMMENT '区县',
    `channel`          VARCHAR(255) COMMENT '渠道来源',
    referer            VARCHAR(255) COMMENT '来源页面URL',
    search_keyword     VARCHAR(255) COMMENT '搜索关键词'
) DUPLICATE KEY(dt,log_id)
COMMENT '用户搜索表'
PARTITION BY RANGE (dt) (
    PARTITION p202510 VALUES LESS THAN ('2025-08-10'),
    PARTITION p202511 VALUES LESS THAN ('2025-08-11')
    -- 可根据实际需求添加更多分区
)
DISTRIBUTED BY HASH(log_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);




-- 用户搜索
INSERT INTO bodhidharma_disc.dwd_user_search_log
SELECT
    t1.dt
    ,t1.log_id
    ,t1.order_id
    ,t1.user_id
    ,t2.username
    ,t2.user_birth_of_date
    ,t2.user_weight
    ,t2.user_height
    ,t1.behavior_type
    ,t1.behavior_time
    ,t1.item_id
    ,t3.product_name
    ,t3.category_id
    ,t3.category_name
    ,t1.brand_id
    ,t1.brand_name
    ,t1.price
    ,t1.quantity
    ,t1.platform
    ,t1.os_version
    ,t1.device_model
    ,t1.network_type
    ,t1.ip_address
    ,t1.province
    ,t1.city
    ,t1.district
    ,t1.`channel`
    ,t1.referer
    ,t1.search_keyword
FROM hive_catalogs.bodhidharma_disc.ods_user_behavior_log t1
LEFT JOIN hive_catalogs.bodhidharma_disc.ods_user_info t2 ON t1.user_id = t2.user_id
LEFT JOIN hive_catalogs.bodhidharma_disc.ods_product_info t3 ON t1.item_id = t3.item_id
WHERE t1.behavior_type = 2
;