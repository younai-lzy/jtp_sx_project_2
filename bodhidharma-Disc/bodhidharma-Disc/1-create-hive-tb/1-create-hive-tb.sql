CREATE DATABASE IF NOT EXISTS `bodhidharma_disc`
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/bodhidharma_disc';
USE `bodhidharma_disc`;


DROP TABLE IF EXISTS bodhidharma_disc.ods_user_behavior_log;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.ods_user_behavior_log
(
    `log_id`         BIGINT COMMENT '日志唯一ID',
    `user_id`        BIGINT NOT NULL COMMENT '用户唯一ID',
    `behavior_type`  BIGINT COMMENT '行为类型:1-浏览,2-搜索,3-收藏,4-加购,5-购买',
    `behavior_time`  BIGINT COMMENT '行为时间戳',
    `item_id`        BIGINT COMMENT '商品ID',
    `brand_id`       INT COMMENT '品牌ID',
    `brand_name`     STRING COMMENT '品牌名称',
    `price`          DECIMAL(10, 2) COMMENT '商品价格',
    `quantity`       INT COMMENT '购买数量',
    `platform`       STRING COMMENT '平台类型:iOS/Android/PC/MiniProgram',
    `os_version`     STRING COMMENT '操作系统版本',
    `device_model`   STRING COMMENT '设备型号',
    `network_type`   STRING COMMENT '网络类型:WIFI/4G/5G',
    `ip_address`     STRING COMMENT 'IP地址',
    `province`       STRING COMMENT '省份',
    `city`           STRING COMMENT '城市',
    `district`       STRING COMMENT '区县',
    `channel`        STRING COMMENT '渠道来源',
    `referer`        STRING COMMENT '来源页面URL',
    `search_keyword` STRING COMMENT '搜索关键词',
    `duration`       INT COMMENT '页面停留时长(秒)'
)
    COMMENT '用户行为日志事实表'
    PARTITIONED BY (`dt` STRING COMMENT '日期分区(yyyy-MM-dd)')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/bodhidharma_disc/ods_user_behavior_log'
;

-- 加载数据
LOAD DATA LOCAL INPATH '/home/bwie/data/ods_user_behavior_log.csv' OVERWRITE INTO TABLE bodhidharma_disc.ods_user_behavior_log PARTITION (dt = '2025-08-10');

SELECT *
FROM ods_user_behavior_log
;

DROP TABLE IF EXISTS bodhidharma_disc.ods_product_info;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.ods_product_info
(
    item_id       BIGINT COMMENT '商品ID',
    product_name  STRING COMMENT '商品名称',
    `category_id` INT COMMENT '商品类目ID',
    category_name STRING COMMENT '商品所属类目名称'
)
    COMMENT '原始商品信息表'
    PARTITIONED BY (`dt` STRING COMMENT '日期分区(yyyy-MM-dd)')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/bodhidharma_disc/ods_product_info';

-- 加载数据
LOAD DATA LOCAL INPATH '/home/bwie/data/ods_product_info.csv' OVERWRITE INTO TABLE bodhidharma_disc.ods_product_info PARTITION (dt = '2025-08-10');
SELECT *
FROM ods_product_info
;

DROP TABLE IF EXISTS bodhidharma_disc.user_info;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.user_info
(
    `user_id`            BIGINT COMMENT '用户唯一ID',
    `username`           STRING COMMENT '用户名',
    `user_birth_of_date` STRING COMMENT '用户出生日期',
    `user_weight`        BIGINT COMMENT '用户体重',
    `province`           STRING COMMENT '省份',
    `city`               STRING COMMENT '城市',
    `district`           STRING COMMENT '区县',
    `address`            STRING COMMENT '详细地址',
    `postcode`           STRING COMMENT '邮编',
    `mobile`             STRING COMMENT '手机号'
)
    COMMENT '用户信息表'
    PARTITIONED BY (`dt` STRING COMMENT '日期分区(yyyy-MM-dd)')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/bodhidharma_disc/user_info'
;

-- 加载数据
LOAD DATA LOCAL INPATH '/home/bwie/data/user_info.csv' OVERWRITE INTO TABLE bodhidharma_disc.user_info PARTITION (dt = '2025-08-10');
SELECT *
FROM user_info
;
