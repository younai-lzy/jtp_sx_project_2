CREATE DATABASE IF NOT EXISTS `bodhidharma_disc`
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/bodhidharma_disc';
USE `bodhidharma_disc`;


DROP TABLE IF EXISTS bodhidharma_disc.ods_user_behavior_log;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.ods_user_behavior_log
(
    `log_id`         BIGINT COMMENT '日志唯一ID',
    `order_id`       BIGINT NOT NULL COMMENT '订单ID',
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

-- 商品信息表
DROP TABLE IF EXISTS bodhidharma_disc.ods_product_info;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.ods_product_info
(
    `item_id`       BIGINT COMMENT '商品ID',
    `product_name`  STRING COMMENT '商品名称',
    `category_id` INT COMMENT '商品类目ID',
    `category_name` STRING COMMENT '商品所属类目名称'
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

-- 用户信息表
DROP TABLE IF EXISTS bodhidharma_disc.ods_user_info;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.ods_user_info
(
    `user_id`            BIGINT COMMENT '用户唯一ID',
    `username`           STRING COMMENT '用户名',
    `user_birth_of_date` STRING COMMENT '用户出生日期',
    `user_weight`        BIGINT COMMENT '用户体重',
    `user_height`        BIGINT COMMENT '用户身高',
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
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/bodhidharma_disc/ods_user_info'
;

-- 加载数据
LOAD DATA LOCAL INPATH '/home/bwie/data/ods_user_info.csv' OVERWRITE INTO TABLE bodhidharma_disc.ods_user_info PARTITION (dt = '2025-08-10');

SELECT *
FROM ods_user_info
;

-- 订单表
DROP TABLE IF EXISTS bodhidharma_disc.ods_order_info;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.ods_order_info
(
    `order_id`          BIGINT COMMENT '订单ID',
    `user_id`           BIGINT COMMENT '用户ID',
    `order_time`        TIMESTAMP COMMENT '下单时间',
    `pay_time`          TIMESTAMP COMMENT '支付时间',
    `pay_amount`        DECIMAL(10, 2) COMMENT '支付金额',
    `pay_status`        BIGINT COMMENT '支付状态:0-未支付,1-已支付,2-支付失败',
    `delivery_time`     TIMESTAMP COMMENT '发货时间',
    `size_choice`       STRING COMMENT '尺码选择',
    `learning_products` VARCHAR(255) COMMENT '智能硬件（体重秤、健康类商品、）',
    `is_member`         BOOLEAN COMMENT '是否会员:0-非会员,1-会员',
    `activity_id`       BIGINT COMMENT '活动ID'
)
    COMMENT '原始商品信息表'
    PARTITIONED BY (`dt` STRING COMMENT '日期分区(yyyy-MM-dd)')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/bodhidharma_disc/ods_order_info';

-- 加载数据
LOAD DATA LOCAL INPATH '/home/bwie/data/ods_order_info.csv' OVERWRITE INTO TABLE bodhidharma_disc.ods_order_info PARTITION (dt = '2025-08-10');

SELECT *
FROM ods_order_info
;

-- 创建 Doris 活动信息表
DROP TABLE IF EXISTS bodhidharma_disc.ods_activity_info;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.ods_activity_info
(
    `activity_id`   BIGINT COMMENT '活动ID',
    `activity_name` VARCHAR(255) COMMENT '活动名称',
    `coupon_id`     BIGINT COMMENT '优惠券ID',
    `activity_type` STRING COMMENT '活动类型 (例如: 促销, 节日, 新品发布)',
    `start_time`    TIMESTAMP COMMENT '活动开始时间',
    `end_time`      TIMESTAMP COMMENT '活动结束时间',
    `creation_time` TIMESTAMP COMMENT '活动创建时间'
)
    COMMENT '原始商品信息表'
    PARTITIONED BY (`dt` STRING COMMENT '日期分区(yyyy-MM-dd)')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/bodhidharma_disc/ods_activity_info'
;
-- 加载数据
LOAD DATA LOCAL INPATH '/home/bwie/data/ods_activity_info.csv' OVERWRITE INTO TABLE bodhidharma_disc.ods_activity_info PARTITION (dt = '2025-08-10');

SELECT *
FROM ods_activity_info
;
