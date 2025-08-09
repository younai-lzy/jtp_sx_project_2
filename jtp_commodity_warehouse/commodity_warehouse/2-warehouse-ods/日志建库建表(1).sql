CREATE DATABASE IF NOT EXISTS jtp_commodity_warehouse
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse';
USE jtp_commodity_warehouse;

-- 1. 商品价格变动日志表（按变动日期分区）
DROP TABLE IF EXISTS ods_price_trend_log;
CREATE TABLE IF NOT EXISTS ods_price_trend_log (
    id INT COMMENT '自增主键',
    sku_id INT NOT NULL COMMENT 'SKU ID',
    price_before DECIMAL(10, 2) NOT NULL COMMENT '价格变动前',
    price_after DECIMAL(10, 2) NOT NULL COMMENT '价格变动后',
    change_time TIMESTAMP NOT NULL COMMENT '价格变动发生的时间'
)
COMMENT '商品价格变动日志表'
PARTITIONED BY (dt STRING COMMENT '订单日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_price_trend_log';

ALTER TABLE ods_price_trend_log ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');

-- 2. 用户行为日志表（按日期分区）
DROP TABLE IF EXISTS ods_user_action_log;
CREATE TABLE IF NOT EXISTS ods_user_action_log (
    log_id BIGINT NOT NULL COMMENT '日志ID，唯一标识一条行为记录',
    session_id STRING NOT NULL COMMENT '会话ID',
    user_id INT NOT NULL COMMENT '用户ID',
    sku_id INT COMMENT '用户行为相关的SKU ID',
    content_id INT COMMENT '用户行为相关的内容ID',
    content_type STRING COMMENT '内容类型：live, short_video, graphic',
    action_type STRING NOT NULL COMMENT '行为类型',
    source_channel STRING NOT NULL COMMENT '流量来源渠道',
    log_timestamp BIGINT NOT NULL COMMENT '行为发生时的时间戳'
)
COMMENT '用户行为日志表'
PARTITIONED BY (dt STRING COMMENT '订单日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_user_action_log';




ALTER TABLE ods_user_action_log ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');

