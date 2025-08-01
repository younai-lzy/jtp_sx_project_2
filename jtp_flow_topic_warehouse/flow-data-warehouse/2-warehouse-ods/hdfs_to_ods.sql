CREATE DATABASE IF NOT EXISTS jtp_flow_topic_warehouse
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse';
USE jtp_flow_topic_warehouse;
-- ODS层表结构

-- 1. ods_user_action_log: 用户行为日志表
-- 记录用户在电商平台上的各种行为，如页面浏览、点击、加入购物车、购买等。
DROP TABLE IF EXISTS ods_user_action_log;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_user_action_log
(
    log_id      STRING COMMENT '日志唯一ID',
    user_id     STRING COMMENT '用户ID',
    session_id  STRING COMMENT '会话ID',
    page_id     STRING COMMENT '页面ID',
    element_id  STRING COMMENT '点击元素ID (如果事件类型为点击)',
    event_type  STRING COMMENT '事件类型 (page_view, click, add_to_cart, purchase)',
    event_time  TIMESTAMP COMMENT '事件发生时间',
    product_id  STRING COMMENT '如果事件与商品相关，记录商品ID',
    ip_address  STRING COMMENT '用户IP地址',
    device_type STRING COMMENT '设备类型 (PC, Mobile)'
)
    COMMENT '用户行为日志原始表'
    PARTITIONED BY (dt STRING COMMENT '分区日期，格式YYYYMMDD')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/ods_user_action_log';
-- 查询

ALTER TABLE ods_user_action_log
    ADD IF NOT EXISTS PARTITION (dt = '2025-07-31');
SELECT log_id,
       user_id,
       session_id,
       page_id,
       element_id,
       event_type,
       event_time,
       product_id,
       ip_address,
       device_type,
       dt
FROM ods_user_action_log
WHERE dt = '2025-07-31'
LIMIT 10
;

-- 2. ods_order_info: 订单信息表
-- 记录用户的订单详情，用于计算引导支付金额。
DROP TABLE IF EXISTS ods_order_info;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_order_info
(
    order_id     STRING COMMENT '订单ID',
    user_id      STRING COMMENT '用户ID',
    product_id   STRING COMMENT '商品ID',
    order_amount DOUBLE COMMENT '订单金额',
    order_time   TIMESTAMP COMMENT '下单时间',
    pay_time     TIMESTAMP COMMENT '支付时间',
    order_status STRING COMMENT '订单状态 (paid, unpaid, cancelled)'
)
    COMMENT '订单信息原始表'
    PARTITIONED BY (dt STRING COMMENT '分区日期，格式YYYYMMDD')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/ods_order_info';

-- 添加分区
ALTER TABLE ods_order_info
    ADD IF NOT EXISTS PARTITION (dt = '2025-07-31');

SELECT order_id,
       user_id,
       product_id,
       order_amount,
       order_time,
       pay_time,
       order_status,
       dt
FROM ods_order_info
WHERE dt = '2025-07-31'
;
-- 3. ods_product_info: 商品信息表
-- 记录商品的基本信息，用于关联商品ID获取商品名称等。
DROP TABLE IF EXISTS ods_product_info;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_info
(
    product_id   STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category     STRING COMMENT '商品类别'
)
    COMMENT '商品维度信息表'
    PARTITIONED BY (dt STRING COMMENT '分区日期，格式YYYYMMDD')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/ods_product_info';

ALTER TABLE ods_product_info
    ADD IF NOT EXISTS PARTITION (dt = '2025-07-31');

SELECT product_id, product_name, category, dt
FROM ods_product_info
WHERE dt = '2025-07-31'
;
-- 4. ods_page_info: 页面信息表
-- 记录页面的基本信息，用于关联页面ID获取页面名称和类型。
DROP TABLE IF EXISTS ods_page_info;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_page_info
(
    page_id   STRING COMMENT '页面ID',
    page_name STRING COMMENT '页面名称',
    page_type STRING COMMENT '页面类型 (home, category, product_detail, activity, search_result)'
)
    COMMENT '页面维度信息表'
    PARTITIONED BY (dt STRING COMMENT '分区日期，格式YYYYMMDD')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/ods_page_info'
;

ALTER TABLE ods_page_info
    ADD IF NOT EXISTS PARTITION (dt = '2025-07-31');
SELECT page_id, page_name, page_type
FROM ods_page_info
where dt = '2025-07-31'
;


