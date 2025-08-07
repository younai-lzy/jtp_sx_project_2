-- 设置 Hive 动态分区和非严格模式
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 优化数据混洗（Shuffle）的并行度
SET spark.sql.shuffle.partitions = 400; -- 此参数可以在运行时设置，保持不变

-- 确保使用正确的数据库
CREATE DATABASE IF NOT EXISTS jtp_flow_topic_warehouse LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse';
USE jtp_flow_topic_warehouse;

-- ====================================================================
-- DWD 层事实表：dwd_page_view_fact (页面访问事实表)
-- 结合 ods_user_action_log, dim_page, dim_user, dim_product
-- --------------------------------------------------------------------

DROP TABLE IF EXISTS dwd_page_view_fact;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_page_view_fact
(
    log_id         STRING COMMENT '日志唯一ID',
    user_id        STRING COMMENT '用户ID',
    session_id     STRING COMMENT '会话ID',
    -- 页面维度信息
    page_id        STRING COMMENT '页面ID',
    page_name      STRING COMMENT '页面名称',
    page_type      STRING COMMENT '页面类型',
    -- 用户维度信息
    user_country   STRING COMMENT '用户国家',
    user_province  STRING COMMENT '用户省份',
    user_city      STRING COMMENT '用户城市',
    -- 事件信息
    element_id     STRING COMMENT '点击元素ID (如果事件类型为点击)',
    event_type     STRING COMMENT '事件类型 (page_view, click, add_to_cart, purchase)',
    event_time     TIMESTAMP COMMENT '事件发生时间',
    -- 商品信息 (如果事件与商品相关)
    product_id     STRING COMMENT '商品ID',
    product_name   STRING COMMENT '商品名称',
    product_category STRING COMMENT '商品类别',
    -- 设备信息
    ip_address     STRING COMMENT '用户IP地址',
    device_type    STRING COMMENT '设备类型 (PC, Mobile)'
) COMMENT '页面访问事实表'
    PARTITIONED BY (dt STRING COMMENT '分区日期，格式YYYYMMDD')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/dwd_page_view_fact';

-- 插入数据到 dwd_page_view_fact
-- 关联 ods_user_action_log, dim_page, dim_user, dim_product
-- 注意：这里假设 ods_product_info 和 ods_page_info 已经有 dt='2025-07-31' 的数据
INSERT OVERWRITE TABLE dwd_page_view_fact PARTITION (dt)
SELECT
    log.log_id,
    log.user_id,
    log.session_id,
    log.page_id,
    dp.page_name,
    dp.page_type,
    du.country AS user_country,
    du.province AS user_province,
    du.city AS user_city,
    log.element_id,
    log.event_type,
    log.event_time,
    log.product_id,
    dpr.product_name,
    dpr.category AS product_category,
    log.ip_address,
    log.device_type,
    log.dt
FROM
    jtp_flow_topic_warehouse.ods_user_action_log log
        LEFT JOIN
    jtp_flow_topic_warehouse.dim_page dp ON log.page_id = dp.page_id AND log.dt = dp.dt
        LEFT JOIN
    jtp_flow_topic_warehouse.dim_user du ON log.user_id = du.user_id AND log.dt = du.dt
        LEFT JOIN
    jtp_flow_topic_warehouse.dim_product dpr ON log.product_id = dpr.product_id AND log.dt = dpr.dt
WHERE
        log.dt = '2025-07-31';


-- ====================================================================
-- DWD 层事实表：dwd_order_fact (订单事实表)
-- 结合 ods_order_info, dim_product, dim_user
-- --------------------------------------------------------------------

DROP TABLE IF EXISTS dwd_order_fact;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_order_fact
(
    order_id     STRING COMMENT '订单ID',
    user_id      STRING COMMENT '用户ID',
    user_country   STRING COMMENT '用户国家',
    user_province  STRING COMMENT '用户省份',
    user_city      STRING COMMENT '用户城市',
    product_id   STRING COMMENT '商品ID',
    product_name   STRING COMMENT '商品名称',
    product_category STRING COMMENT '商品类别',
    order_amount DOUBLE COMMENT '订单金额',
    order_time   TIMESTAMP COMMENT '下单时间',
    pay_time     TIMESTAMP COMMENT '支付时间',
    order_status STRING COMMENT '订单状态 (paid, unpaid, cancelled)'
) COMMENT '订单事实表'
    PARTITIONED BY (dt STRING COMMENT '分区日期，格式YYYYMMDD')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/dwd_order_fact';

-- 插入数据到 dwd_order_fact
-- 关联 ods_order_info, dim_product, dim_user
INSERT OVERWRITE TABLE dwd_order_fact PARTITION (dt)
SELECT
    oi.order_id,
    oi.user_id,
    du.country AS user_country,
    du.province AS user_province,
    du.city AS user_city,
    oi.product_id,
    dpr.product_name,
    dpr.category AS product_category,
    oi.order_amount,
    oi.order_time,
    oi.pay_time,
    oi.order_status,
    oi.dt
FROM
    jtp_flow_topic_warehouse.ods_order_info oi
        LEFT JOIN
    jtp_flow_topic_warehouse.dim_user du ON oi.user_id = du.user_id AND oi.dt = du.dt
        LEFT JOIN
    jtp_flow_topic_warehouse.dim_product dpr ON oi.product_id = dpr.product_id AND oi.dt = dpr.dt
WHERE
        oi.dt = '2025-07-31';

select *
from dwd_order_fact limit 10;