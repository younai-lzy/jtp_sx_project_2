-- 设置 Hive 动态分区和非严格模式
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 优化数据混洗（Shuffle）的并行度
SET spark.sql.shuffle.partitions = 1000; -- 此参数可以在运行时设置，保持不变 缓解数据倾斜和增加并行度

-- 确保使用正确的数据库
CREATE DATABASE IF NOT EXISTS jtp_flow_topic_warehouse LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse';
USE jtp_flow_topic_warehouse;

-- ====================================================================
-- DWS 层事实表：dws_page_traffic_daily (页面流量日统计表)
-- 聚合 dwd_page_view_fact，按天和页面进行统计
-- 聚合每天每个页面的访问量、访客数、点击量等。
-- --------------------------------------------------------------------

DROP TABLE IF EXISTS dws_page_traffic_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_page_traffic_daily
(
    dt                  STRING COMMENT '统计日期',
    page_id             STRING COMMENT '页面ID',
    page_name           STRING COMMENT '页面名称',
    page_type           STRING COMMENT '页面类型',
    total_page_views    BIGINT COMMENT '总页面浏览量',
    unique_visitors     BIGINT COMMENT '独立访客数 (UV)',
    total_clicks        BIGINT COMMENT '总点击次数',
    unique_clickers     BIGINT COMMENT '独立点击用户数',
    add_to_cart_count   BIGINT COMMENT '加入购物车次数',
    purchase_count      BIGINT COMMENT '购买次数'
) COMMENT '页面流量日统计表'
    PARTITIONED BY (dt_partition STRING COMMENT '分区日期，格式YYYYMMDD')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/dws_page_traffic_daily';

-- 插入数据到 dws_page_traffic_daily
INSERT OVERWRITE TABLE dws_page_traffic_daily PARTITION (dt_partition)
SELECT
    dt,
    page_id,
    page_name,
    page_type,
    COUNT(*) AS total_page_views, -- 总页面浏览量
    COUNT(DISTINCT user_id) AS unique_visitors, -- 独立访客数 (UV)
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS total_clicks, -- 总点击次数
    COUNT(DISTINCT CASE WHEN event_type = 'click' THEN user_id ELSE NULL END) AS unique_clickers, -- 独立点击用户数
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count, -- 加入购物车次数
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count, -- 购买次数
    dt AS dt_partition
FROM
    jtp_flow_topic_warehouse.dwd_page_view_fact
WHERE
        dt = '2025-07-31' -- 假设只处理这一天的数据
GROUP BY
    dt, page_id, page_name, page_type;

select *
from dws_page_traffic_daily
;
-- ====================================================================
-- DWS 层事实表：dws_user_behavior_daily (用户行为日统计表)
-- 聚合 dwd_page_view_fact 和 dwd_order_fact，按天和用户进行统计
-- 聚合每天的用户活跃、购买等行为指标。
-- --------------------------------------------------------------------

DROP TABLE IF EXISTS dws_user_behavior_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_user_behavior_daily
(
    dt                  STRING COMMENT '统计日期',
    user_id             STRING COMMENT '用户ID',
    user_country        STRING COMMENT '用户国家',
    user_province       STRING COMMENT '用户省份',
    user_city           STRING COMMENT '用户城市',
    total_page_views    BIGINT COMMENT '总页面浏览量',
    total_clicks        BIGINT COMMENT '总点击次数',
    total_add_to_cart   BIGINT COMMENT '总加入购物车次数',
    total_purchases     BIGINT COMMENT '总购买次数',
    total_order_amount  DOUBLE COMMENT '总订单金额',
    total_paid_amount   DOUBLE COMMENT '总支付金额'
) COMMENT '用户行为日统计表'
    PARTITIONED BY (dt_partition STRING COMMENT '分区日期，格式YYYYMMDD')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/dws_user_behavior_daily';

-- 插入数据到 dws_user_behavior_daily
-- 从 dwd_page_view_fact 聚合页面行为，从 dwd_order_fact 聚合订单行为，然后进行 JOIN
INSERT OVERWRITE TABLE dws_user_behavior_daily PARTITION (dt_partition)
SELECT
    t1.dt,
    t1.user_id,
    t1.user_country,
    t1.user_province,
    t1.user_city,
    t1.total_page_views,
    t1.total_clicks,
    t1.total_add_to_cart,
    t1.total_purchases,
    COALESCE(t2.total_order_amount, 0.0) AS total_order_amount, -- 将NULL改为0.0
    COALESCE(t2.total_paid_amount, 0.0) AS total_paid_amount,   -- 将NULL改为0.0
    t1.dt AS dt_partition
FROM
    ( -- 页面行为聚合
        SELECT
            dt,
            user_id,
            user_country,
            user_province,
            user_city,
            COUNT(*) AS total_page_views,
            SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS total_clicks,
            SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS total_add_to_cart,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases
        FROM
            jtp_flow_topic_warehouse.dwd_page_view_fact
        WHERE
                dt = '2025-07-31'
        GROUP BY
            dt, user_id, user_country, user_province, user_city
    ) t1
        LEFT JOIN
    ( -- 订单行为聚合
        SELECT
            dt,
            user_id,
            SUM(order_amount) AS total_order_amount,
            SUM(CASE WHEN order_status = 'paid' THEN order_amount ELSE 0 END) AS total_paid_amount
        FROM
            jtp_flow_topic_warehouse.dwd_order_fact
        WHERE
                dt = '2025-07-31'
        GROUP BY
            dt, user_id
    ) t2 ON t1.user_id = t2.user_id AND t1.dt = t2.dt;

select *
from dws_user_behavior_daily
limit 10
;