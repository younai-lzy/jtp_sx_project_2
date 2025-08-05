-- 设置 Hive 动态分区和非严格模式
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 优化数据混洗（Shuffle）的并行度
SET spark.sql.shuffle.partitions = 1000;
-- 此参数可以在运行时设置，保持不变 缓解数据倾斜和增加并行度

-- ADS层表结构
--
-- ads_page_traffic_analysis_daily: 每日页面流量分析汇总表
-- 聚合每日页面流量数据，提供分析看板所需的核心指标。

CREATE DATABASE IF NOT EXISTS jtp_flow_topic_warehouse
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse';
USE jtp_flow_topic_warehouse;

-- ====================================================================
-- ADS 层：ads_page_analysis_daily (页面分析日表)
-- 用于页面概览、数据趋势、点击偏好等看板展示
-- --------------------------------------------------------------------
-- ADS 层：ads_page_analysis_daily (页面分析日表)
-- 用于页面概览、数据趋势、点击偏好等看板展示、引导下单/支付指标
-- --------------------------------------------------------------------

DROP TABLE IF EXISTS ads_page_analysis_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS ads_page_analysis_daily
(
    dt                          STRING COMMENT '统计日期',
    page_id                     STRING COMMENT '页面ID',
    page_name                   STRING COMMENT '页面名称',
    page_type                   STRING COMMENT '页面类型',
    total_page_views            BIGINT COMMENT '总页面浏览量',
    unique_visitors             BIGINT COMMENT '独立访客数 (UV)',
    total_clicks                BIGINT COMMENT '总点击次数',
    unique_clickers             BIGINT COMMENT '独立点击用户数',
    add_to_cart_count           BIGINT COMMENT '加入购物车次数',
    purchase_count              BIGINT COMMENT '购买次数',
    conversion_rate_click       DECIMAL(5, 4) COMMENT '点击率 (点击次数/浏览量)',
    conversion_rate_add_to_cart DECIMAL(5, 4) COMMENT '加购率 (加购次数/浏览量)',
    conversion_rate_purchase    DECIMAL(5, 4) COMMENT '购买率 (购买次数/浏览量)',
    -- 新增指标
    guided_order_buyers_count   BIGINT COMMENT '引导下单买家数',
    guided_paid_amount          DOUBLE COMMENT '引导支付金额',
    guided_paid_buyers_count    BIGINT COMMENT '引导支付买家数'
) COMMENT '页面分析日表'
    PARTITIONED BY (dt_partition STRING COMMENT '分区日期，格式YYYYMMDD')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/ads_page_analysis_daily';

-- 插入数据到 ads_page_analysis_daily
INSERT OVERWRITE TABLE ads_page_analysis_daily PARTITION (dt_partition)
SELECT t1.dt,
       t1.page_id,
       t1.page_name,
       t1.page_type,
       t1.total_page_views,
       t1.unique_visitors,
       t1.total_clicks,
       t1.unique_clickers,
       t1.add_to_cart_count,
       t1.purchase_count,
-- -- 计算点击率、加购率、购买率，避免除以0
       CAST(total_clicks * 1.0 /
            CASE WHEN total_page_views = 0 THEN 1 ELSE total_page_views END AS DECIMAL(5, 4)) AS conversion_rate_click,
       CAST(add_to_cart_count * 1.0 / CASE
                                          WHEN total_page_views = 0 THEN 1
                                          ELSE total_page_views END AS DECIMAL(5, 4))         AS conversion_rate_add_to_cart,
       CAST(purchase_count * 1.0 / CASE
                                       WHEN total_page_views = 0 THEN 1
                                       ELSE total_page_views END AS DECIMAL(5, 4))            AS conversion_rate_purchase,
       -- 关联 dwd_order_fact 或 dws_user_behavior_daily 来获取引导下单/支付指标
       -- 这里的逻辑需要从 dwd_page_view_fact 和 dwd_order_fact 关联计算
       COALESCE(t2.guided_order_buyers_count, 0) AS guided_order_buyers_count,
       COALESCE(t2.guided_paid_amount, 0.0)      AS guided_paid_amount,
       COALESCE(t2.guided_paid_buyers_count, 0)  AS guided_paid_buyers_count,
       t1.dt                                     AS dt_partition
FROM jtp_flow_topic_warehouse.dws_page_traffic_daily t1
         LEFT JOIN (
    -- 计算每个页面引导的下单和支付指标
    SELECT dpv.dt,
           dpv.page_id,
           COUNT(DISTINCT dof.user_id)                                                        AS guided_order_buyers_count, -- 引导下单买家数
           SUM(CASE WHEN dof.order_status = 'paid' THEN dof.order_amount ELSE 0 END)          AS guided_paid_amount,        -- 引导支付金额
           COUNT(DISTINCT CASE
                              WHEN dof.order_status = 'paid' THEN dof.user_id
                              ELSE NULL END)                                                  AS guided_paid_buyers_count   -- 引导支付买家数
    FROM jtp_flow_topic_warehouse.dwd_page_view_fact dpv
             JOIN
         jtp_flow_topic_warehouse.dwd_order_fact dof
         ON
                     dpv.user_id = dof.user_id
                 AND dpv.dt = dof.dt
                 AND dpv.product_id = dof.product_id -- 假设页面引导的购买事件与商品相关
                 AND dpv.event_type = 'purchase' -- 仅考虑购买事件作为引导的最终结果
    WHERE dpv.dt = '2025-07-31'
    GROUP BY dpv.dt, dpv.page_id) t2 ON t1.dt = t2.dt AND t1.page_id = t2.page_id
WHERE t1.dt_partition = '2025-07-31';

select *
from ads_page_analysis_daily;

-- ====================================================================
-- ADS 层：ads_page_guidance_daily (页面引导日表)
-- 用于分析页面引导到商品的具体效果 (符合装修诊断中的引导详情)
-- --------------------------------------------------------------------

DROP TABLE IF EXISTS ads_page_guidance_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS ads_page_guidance_daily
(
    dt                        STRING COMMENT '统计日期',
    page_id                   STRING COMMENT '页面ID',
    page_name                 STRING COMMENT '页面名称',
    page_type                 STRING COMMENT '页面类型',
    product_id                STRING COMMENT '被引导的商品ID',
    product_name              STRING COMMENT '被引导的商品名称',
    product_category          STRING COMMENT '被引导的商品类别',
    guided_page_views         BIGINT COMMENT '该页面引导到该商品的浏览量',     -- 商品访客数
    guided_product_clicks     BIGINT COMMENT '该页面引导到该商品的商品点击量',
    guided_add_to_cart_events BIGINT COMMENT '该页面引导到该商品的加购事件数',
    guided_purchase_events    BIGINT COMMENT '该页面引导到该商品的购买事件数', -- 下单件数
    guidance_to_purchase_rate DECIMAL(5, 4) COMMENT '引导购买率 (购买事件数/引导浏览量)'
) COMMENT '页面引导日表'
    PARTITIONED BY (dt_partition STRING COMMENT '分区日期，格式YYYYMMDD')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/ads_page_guidance_daily';

-- 插入数据到 ads_page_guidance_daily
-- 聚合 dwd_page_view_fact，按页面和商品进行统计
INSERT OVERWRITE TABLE ads_page_guidance_daily PARTITION (dt_partition)
SELECT dt,
       page_id,
       page_name,
       page_type,
       product_id,
       product_name,
       product_category,
       COUNT(*)                                                                         AS guided_page_views,         -- 页面引导到该商品的浏览量 (即该页面上包含该商品ID的事件数)
       SUM(CASE WHEN event_type = 'click' AND product_id IS NOT NULL THEN 1 ELSE 0 END) AS guided_product_clicks,     -- 页面引导到该商品的商品点击量
       SUM(CASE
               WHEN event_type = 'add_to_cart' AND product_id IS NOT NULL THEN 1
               ELSE 0 END)                                                              AS guided_add_to_cart_events, -- 页面引导到该商品的加购事件数
       SUM(CASE
               WHEN event_type = 'purchase' AND product_id IS NOT NULL THEN 1
               ELSE 0 END)                                                              AS guided_purchase_events,    -- 页面引导到该商品的购买事件数
-- 计算引导购买率，避免除以0
       CAST(SUM(CASE WHEN event_type = 'purchase' AND product_id IS NOT NULL THEN 1 ELSE 0 END) * 1.0 /
            CASE
                WHEN COUNT(*) = 0 THEN 1
                ELSE COUNT(*) END AS DECIMAL(5, 4))                                     AS guidance_to_purchase_rate,
       dt                                                                               AS dt_partition
FROM jtp_flow_topic_warehouse.dwd_page_view_fact
WHERE dt = '2025-07-31'
  AND page_id IS NOT NULL    -- 确保页面ID存在
  AND product_id IS NOT NULL -- 确保与商品相关
GROUP BY dt, page_id, page_name, page_type, product_id, product_name, product_category
;
select *
from ads_page_guidance_daily;

-- ====================================================================
-- ADS 层：ads_page_trend_daily (页面趋势日表)
-- 用于展示近30天页面的访客数、点击人数等指标的变化趋势
-- --------------------------------------------------------------------

DROP TABLE IF EXISTS ads_page_trend_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS ads_page_trend_daily
(
    dt               STRING COMMENT '统计日期',
    page_id          STRING COMMENT '页面ID',
    page_name        STRING COMMENT '页面名称',
    page_type        STRING COMMENT '页面类型',
    total_page_views BIGINT COMMENT '总页面浏览量',
    unique_visitors  BIGINT COMMENT '独立访客数 (UV)',
    total_clicks     BIGINT COMMENT '总点击次数',
    unique_clickers  BIGINT COMMENT '独立点击用户数'
) COMMENT '页面趋势日表'
    PARTITIONED BY (dt_partition STRING COMMENT '分区日期，格式YYYYMMDD')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/ads_page_trend_daily';

-- 插入数据到 ads_page_trend_daily
-- 本查询将获取 dws_page_traffic_daily 中最近 30 天的数据，并覆盖到 ads_page_trend_daily
-- 在实际调度中，'2025-07-31' 应替换为动态日期变量，例如 ${hivevar:biz_date}
-- 这样每次运行都会更新 ads_page_trend_daily 为最新的 30 天数据
INSERT OVERWRITE TABLE ads_page_trend_daily PARTITION (dt_partition)
SELECT t.dt,
       t.page_id,
       t.page_name,
       t.page_type,
       t.total_page_views,
       t.unique_visitors,
       t.total_clicks,
       t.unique_clickers,
       t.dt AS dt_partition
FROM jtp_flow_topic_warehouse.dws_page_traffic_daily t
WHERE
  -- 假设当前日期为 '2025-07-31'，则获取 '2025-07-02' 到 '2025-07-31' 的数据
  -- 在实际调度中，'2025-07-31' 应替换为动态日期变量
    t.dt >= DATE_SUB('2025-07-31', 29)
  AND t.dt <= '2025-07-31';

select *
from ads_page_trend_daily;

