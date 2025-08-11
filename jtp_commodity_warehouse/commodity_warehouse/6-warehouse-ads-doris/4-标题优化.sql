DROP CATALOG IF EXISTS hive_catalog;
CREATE CATALOG IF NOT EXISTS hive_catalog PROPERTIES (
  'type'='hms',
  'hive.metastore.type' = 'hms',
  'hive.version' = '3.1.2',
  'fs.defaultFS' = 'hdfs://node101:8020',
  'hive.metastore.uris' = 'thrift://node101:9083'
);

DROP DATABASE IF EXISTS jtp_gd03_warehouse;
CREATE DATABASE IF NOT EXISTS jtp_gd03_warehouse;
USE jtp_gd03_warehouse;
-- ADS层搜索词指标表
-- 存储每日每个SKU-搜索词的引流、转化指标
-- ADS层搜索词指标表
-- 存储每日每个SKU-搜索词的引流、转化指标
DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_search_keyword_metrics;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.ads_search_keyword_metrics
(
    `dt`                   DATE NOT NULL COMMENT '数据日期',
    `sku_id`               INT  NOT NULL COMMENT '商品SKU ID',
    `search_keyword` VARCHAR(255) NOT NULL COMMENT '用户搜索关键词',
    `traffic_uv`           BIGINT SUM COMMENT '引流人数（搜索UV）',
    `view_uv`              BIGINT SUM COMMENT '访客数（浏览UV）',
    `buyer_uv`             BIGINT SUM COMMENT '支付买家数',
    `add_cart_uv`          BIGINT SUM COMMENT '加购人数',
    `conversion_rate_pay`  DOUBLE REPLACE COMMENT '支付转化率',
    `conversion_rate_cart` DOUBLE REPLACE COMMENT '加购转化率'
) AGGREGATE KEY(dt, sku_id, search_keyword)
PARTITION BY RANGE(`dt`) (
    PARTITION p20250810 VALUES LESS THAN ('2025-08-11'),
    PARTITION p20250811 VALUES LESS THAN ('2025-08-12')
)
DISTRIBUTED BY HASH(sku_id, search_keyword)
PROPERTIES (
    "replication_num" = "1"
);

-- ADS层词根拆分指标表
-- 存储每日每个SKU-词根的引流和转化指标
DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_keyword_root_metrics;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.ads_keyword_root_metrics
(
    `dt`              DATE NOT NULL COMMENT '数据日期',
    `sku_id`          INT  NOT NULL COMMENT '商品SKU ID',
    `keyword_root` VARCHAR(255) NOT NULL COMMENT '搜索词根',
    `traffic_uv`      BIGINT SUM COMMENT '引流人数（包含该词根的搜索UV）',
    `conversion_rate` DOUBLE REPLACE COMMENT '转化率'
) AGGREGATE KEY(dt, sku_id, keyword_root)
PARTITION BY RANGE(`dt`) (
    PARTITION p20250810 VALUES LESS THAN ('2025-08-11'),
    PARTITION p20250811 VALUES LESS THAN ('2025-08-12')
)
DISTRIBUTED BY HASH(sku_id)
PROPERTIES (
    "replication_num" = "1"
);

-- 插入数据到 ads_search_keyword_metrics 表
-- 计算搜索词引流效果：访客数、支付买家数、支付转化率
-- 注意：由于缺少 dwd_user_search_action_detail 表，这里无法准确计算基于搜索关键词的引流。
-- 此处的 SQL 仅为示例，在没有搜索行为明细表的情况下无法实际运行。
INSERT INTO ads_search_keyword_metrics
-- 移除错误的 PARTITION 语法
SELECT
    -- 分区字段
    T1.dts AS dt,
    -- 维度字段
    T1.sku_id,
    -- 由于没有搜索关键词表，此处无法获取关键词，使用 NULL 作为占位符。
    CAST(NULL AS VARCHAR(255)) AS search_keyword,
    -- 指标计算
    COUNT(DISTINCT T1.user_id) AS traffic_uv,  -- 搜索引流人数（搜索UV）
    COUNT(DISTINCT T1.user_id) AS view_uv,     -- 浏览访客数（浏览UV）
    COUNT(DISTINCT T2.user_id) AS buyer_uv,    -- 支付买家数
    COUNT(DISTINCT T3.user_id) AS add_cart_uv, -- 加购人数
    -- 转化率计算
    IFNULL(CAST(COUNT(DISTINCT T2.user_id) AS DOUBLE) / COUNT(DISTINCT T1.user_id), 0) AS conversion_rate_pay,
    IFNULL(CAST(COUNT(DISTINCT T3.user_id) AS DOUBLE) / COUNT(DISTINCT T1.user_id), 0) AS conversion_rate_cart
FROM
    hive_catalog.jtp_commodity_warehouse.dwd_user_action_detail AS T1 -- 用户行为明细
        LEFT JOIN
    hive_catalog.jtp_commodity_warehouse.dwd_order_detail AS T2 -- 订单明细
    ON T1.user_id = T2.user_id AND T1.sku_id = T2.sku_id AND T1.dts = T2.dts
        LEFT JOIN
    (SELECT DISTINCT user_id, sku_id, dts FROM hive_catalog.jtp_commodity_warehouse.dwd_user_action_detail WHERE action_type = 'add_cart') AS T3
    ON T1.user_id = T3.user_id AND T1.sku_id = T3.sku_id AND T1.dts = T3.dts
WHERE
        T1.dts = '2025-08-07' -- 请替换为实际分区日期
  AND T1.action_type = 'view'
  AND T2.dts = '2025-08-07'
GROUP BY T1.dts, T1.sku_id;


-- 插入数据到 ads_keyword_root_metrics 表
-- 计算词根拆分：引流人数和转化率
-- 注意：由于缺少搜索关键词和分词表，此处的 SQL 无法实际运行。
INSERT INTO ads_keyword_root_metrics
-- 移除错误的 PARTITION 语法
SELECT
    -- 分区字段
    T1.dts AS dt,
    -- 维度字段
    T1.sku_id,
    -- 由于没有搜索关键词表，此处无法获取词根，使用 NULL 作为占位符。
    CAST(NULL AS VARCHAR(255)) AS keyword_root,
    -- 指标计算
    COUNT(DISTINCT T1.user_id) AS traffic_uv, -- 引流人数
    IFNULL(CAST(COUNT(DISTINCT T2.user_id) AS DOUBLE) / COUNT(DISTINCT T1.user_id), 0) AS conversion_rate
FROM
    hive_catalog.jtp_commodity_warehouse.dwd_user_action_detail AS T1 -- 用户行为明细
        LEFT JOIN
    hive_catalog.jtp_commodity_warehouse.dwd_order_detail AS T2 -- 订单明细
    ON T1.user_id = T2.user_id AND T1.sku_id = T2.sku_id AND T1.dts = T2.dts
WHERE
        T1.dts = '2025-08-07' -- 请替换为实际分区日期
  AND T1.action_type = 'view'
GROUP BY T1.dts, T1.sku_id;
