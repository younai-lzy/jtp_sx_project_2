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
-- ADS层搜索词指标表
-- 存储每日每个SKU-商品名称的引流、转化指标
DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_search_keyword_metrics;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.ads_search_keyword_metrics
(
    `dt`                   DATE         NOT NULL COMMENT '数据日期',
    `sku_id`               INT          NOT NULL COMMENT '商品SKU ID',
    `search_keyword`       VARCHAR(255) NOT NULL COMMENT '用户搜索关键词（此处用商品名称代替）',
    `traffic_uv`           BIGINT SUM COMMENT '引流人数（搜索UV）',
    `view_uv`              BIGINT SUM COMMENT '访客数（浏览UV）',
    `buyer_uv`             BIGINT SUM COMMENT '支付买家数',
    `add_cart_uv`          BIGINT SUM COMMENT '加购人数',
    `conversion_rate_pay`  DOUBLE REPLACE COMMENT '支付转化率',
    `conversion_rate_cart` DOUBLE REPLACE COMMENT '加购转化率'
) AGGREGATE KEY(dt, sku_id, search_keyword)
PARTITION BY RANGE(`dt`) (
    PARTITION p20250807 VALUES LESS THAN ('2025-08-08')
)
DISTRIBUTED BY HASH(sku_id, search_keyword)
PROPERTIES (
    "replication_num" = "1"
);

-- ADS层词根拆分指标表
-- 存储每日每个SKU-商品名称的引流和转化指标
DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_keyword_root_metrics;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.ads_keyword_root_metrics
(
    `dt`              DATE         NOT NULL COMMENT '数据日期',
    `sku_id`          INT          NOT NULL COMMENT '商品SKU ID',
    `keyword_root`    VARCHAR(255) NOT NULL COMMENT '搜索词根（此处用商品名称代替）',
    `traffic_uv`      BIGINT SUM COMMENT '引流人数（包含该词根的搜索UV）',
    `conversion_rate` DOUBLE REPLACE COMMENT '转化率'
) AGGREGATE KEY(dt, sku_id, keyword_root)
PARTITION BY RANGE(`dt`) (
    PARTITION p20250807 VALUES LESS THAN ('2025-08-08')
)
DISTRIBUTED BY HASH(sku_id)
PROPERTIES (
    "replication_num" = "1"
);


-- 插入数据到 ads_search_keyword_metrics 表
INSERT INTO ads_search_keyword_metrics PARTITION(p20250807)
SELECT
    -- 分区字段
    T1.dts AS dt,
    -- 维度字段
    T1.sku_id,
    T1.product_name AS search_keyword, -- 使用商品名称代替搜索关键词
    -- 指标计算（以'view'行为为基准）
    COUNT(DISTINCT T1.user_id) AS traffic_uv,  -- 引流人数（访客数）
    COUNT(DISTINCT T1.user_id) AS view_uv,     -- 浏览访客数
    COUNT(DISTINCT T2.user_id_o) AS buyer_uv,  -- 支付买家数
    COUNT(DISTINCT T3.user_id) AS add_cart_uv, -- 加购人数
    -- 转化率计算
    IFNULL(CAST(COUNT(DISTINCT T2.user_id_o) AS DOUBLE) / COUNT(DISTINCT T1.user_id), 0) AS conversion_rate_pay,
    IFNULL(CAST(COUNT(DISTINCT T3.user_id) AS DOUBLE) / COUNT(DISTINCT T1.user_id), 0) AS conversion_rate_cart
FROM
    hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide AS T1
        LEFT JOIN
    -- 从宽表中筛选出订单数据
        (SELECT DISTINCT user_id_o, sku_id_o, dts FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide WHERE action_type = 'buy') AS T2
    ON T1.user_id = T2.user_id_o AND T1.sku_id = T2.sku_id_o AND T1.dts = T2.dts
        LEFT JOIN
    -- 从宽表中筛选出加购数据
        (SELECT DISTINCT user_id, sku_id, dts FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide WHERE action_type = 'add_cart') AS T3
    ON T1.user_id = T3.user_id AND T1.sku_id = T3.sku_id AND T1.dts = T3.dts
WHERE
        T1.dts = '2025-08-07'
  AND T1.action_type = 'view'
  AND T1.sku_id IS NOT NULL AND T1.sku_id <> 0 -- 过滤掉sku_id为空或0的数据
  AND T1.product_name IS NOT NULL AND T1.product_name <> '' -- 过滤掉商品名称为空的数据
GROUP BY
    T1.dts, T1.sku_id, T1.product_name;

-- 插入数据到 ads_keyword_root_metrics 表
-- 基于dwd_aggregated_wide宽表计算引流和转化率
-- 注意：`keyword_root` 字段使用 `product_name` 代替
INSERT INTO ads_keyword_root_metrics PARTITION(p20250807)
SELECT
    -- 分区字段
    T1.dts AS dt,
    -- 维度字段
    T1.sku_id,
    T1.product_name AS keyword_root, -- 使用商品名称代替词根
    -- 指标计算（以'view'行为为基准）
    COUNT(DISTINCT T1.user_id) AS traffic_uv, -- 引流人数
    IFNULL(CAST(COUNT(DISTINCT T2.user_id_o) AS DOUBLE) / COUNT(DISTINCT T1.user_id), 0) AS conversion_rate
FROM
    hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide AS T1
        LEFT JOIN
    (SELECT DISTINCT user_id_o, sku_id_o, dts FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide WHERE action_type = 'buy') AS T2
    ON T1.user_id = T2.user_id_o AND T1.sku_id = T2.sku_id_o AND T1.dts = T2.dts
WHERE
        T1.dts = '2025-08-07'
  AND T1.action_type = 'view'
  AND T1.sku_id IS NOT NULL AND T1.sku_id <> 0 -- 过滤掉sku_id为空或0的数据
  AND T1.product_name IS NOT NULL AND T1.product_name <> '' -- 过滤掉商品名称为空的数据
GROUP BY
    T1.dts, T1.sku_id, T1.product_name;
