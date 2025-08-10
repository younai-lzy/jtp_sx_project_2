DROP
CATALOG IF EXISTS hive_catalog;
CREATE
CATALOG IF NOT EXISTS hive_catalog PROPERTIES (
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
INSERT INTO jtp_gd03_warehouse.ads_search_keyword_metrics
    PARTITION (dt='2025-08-07') -- 请替换为实际分区日期
SELECT
    -- 分区字段
    T1.dts AS dt,
    -- 维度字段
    T1.sku_id,
    T1.search_keyword,
    -- 指标计算
    COUNT(DISTINCT T1.user_id) AS traffic_uv,  -- 搜索引流人数（搜索UV）
    COUNT(DISTINCT T3.user_id) AS view_uv,     -- 浏览访客数（浏览UV）
    COUNT(DISTINCT T2.user_id) AS buyer_uv,    -- 支付买家数
    COUNT(DISTINCT T4.user_id) AS add_cart_uv, -- 加购人数
    -- 转化率计算
    IFNULL(CAST(COUNT(DISTINCT T2.user_id) AS DOUBLE) / COUNT(DISTINCT T1.user_id), 0) AS conversion_rate_pay,
    IFNULL(CAST(COUNT(DISTINCT T4.user_id) AS DOUBLE) / COUNT(DISTINCT T1.user_id), 0) AS conversion_rate_cart
FROM
    hive_catalog.jtp_commodity_warehouse.dwd_user_search_action_detail AS T1 -- 搜索行为明细
        LEFT JOIN
    hive_catalog.jtp_commodity_warehouse.dwd_order_detail AS T2 -- 订单明细
    ON T1.user_id = T2.user_id AND T1.sku_id = T2.sku_id AND T1.dts = T2.dts
        LEFT JOIN
    hive_catalog.jtp_commodity_warehouse.dwd_user_action_detail AS T3 -- 用户行为明细（获取浏览行为）
    ON T1.user_id = T3.user_id AND T1.sku_id = T3.sku_id AND T1.dts = T3.dts AND T3.action_type = 'view'
        LEFT JOIN
    hive_catalog.jtp_commodity_warehouse.dwd_user_action_detail AS T4 -- 用户行为明细（获取加购行为）
    ON T1.user_id = T4.user_id AND T1.sku_id = T4.sku_id AND T1.dts = T4.dts AND T4.action_type = 'add_cart'
WHERE T1.dts = '2025-08-07' -- 请替换为实际分区日期
GROUP BY T1.dts, T1.sku_id, T1.search_keyword;


-- 插入数据到 ads_keyword_root_metrics 表
-- 计算词根拆分：引流人数和转化率
INSERT INTO ads_keyword_root_metrics
    PARTITION (dt='2025-08-10') -- 请替换为实际分区日期
SELECT
    -- 分区字段
    T1.dts AS dt,
    -- 维度字段
    T1.sku_id,
    T2.keyword_root, -- 假设这是一个分词后的词根
    -- 指标计算
    COUNT(DISTINCT T1.user_id) AS traffic_uv, -- 引流人数
    IFNULL(CAST(COUNT(DISTINCT T3.user_id) AS DOUBLE) / COUNT(DISTINCT T1.user_id), 0) AS conversion_rate
FROM
    dwd_user_search_action_detail AS T1 -- 搜索行为明细
        INNER JOIN
    -- 假设的中间表，包含了关键词到词根的映射
        (
            -- 这是一个模拟分词的子查询，在实际中需要专门的ETL过程
            SELECT
                '手机' as keyword_root,
                '小米手机' as search_keyword,
                '2025-08-10' as dts
            UNION ALL
            SELECT
                '手机' as keyword_root,
                '华为手机' as search_keyword,
                '2025-08-10' as dts
        ) AS T2
    ON
                T1.search_keyword = T2.search_keyword AND T1.dts = T2.dts
        LEFT JOIN
    dwd_order_detail AS T3 -- 订单明细
    ON T1.user_id = T3.user_id AND T1.sku_id = T3.sku_id AND T1.dts = T3.dts
WHERE T1.dts = '2025-08-10' -- 请替换为实际分区日期
GROUP BY T1.dts, T1.sku_id, T2.keyword_root;
