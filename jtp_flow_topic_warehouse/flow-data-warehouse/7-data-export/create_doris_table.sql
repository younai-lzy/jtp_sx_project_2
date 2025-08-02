CREATE DATABASE IF NOT EXISTS jtp_flow_topic_warehouse;
USE jtp_flow_topic_warehouse;

-- 1. Doris 表: ads_page_analysis_daily
-- 对应 Hive 的 ads_page_analysis_daily
DROP TABLE IF EXISTS ads_page_analysis_daily;
CREATE TABLE ads_page_analysis_daily
(
    dt                          DATE COMMENT '统计日期',
    page_id                     VARCHAR(64) COMMENT '页面ID',
    page_name                   VARCHAR(256) COMMENT '页面名称',
    page_type                   VARCHAR(64) COMMENT '页面类型',
    total_page_views            BIGINT COMMENT '总页面浏览量',
    unique_visitors             BIGINT COMMENT '独立访客数 (UV)',
    total_clicks                BIGINT COMMENT '总点击次数',
    unique_clickers             BIGINT COMMENT '独立点击用户数',
    add_to_cart_count           BIGINT COMMENT '加入购物车次数',
    purchase_count              BIGINT COMMENT '购买次数',
    conversion_rate_click       DECIMAL(5, 4) COMMENT '点击率 (点击次数/浏览量)',
    conversion_rate_add_to_cart DECIMAL(5, 4) COMMENT '加购率 (加购次数/浏览量)',
    conversion_rate_purchase    DECIMAL(5, 4) COMMENT '购买率 (购买次数/浏览量)'
) ENGINE = OLAP -- 明确指定引擎
    COMMENT '页面分析日表' -- COMMENT 放在这里
-- 移除 DUPLICATE KEY(dt, page_id)
    PARTITION BY RANGE (dt) (
        -- 这里需要根据实际日期范围定义分区，例如按月或按天
        PARTITION p20250731 VALUES [('2025-07-31'), ('2025-08-01')) )
    DISTRIBUTED BY HASH(page_id) BUCKETS 10
    PROPERTIES (
        "replication_num" = "1", -- 副本数，生产环境建议3
        "storage_type" = "COLUMN" -- 明确存储类型
    );

-- 2. Doris 表: ads_page_guidance_daily
-- 对应 Hive 的 ads_page_guidance_daily
DROP TABLE IF EXISTS ads_page_guidance_daily;
CREATE TABLE ads_page_guidance_daily
(
    dt                        DATE COMMENT '统计日期',
    page_id                   VARCHAR(64) COMMENT '页面ID',
    page_name                 VARCHAR(256) COMMENT '页面名称',
    page_type                 VARCHAR(64) COMMENT '页面类型',
    product_id                VARCHAR(64) COMMENT '被引导的商品ID',
    product_name              VARCHAR(256) COMMENT '被引导的商品名称',
    product_category          VARCHAR(64) COMMENT '被引导的商品类别',
    guided_page_views         BIGINT COMMENT '该页面引导到该商品的浏览量',
    guided_product_clicks     BIGINT COMMENT '该页面引导到该商品的商品点击量',
    guided_add_to_cart_events BIGINT COMMENT '该页面引导到该商品的加购事件数',
    guided_purchase_events    BIGINT COMMENT '该页面引导到该商品的购买事件数',
    guidance_to_purchase_rate DECIMAL(5, 4) COMMENT '引导购买率 (购买事件数/引导浏览量)'
) ENGINE = OLAP -- 明确指定引擎
    COMMENT '页面引导日表' -- COMMENT 放在这里
-- 移除 DUPLICATE KEY(dt, page_id, product_id)
    PARTITION BY RANGE (dt) (
        PARTITION p20250731 VALUES [('2025-07-31'), ('2025-08-01')) )
    DISTRIBUTED BY HASH(page_id, product_id) BUCKETS 10
    PROPERTIES (
        "replication_num" = "1", -- 副本数，生产环境建议3
        "storage_type" = "COLUMN" -- 明确存储类型
    );
