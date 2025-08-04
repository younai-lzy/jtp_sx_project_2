CREATE DATABASE IF NOT EXISTS jtp_flow_topic_warehouse;
USE jtp_flow_topic_warehouse;

-- 创建 Doris 数据库 (如果不存在)
-- 在 Doris 客户端 (mysql-client) 中执行:

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
    conversion_rate_purchase    DECIMAL(5, 4) COMMENT '购买率 (购买次数/浏览量)',
    guided_order_buyers_count   BIGINT COMMENT '引导下单买家数',
    guided_paid_amount          DOUBLE COMMENT '引导支付金额',
    guided_paid_buyers_count    BIGINT COMMENT '引导支付买家数'
) ENGINE = OLAP
    COMMENT '页面分析日表'
    PARTITION BY RANGE (dt) (
        -- 示例分区，请根据实际数据日期范围调整，通常按天或按月
        PARTITION p20250731 VALUES [('2025-07-31'), ('2025-08-01'))
    -- 更多分区示例:
    -- PARTITION p202508 VALUES [('2025-08-01'), ('2025-09-01')),
    -- PARTITION p202509 VALUES [('2025-09-01'), ('2025-10-01'))
    )
DISTRIBUTED BY HASH(page_id) BUCKETS 10 -- BUCKETS 数量根据 BE 节点数和数据量调整
PROPERTIES (
    "replication_num" = "1", -- 副本数，生产环境建议3
    "storage_type" = "COLUMN"
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
) ENGINE = OLAP
    COMMENT '页面引导日表'
    PARTITION BY RANGE (dt) (
        -- 示例分区，请根据实际数据日期范围调整
        PARTITION p20250731 VALUES [('2025-07-31'), ('2025-08-01')) )
DISTRIBUTED BY HASH(page_id, product_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "storage_type" = "COLUMN"
);

-- 3. Doris 表: ads_page_trend_daily
-- 对应 Hive 的 ads_page_trend_daily
DROP TABLE IF EXISTS ads_page_trend_daily;
CREATE TABLE ads_page_trend_daily
(
    dt               DATE COMMENT '统计日期',
    page_id          VARCHAR(64) COMMENT '页面ID',
    page_name        VARCHAR(256) COMMENT '页面名称',
    page_type        VARCHAR(64) COMMENT '页面类型',
    total_page_views BIGINT COMMENT '总页面浏览量',
    unique_visitors  BIGINT COMMENT '独立访客数 (UV)',
    total_clicks     BIGINT COMMENT '总点击次数',
    unique_clickers  BIGINT COMMENT '独立点击用户数'
) ENGINE = OLAP
    COMMENT '页面趋势日表'
    PARTITION BY RANGE (dt) (
        -- 示例分区，请根据实际数据日期范围调整
        -- 如果是滚动30天数据，可以考虑按月或按周分区，或者不分区，但每次全量覆盖
        PARTITION p202507 VALUES [('2025-07-01'), ('2025-08-01')) )
DISTRIBUTED BY HASH(page_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "storage_type" = "COLUMN"
);