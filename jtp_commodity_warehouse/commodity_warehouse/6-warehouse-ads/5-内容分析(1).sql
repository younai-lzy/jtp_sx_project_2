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

SHOW DATABASES ;

use jtp_gd03_warehouse;

SHOW TABLES ;

SELECT * FROM hive_catalog.jtp_gd03_warehouse.dwd_user_action_detail;

-- 创建短视频内容分析表
DROP TABLE IF EXISTS jtp_gd03_warehouse.short_video_analysis;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.short_video_analysis (
    `dt` DATE COMMENT '数据日期，分区字段，格式：YYYY-MM-DD',
     sku_id INT COMMENT 'SKU ID',
     product_id INT COMMENT '商品ID',
    `product_click_count` BIGINT COMMENT '指标1：商品点击次数',
    `fan_click_count` BIGINT COMMENT '指标2：粉丝点击次数',
    `collect_count` BIGINT COMMENT '指标3：引导收藏次数',
    `add_cart_count` BIGINT COMMENT '指标4：引导加购件数',
    `grass_planting_amount` DECIMAL(12, 2) COMMENT '指标5：种草成交金额'
)
    ENGINE = OLAP
-- 明细模型使用 DUPLICATE KEY（仅作为排序键，不进行去重）
    DUPLICATE KEY (dt,sku_id)
COMMENT '短视频分析'
-- 按 sku_id 哈希分布，优化商品维度查询
DISTRIBUTED BY HASH(dt,sku_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);


INSERT INTO jtp_gd03_warehouse.short_video_analysis
SELECT
    dt,
    sku_id,
    product_id,
    -- 指标1：商品点击次数
    COUNT(CASE WHEN content_type = 'short_video' AND action_type = 'click' AND sku_id IS NOT NULL THEN 1 END) AS product_click_count,
    -- 指标2：粉丝点击次数
    COUNT(CASE WHEN content_type = 'short_video' AND action_type = 'click' AND 1 = 1 THEN 1 END) AS fan_click_count,
    -- 指标3：引导收藏次数
    COUNT(CASE WHEN content_type = 'short_video' AND action_type = 'favorite' THEN 1 END) AS collect_count,
    -- 指标4：引导加购件数
    SUM(CASE WHEN content_type = 'short_video' AND action_type = 'add_to_cart' THEN buy_num END) AS add_cart_count,
    -- 指标5：种草成交金额
    SUM(CASE WHEN content_type = 'short_video' AND action_type = 'favorite' THEN item_total END) AS grass_planting_amount
FROM
    hive_catalog.jtp_gd03_warehouse.dwd_aggregated_wide
WHERE dt = '2025-08-07'
GROUP BY dt, sku_id, product_id;

SELECT * FROM jtp_gd03_warehouse.short_video_analysis;





