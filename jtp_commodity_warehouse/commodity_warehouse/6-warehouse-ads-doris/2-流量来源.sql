DROP CATALOG IF EXISTS hive_catalog;
CREATE CATALOG IF NOT EXISTS hive_catalog PROPERTIES (
  'type'='hms',
  'hive.metastore.type' = 'hms',
  'hive.version' = '3.1.2',
  'fs.defaultFS' = 'hdfs://node101:8020',
  'hive.metastore.uris' = 'thrift://node101:9083'
);


CREATE DATABASE IF NOT EXISTS jtp_gd03_warehouse;

USE jtp_gd03_warehouse;

DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_Source_of_traffic;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.ads_Source_of_traffic(
    Traffic_source_channels VARCHAR(20) COMMENT '流量来源',
    uv BIGINT COMMENT 'uv',
    Collection_count BIGINT COMMENT '商品收藏人数',
    additional_count BIGINT COMMENT '加购人数',
    pay_buy_number BIGINT COMMENT '支付买家数'
)   ENGINE = OLAP
-- 明细模型使用 DUPLICATE KEY（仅作为排序键，不进行去重）
    DUPLICATE KEY (Traffic_source_channels)
COMMENT '流量来源'
-- 按 sku_id 哈希分布，优化商品维度查询
DISTRIBUTED BY HASH(Traffic_source_channels) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);

INSERT INTO jtp_gd03_warehouse.ads_Source_of_traffic
SELECT
    source_channel AS Traffic_source_channels,  -- 如 app、wap、pc 等
    COUNT(DISTINCT user_id) AS uv,  -- 去重统计各渠道独立访客

    -- 商品收藏人数（假设行为类型为 'collect'，需按实际业务值调整）
    COUNT(DISTINCT if(action_type = 'favorite',user_id,null)) AS Collection_count,

    -- 加购人数（行为类型为 'add_cart'）
    COUNT(DISTINCT if(action_type = 'add_to_cart',user_id,null)) AS additional_count,

    -- 支付买家数（订单状态为 'paid'，需按实际业务状态调整）
    COUNT(DISTINCT if(status = 'paid',user_id,null)) AS pay_buy_number

FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
GROUP BY source_channel  -- 按流量来源分组
ORDER BY uv DESC
;  -- 按访客数降序，优先看核心渠道



SELECT
    *
FROM jtp_gd03_warehouse.ads_Source_of_traffic;



