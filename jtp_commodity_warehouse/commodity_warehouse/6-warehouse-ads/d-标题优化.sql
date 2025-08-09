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

CREATE DATABASE IF NOT EXISTS ecommerce_dw;
use ecommerce_dw;



CREATE TABLE IF NOT EXISTS ecommerce_temp.dwd_search_term_analysis
(
    search_term  STRING COMMENT '搜索词',
    term_root    STRING COMMENT '词根',
    sku_id       INT COMMENT '关联SKU',
    pv           BIGINT COMMENT '曝光量',
    uv           BIGINT COMMENT '访客数',
    cart_num     BIGINT COMMENT '加购数',
    pay_user_cnt BIGINT COMMENT '支付用户数',
    pay_amount   DECIMAL(18, 2) COMMENT '支付金额',
    dt           STRING COMMENT '统计日期'
) PARTITION BY
LIST (partition_dt) (
    PARTITION p_20250807 VALUES IN ('2025-08-07')
    )
    DISTRIBUTED BY HASH(sku_id) BUCKETS 10;

-- 注意：
-- Doris 不支持 STORED AS PARQUET
-- Doris 使用 PARTITION BY LIST 或 RANGE，这里用 LIST 简单分区
-- DISTRIBUTED BY 是必须的，用于数据分片

-- 2. ETL 写入数据（INSERT OVERWRITE 分区）
INSERT OVERWRITE TABLE ecommerce_temp.dwd_search_term_analysis PARTITION (partition_dt = '2025-08-07')
WITH
-- 提取搜索行为（仅 action_type = 'search' 的记录）
search_data AS (SELECT user_id,
                       regexp_extract(review_content, '(搜索|查找|找)(.*?)[\\.\\,\\s]', 2) AS search_term,
                       sku_id
                FROM ecommerce_dw.dwd_user_action_detail
                WHERE dt = '2025-08-07'
                  AND action_type = 'search'),

-- 简化：不再做 LATERAL VIEW 拆词，term_root 假设为固定值或提前处理
-- 如果你要做真正的“分词”，需要提前处理或使用 UDF
term_split AS (SELECT user_id,
                      search_term,
-- 假设 term_root 是搜索词中的第一个词，或通过其它方式提取
-- 这里简化：直接取搜索词前几个字符作为词根（实际应该用真实分词逻辑）
-- 或者 term_root 可以是一个固定值，比如 'default'，或者从其它表关联
                      CASE
                          WHEN search_term IS NOT NULL AND length(search_term) > 0 THEN substr(search_term, 1, 1)
                          ELSE 'unk'
                          END AS term_root,
                      sku_id
               FROM search_data),

-- 关联用户行为获取加购/支付等转化数据
conversion_data AS (SELECT t.user_id,
                           t.search_term,
                           t.term_root,
                           t.sku_id,
                           COUNT(DISTINCT t.user_id)                                   AS uv,
                           COUNT(*)                                                    AS pv,
                           SUM(CASE WHEN a.action_type = 'add_cart' THEN 1 ELSE 0 END) AS cart_num,
                           SUM(CASE WHEN o.order_id IS NOT NULL THEN 1 ELSE 0 END)     AS pay_user_cnt,
                           SUM(COALESCE(o.total_amount, 0))                            AS pay_amount
                    FROM term_split t
                             LEFT JOIN ecommerce_dw.dwd_user_action_detail a
                                       ON t.user_id = a.user_id AND t.sku_id = a.sku_id AND a.dt = '2025-08-07'
                             LEFT JOIN ecommerce_dw.dwd_order_detail o
                                       ON t.user_id = o.user_id AND o.dt = '2025-08-07'
                    GROUP BY t.user_id, t.search_term, t.term_root, t.sku_id)

-- 最终写入目标表
SELECT search_term,
       term_root,
       sku_id,
       pv,
       uv,
       cart_num,
       pay_user_cnt,
       pay_amount,
       '2025-08-07' AS dt
FROM conversion_data;
















