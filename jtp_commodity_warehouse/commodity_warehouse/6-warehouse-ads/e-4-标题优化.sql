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



CREATE TABLE IF NOT EXISTS ecommerce_dw.ads_term_root_analysis
(
    dt                      VARCHAR(10) NOT NULL COMMENT '统计日期',
    term_type               VARCHAR(20) COMMENT '词根类型(title/search)',
    term_root               VARCHAR(128) COMMENT '词根',
    sku_id                  INT COMMENT '关联SKU',
    exposure_users          BIGINT COMMENT '引流人数',
    exposure_cnt            BIGINT COMMENT '曝光量',
    click_users             BIGINT COMMENT '点击人数',
    click_cnt               BIGINT COMMENT '点击量',
    cart_users              BIGINT COMMENT '加购人数',
    consult_users           BIGINT COMMENT '咨询人数',
    first_step_convert_rate DECIMAL(5, 2) COMMENT '初级转化率'
)
    COMMENT '词根基础转化分析表' PARTITION BY
LIST (dt) (
    PARTITION p_20250807 VALUES IN ('2025-08-07')
    )
    DISTRIBUTED BY HASH(dt) BUCKETS 10
-- ✅ 关键：显式指定副本数为 1
    PROPERTIES (
    "replication_num" = "1"
    );



CREATE FUNCTION chinese_segment(VARCHAR) RETURNS ARRAY < VARCHAR >
    PROPERTIES (
    "symbol" = "com.example.ChineseSegment",
    "file" = "file:///path/to/your/udf.jar"
    );



#
INSERT OVERWRITE TABLE ecommerce_dw.ads_term_root_analysis PARTITION (partition_dt = '2025-08-07')
WITH
-- 标题词根提取（修正后的split用法）
title_terms AS (SELECT sku_id,
                       term,
                       'title' AS term_type
                FROM hive_catalog.ecommerce_dw.dwd_product_detail
                         LATERAL VIEW explode(split(cast(product_name AS STRING), '[ ,，、；;]')) t AS term
                WHERE dt = '2025-08-07'
                  AND length(trim(term)) > 1),

-- 搜索词根提取
search_terms AS (SELECT sku_id,
                        regexp_extract(cast(content AS STRING), '(搜索|查找)(.*?)[\\.\\,\\s]', 2) AS term,
                        'search'                                                                  AS term_type
                 FROM dwd_user_action_detail
                 WHERE dt = '2025-08-07'
                   AND action_type = 'search'
                   AND length(regexp_extract(cast(content AS STRING), '(搜索|查找)(.*?)[\\.\\,\\s]', 2)) > 0),

-- 合并词根
all_terms AS (SELECT *
              FROM title_terms
              UNION ALL
              SELECT *
              FROM search_terms),

-- 曝光行为统计
exposure_stats AS (SELECT t.term,
                          t.term_type,
                          t.sku_id,
                          COUNT(DISTINCT a.user_id) AS exposure_users,
                          COUNT(*)                  AS exposure_cnt
                   FROM all_terms t
                            JOIN dwd_user_action_detail a ON t.sku_id = a.sku_id
                   WHERE a.dt = '2025-08-07'
                     AND a.action_type = 'exposure'
                   GROUP BY t.term, t.term_type, t.sku_id),

-- 点击行为统计
click_stats AS (SELECT t.term,
                       t.term_type,
                       t.sku_id,
                       COUNT(DISTINCT a.user_id) AS click_users,
                       COUNT(*)                  AS click_cnt
                FROM all_terms t
                         JOIN dwd_user_action_detail a ON t.sku_id = a.sku_id
                WHERE a.dt = '2025-08-07'
                  AND a.action_type = 'click'
                GROUP BY t.term, t.term_type, t.sku_id),

-- 加购行为统计
cart_stats AS (SELECT t.term,
                      t.term_type,
                      t.sku_id,
                      COUNT(DISTINCT a.user_id) AS cart_users
               FROM all_terms t
                        JOIN dwd_user_action_detail a ON t.sku_id = a.sku_id
               WHERE a.dt = '2025-08-07'
                 AND a.action_type = 'add_cart'
               GROUP BY t.term, t.term_type, t.sku_id),

-- 咨询行为统计
consult_stats AS (SELECT t.term,
                         t.term_type,
                         t.sku_id,
                         COUNT(DISTINCT a.user_id) AS consult_users
                  FROM all_terms t
                           JOIN dwd_user_action_detail a ON t.sku_id = a.sku_id
                  WHERE a.dt = '2025-08-07'
                    AND a.action_type = 'consult'
                  GROUP BY t.term, t.term_type, t.sku_id)

SELECT '2025-08-07'                  AS dt,
       t.term_type,
       t.term                        AS term_root,
       t.sku_id,
       COALESCE(e.exposure_users, 0) AS exposure_users,
       COALESCE(e.exposure_cnt, 0)   AS exposure_cnt,
       COALESCE(c.click_users, 0)    AS click_users,
       COALESCE(c.click_cnt, 0)      AS click_cnt,
       COALESCE(ca.cart_users, 0)    AS cart_users,
       COALESCE(co.consult_users, 0) AS consult_users,
       ROUND(
                       (COALESCE(ca.cart_users, 0) + COALESCE(co.consult_users, 0)) /
                       NULLIF(COALESCE(e.exposure_users, 0), 0) * 100,
                       2)            AS first_step_convert_rate
FROM all_terms t
         LEFT JOIN exposure_stats e ON t.term = e.term AND t.term_type = e.term_type AND t.sku_id = e.sku_id
         LEFT JOIN click_stats c ON t.term = c.term AND t.term_type = c.term_type AND t.sku_id = c.sku_id
         LEFT JOIN cart_stats ca ON t.term = ca.term AND t.term_type = ca.term_type AND t.sku_id = ca.sku_id
         LEFT JOIN consult_stats co ON t.term = co.term AND t.term_type = co.term_type AND t.sku_id = co.sku_id;


























