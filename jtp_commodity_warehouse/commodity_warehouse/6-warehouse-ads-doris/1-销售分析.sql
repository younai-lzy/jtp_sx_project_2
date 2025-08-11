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
SHOW DATABASES;

# todo 核心概况
#   所有端
DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_Core_Overview;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.ads_Core_Overview
(
    dt                          DATE COMMENT '数据统计日期',
    sku_id                      BIGINT COMMENT '商品id',
    product_visitor_count       BIGINT COMMENT '商品访客数',
    product_micro_visitor_count BIGINT COMMENT '商品微详情访客数',
    product_view_count          BIGINT COMMENT '商品浏览量',
    avg_stay_duration           DECIMAL(16, 2) COMMENT '平均停留时长',
    bounce_rate                 DECIMAL(16, 2) COMMENT '商品详情页跳出率',
    product_add_cart_count      BIGINT COMMENT '商品加购人数'
) ENGINE = OLAP
-- 明细模型使用 DUPLICATE KEY（仅作为排序键，不进行去重）
    DUPLICATE KEY (dt, sku_id)
COMMENT '核心概况所有端'
-- 按 sku_id 哈希分布，优化商品维度查询
DISTRIBUTED BY HASH(sku_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);

INSERT INTO jtp_gd03_warehouse.ads_Core_Overview
SELECT dt,
       sku_id,
       -- 1. 商品访客数
       COUNT(DISTINCT if(action_type IN ('view', 'view_micro'), user_id, null)) AS product_visitor_count,
       -- 2. 商品微详情访客数
       COUNT(DISTINCT if(action_type = 'view_micro', 1, 0))                     AS product_micro_visitor_count,
       -- 3. 商品浏览量
       COUNT(if(action_type IN ('view', 'view_micro'), 1, 0))                   AS product_view_count,
       -- 4. 平均停留时长（需宽表有 stay_duration 字段）
       COUNT(if(action_type = 'view', 1, 0))                                    AS avg_stay_duration,
       -- 5. 商品详情页跳出率  假设1次行为即跳出
       ROUND(
                   COUNT(DISTINCT CASE
                                      WHEN action_type IN ('view', 'view_micro')
                                          AND session_action_cnt = 1 THEN user_id END)
                   / NULLIF(COUNT(DISTINCT user_id), 0),
                   2
           )                                                                    AS bounce_rate,

       -- 6. 商品加购人数
       COUNT(DISTINCT CASE WHEN action_type = 'add_cart' THEN user_id END)
                                                                                AS product_add_cart_count
FROM (
         -- 子查询：统计会话内行为次数（辅助计算跳出率）
         SELECT dt,
                sku_id,
                user_id,
                action_type,
                stay_duration,
                COUNT(1) OVER (PARTITION BY dt, sku_id, user_id, session_id) AS session_action_cnt
         FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide) sub
GROUP BY dt, sku_id;

SELECT *
FROM jtp_gd03_warehouse.ads_Core_Overview;


# todo 无线端
DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_Wireless_Core_Overview;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.ads_Wireless_Core_Overview
(
    dt                          DATE COMMENT '数据统计日期',
    sku_id                      BIGINT COMMENT '商品id',
    product_visitor_count       BIGINT COMMENT '商品访客数',
    product_micro_visitor_count BIGINT COMMENT '商品微详情访客数',
    product_view_count          BIGINT COMMENT '商品浏览量',
    avg_stay_duration           DECIMAL(16, 2) COMMENT '平均停留时长',
    bounce_rate                 DECIMAL(16, 2) COMMENT '商品详情页跳出率',
    product_add_cart_count      BIGINT COMMENT '商品加购人数'
) ENGINE = OLAP
-- 明细模型使用 DUPLICATE KEY（仅作为排序键，不进行去重）
    DUPLICATE KEY (dt, sku_id)
COMMENT '核心概况无线端'
-- 按 sku_id 哈希分布，优化商品维度查询
DISTRIBUTED BY HASH(sku_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);


INSERT INTO jtp_gd03_warehouse.ads_Wireless_Core_Overview
SELECT dt,
       sku_id,
       -- 1. 商品访客数
       COUNT(DISTINCT if(action_type IN ('view', 'view_micro'), user_id, null)) AS product_visitor_count,
       -- 2. 商品微详情访客数
       COUNT(DISTINCT if(action_type = 'view_micro', 1, 0))                     AS product_micro_visitor_count,
       -- 3. 商品浏览量
       COUNT(if(action_type IN ('view', 'view_micro'), 1, 0))                   AS product_view_count,
       -- 4. 平均停留时长（需宽表有 stay_duration 字段）
       COUNT(if(action_type = 'view', 1, 0))                                    AS avg_stay_duration,
       -- 5. 商品详情页跳出率  假设1次行为即跳出
       ROUND(
                   COUNT(DISTINCT CASE
                                      WHEN action_type IN ('view', 'view_micro')
                                          AND session_action_cnt = 1 THEN user_id END)
                   / NULLIF(COUNT(DISTINCT user_id), 0),
                   2
           )                                                                    AS bounce_rate,

       -- 6. 商品加购人数
       COUNT(DISTINCT CASE WHEN action_type = 'add_cart' THEN user_id END)      AS product_add_cart_count
FROM (
         -- 子查询：统计会话内行为次数（辅助计算跳出率）
         SELECT dt,
                sku_id,
                user_id,
                action_type,
                stay_duration,
                COUNT(1) OVER (PARTITION BY dt, sku_id, user_id, session_id) AS session_action_cnt
         FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
         WHERE device_type in ('无线端')) sub
GROUP BY dt, sku_id;

SELECT *
FROM jtp_gd03_warehouse.ads_Wireless_Core_Overview;

# todo 2.销售详情
DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_Sales_details;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.ads_Sales_details
(
    dts               DATE COMMENT '数据统计日期',
    sku_id            BIGINT COMMENT 'SKUID',
    product_id        BIGINT COMMENT '商品ID',
    product_name      VARCHAR(220) COMMENT '商品名称',
    add_cart_quantity BIGINT COMMENT '加购件数',
    payment_amount    DECIMAL(16, 2) COMMENT '支付金额',
    paying_buyers     DECIMAL(16, 2) COMMENT '支付买家数',
    sku_info          VARCHAR(220) COMMENT 'SKU信息'
) ENGINE = OLAP
-- 明细模型使用 DUPLICATE KEY（仅作为排序键，不进行去重）
    DUPLICATE KEY (dts, sku_id)
COMMENT '销售详情'
-- 按 sku_id 哈希分布，优化商品维度查询
DISTRIBUTED BY HASH(sku_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);

INSERT INTO jtp_gd03_warehouse.ads_Sales_details
SELECT dts,
       sku_id,
       product_id,
       product_name,
       -- 指标1：加购件数（统计加购行为的商品数量总和，按 sku 维度，可根据实际业务逻辑调整，这里简单统计加购行为的记录数代表加购件数 ）
       COUNT(if(action_type = 'add_cart', 1, 0))          AS add_cart_quantity,
       -- 指标2：支付金额（统计支付成功订单的总金额，根据 order_status 判断支付状态，可结合实际业务状态值调整 ）
       SUM(if(status = 'paid', total_amount, 0))          AS payment_amount,
       -- 指标3：支付买家数（统计支付成功订单的独立买家数量 ）
       COUNT(DISTINCT if(status = 'paid', user_id, null)) AS paying_buyers,
       -- 指标4：SKU 信息（直接选取宽表中 SKU 相关的标识字段，这里选取 sku_id、product_name 等代表 SKU 信息，可按需扩充 ）
       CONCAT(sku_id, '-', product_name)                  AS sku_info
FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
GROUP BY dts,
         sku_id,
         product_id,
         product_name
ORDER BY dts,
         sku_id;

SELECT *
FROM jtp_gd03_warehouse.ads_Sales_details;

# todo 颜色分类
DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_Color_classification;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.ads_Color_classification
(
    color_category     VARCHAR(20) COMMENT '颜色类型',
    pay_money          BIGINT COMMENT '支付金额',
    pay_count          BIGINT COMMENT '支付次数',
    pay_buy_number     VARCHAR(20) COMMENT '支付买家人数',
    sum_pay_money      BIGINT COMMENT '总支付金额',
    sum_pay_buy_number DECIMAL(16, 2) COMMENT '总支付买家人数',
    pay_money_rate     DECIMAL(16, 2) COMMENT '支付金额占比',
    pay_buyer_rate     BIGINT COMMENT '支付买家数占比'
) ENGINE = OLAP
-- 明细模型使用 DUPLICATE KEY（仅作为排序键，不进行去重）
    DUPLICATE KEY (color_category)
COMMENT '颜色分类'
-- 按 sku_id 哈希分布，优化商品维度查询
DISTRIBUTED BY HASH(color_category) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);



INSERT INTO jtp_gd03_warehouse.ads_Color_classification
WITH color_stats AS (SELECT color_category                                     AS color_category,
                            SUM(if(status = 'paid', total_amount, 0))          AS pay_money,
                            SUM(if(status = 'paid', buy_num, 0))               AS pay_count,
                            COUNT(DISTINCT if(status = 'paid', user_id, null)) AS pay_buy_number
                     FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
                     GROUP BY color_category),
     total_stats AS (
         -- 单独计算总支付金额和总支付买家数
         SELECT SUM(pay_count)                   AS sum_pay_money,
                count(distinct (pay_buy_number)) AS sum_pay_buy_number
         FROM color_stats)
-- 修正总支付买家数的计算方式
SELECT *,
       ROUND(pay_count / NULLIF(total_stats.sum_pay_money, 0), 4)           AS pay_money_rate,
       ROUND(pay_buy_number / NULLIF(total_stats.sum_pay_buy_number, 0), 4) AS pay_buyer_rate
FROM color_stats,
     total_stats
ORDER BY sum_pay_money DESC
;

SELECT *
FROM jtp_gd03_warehouse.ads_Color_classification;


# todo 价格分析
DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_day_price;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.ads_day_price
(
    dt         DATE COMMENT '日期',
    item_price BIGINT COMMENT '单价'
) ENGINE = OLAP
-- 明细模型使用 DUPLICATE KEY（仅作为排序键，不进行去重）
    DUPLICATE KEY (dt)
COMMENT '价格分析'
-- 按 sku_id 哈希分布，优化商品维度查询
DISTRIBUTED BY HASH(dt) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);

INSERT INTO jtp_gd03_warehouse.ads_day_price
SELECT dt,
       item_price
FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
WHERE status = 'paid'
ORDER BY dt; -- 按日期排序，呈现趋势

SELECT *
FROM jtp_gd03_warehouse.ads_day_price;