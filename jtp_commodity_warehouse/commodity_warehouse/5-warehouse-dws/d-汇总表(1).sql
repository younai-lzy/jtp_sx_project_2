-- 创建数据库
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
-- todo 商品销售与流量汇总表

DROP TABLE IF EXISTS jtp_gd03_warehouse.dws_product_sales_traffic;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.dws_product_sales_traffic (
    dt VARCHAR(255) COMMENT '分区日期',
    sku_id BIGINT COMMENT '商品SKU ID',
    product_id BIGINT COMMENT '商品ID',
    product_name VARCHAR(255) COMMENT '商品名称',
    category_name VARCHAR(255) COMMENT '类目名称',
    total_uv BIGINT COMMENT '总访客数',
    wireless_uv BIGINT COMMENT '无线端访客数',
    pay_amount DECIMAL(20,2) COMMENT '支付金额',
    pay_buyers BIGINT COMMENT '支付买家数',
    add_cart_cnt BIGINT COMMENT '加购件数',
    bounce_rate DECIMAL(16,4) COMMENT '跳出率'
)
    ENGINE = OLAP
-- 明细模型使用 DUPLICATE KEY（仅作为排序键，不进行去重）
    DUPLICATE KEY (dt)
COMMENT '商品销售与流量汇总表'
-- 按 sku_id 哈希分布，优化商品维度查询
DISTRIBUTED BY HASH(dt) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);

INSERT INTO jtp_gd03_warehouse.dws_product_sales_traffic
SELECT
    a.dt,
    a.sku_id,
    p.product_id,
    p.product_name,
    p.category_name,
    -- 总访客数（去重用户ID）
    COUNT(DISTINCT a.user_id) AS total_uv,
    -- 无线端访客数（过滤设备类型）
    COUNT(DISTINCT CASE WHEN a.device_type = '无线端' THEN a.user_id END) AS wireless_uv,
    -- 支付金额（订单表求和）
    SUM(o.item_total) AS pay_amount,
    -- 支付买家数（去重支付用户）
    COUNT(DISTINCT o.user_id) AS pay_buyers,
    -- 加购件数（行为表统计）
    COUNT(DISTINCT CASE WHEN a.action_type = 'add_cart' THEN a.log_id END) AS add_cart_cnt,
    -- 跳出率（会话内仅1次行为的用户占比）
    ROUND(
                COUNT(DISTINCT CASE WHEN s.session_action_cnt = 1 THEN a.user_id END)
                / NULLIF(COUNT(DISTINCT a.user_id), 0), 4
        ) AS bounce_rate
FROM
    -- DWD层用户行为明细
    hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide a
-- 关联DIM层商品信息
        LEFT JOIN hive_catalog.jtp_commodity_warehouse.dim_product p
                  ON a.sku_id = p.sku_id AND a.dt = p.dt
-- 关联DWD层订单明细（支付状态）
        LEFT JOIN hive_catalog.jtp_commodity_warehouse.dwd_order_detail o
                  ON a.sku_id = o.sku_id AND a.dt = o.dt AND o.status = 'paid'
-- 子查询：计算会话内行为次数（用于跳出率）
        LEFT JOIN (
        SELECT
            user_id, session_id, dt,
            COUNT(1) AS session_action_cnt
        FROM hive_catalog.jtp_commodity_warehouse.dwd_user_action_detail
        GROUP BY user_id, session_id, dt
    ) s ON a.user_id = s.user_id AND a.session_id = s.session_id AND a.dt = s.dt
GROUP BY a.dt, a.sku_id, p.product_id, p.product_name, p.category_name;

SELECT
    *
FROM jtp_gd03_warehouse.dws_product_sales_traffic;



-- todo 商品评价与客群汇总表
DROP TABLE IF EXISTS jtp_gd03_warehouse.dws_product_review_crowd;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.dws_product_review_crowd (
    dt DATE COMMENT '数据日期',
    sku_id BIGINT COMMENT '商品SKU ID',
    overall_rating DECIMAL(5,2) COMMENT '整体评分',
    positive_review_ratio DECIMAL(16,4) COMMENT '正面评价占比',
    new_old_ratio DECIMAL(5,2) COMMENT '新客占比',
    age_distribution VARCHAR(255) COMMENT '年龄分布',
    top_related_sku BIGINT COMMENT '最相关连带商品'
)
    ENGINE = OLAP
-- 明细模型使用 DUPLICATE KEY（仅作为排序键，不进行去重）
    DUPLICATE KEY (dt)
COMMENT '商品评价与客群汇总表'
-- 按 sku_id 哈希分布，优化商品维度查询
DISTRIBUTED BY HASH(dt) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);

INSERT INTO jtp_gd03_warehouse.dws_product_review_crowd
SELECT
    -- 将字符串类型的dt转换为date类型
    TO_DATE(r.dt) AS dt,
    r.sku_id,
    -- 整体评分（评价表均值）
    AVG(r.score) AS overall_rating,
    -- 正面评价占比（正面评价数/总评价数）
    SUM(CASE WHEN r.is_positive = 1 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(r.review_id), 0) AS positive_review_ratio,
    -- 新客占比（通过注册时间判断新客）
    SUM(CASE WHEN DATE(u.registration_time) = TO_DATE(r.dt) THEN 1 ELSE 0 END)
        / NULLIF(COUNT(DISTINCT u.user_id), 0) AS new_old_ratio,
    -- 年龄分布（拼接分桶占比）
    CONCAT(
            '18-25:', ROUND(SUM(CASE WHEN u.age BETWEEN 18 AND 25 THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(DISTINCT u.user_id),0), 2), '%,',
            '26-35:', ROUND(SUM(CASE WHEN u.age BETWEEN 26 AND 35 THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(DISTINCT u.user_id),0), 2), '%,',
            '36-45:', ROUND(SUM(CASE WHEN u.age BETWEEN 36 AND 45 THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(DISTINCT u.user_id),0), 2), '%,',
            '46+: ', ROUND(SUM(CASE WHEN u.age >= 46 THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(DISTINCT u.user_id),0), 2), '%'
        ) AS age_distribution,
    -- 最相关连带商品
    MAX(CASE WHEN co_rank = 1 THEN related_sku END) AS top_related_sku
FROM
    hive_catalog.jtp_commodity_warehouse.dwd_product_review_detail r
        LEFT JOIN hive_catalog.jtp_commodity_warehouse.dwd_user_detail u
                  ON r.user_id = u.user_id AND r.dt = u.dt
        LEFT JOIN (
        SELECT
            main_sku,
            related_sku,
            co_purchase_cnt,
            ROW_NUMBER() OVER (PARTITION BY main_sku ORDER BY co_purchase_cnt DESC) AS co_rank
        FROM (
                 SELECT
                     o1.sku_id AS main_sku,
                     o2.sku_id AS related_sku,
                     COUNT(DISTINCT o1.order_id) AS co_purchase_cnt
                 FROM hive_catalog.jtp_commodity_warehouse.dwd_order_detail o1
                          JOIN hive_catalog.jtp_commodity_warehouse.dwd_order_detail o2
                               ON o1.order_id = o2.order_id AND o1.sku_id != o2.sku_id
                 GROUP BY o1.sku_id, o2.sku_id
             ) t
    ) co ON r.sku_id = co.main_sku AND co.co_rank = 1
GROUP BY TO_DATE(r.dt), r.sku_id;


SELECT
    *
FROM jtp_gd03_warehouse.dws_product_review_crowd;


-- todo 价格与竞争汇总表
DROP TABLE IF EXISTS jtp_gd03_warehouse.dws_price_competition;
CREATE TABLE IF NOT EXISTS jtp_gd03_warehouse.dws_price_competition (
    dt VARCHAR(255) COMMENT '数据日期',
    sku_id BIGINT COMMENT '商品SKU ID',
    price_strength_score DECIMAL(5,2) COMMENT '价格力评分',
    price_fluctuation_rate DECIMAL(16,4) COMMENT '价格波动幅度',
    related_heat_index DECIMAL(16,2) COMMENT '连带热度指数'
)
    ENGINE = OLAP
-- 明细模型使用 DUPLICATE KEY（仅作为排序键，不进行去重）
    DUPLICATE KEY (dt)
COMMENT '价格与竞争汇总表'
-- 按 sku_id 哈希分布，优化商品维度查询
DISTRIBUTED BY HASH(dt) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);


INSERT INTO jtp_gd03_warehouse.dws_price_competition
SELECT
    -- 数据日期（取自价格变动日志的分区日期）
    TO_DATE(pt.change_time) AS dt,
    -- 商品SKU ID（关联商品维度表）
    p.sku_id,
    -- 价格力评分（基于价格波动和转化率加权计算）
    ROUND(
                (pf.price_fluctuation_rate * 0.4) +
                (pc.pay_conversion_rate * 0.6), 2
        ) AS price_strength_score,
    -- 价格波动幅度（(最高价-最低价)/最低价）
    pf.price_fluctuation_rate,
    -- 连带热度指数（关联商品的访客转化占比）
    ROUND(
                    COALESCE(related.related_visitor_count, 0) * 1.0
                / NULLIF(pv.total_visitor_count, 0), 4
        ) AS related_heat_index
FROM
    -- 1. ODS层价格变动日志（原始价格数据）
    hive_catalog.jtp_commodity_warehouse.ods_price_trend_log pt
-- 2. 关联DIM层商品维度表（获取商品基础信息）
        LEFT JOIN hive_catalog.jtp_commodity_warehouse.dim_product p
                  ON pt.sku_id = p.sku_id
                      AND pt.dt = p.dt  -- 按日期分区关联
-- 3. 关联DWD层商品明细（计算价格波动）
        LEFT JOIN (
        SELECT
            sku_id,
            -- 价格波动幅度 = (最高价-最低价)/最低价
            ROUND(
                            (MAX(price_after) - MIN(price_before)) * 1.0
                        / NULLIF(MIN(price_before), 0), 4
                ) AS price_fluctuation_rate
        FROM hive_catalog.jtp_commodity_warehouse.dwd_product_detail
        WHERE dt = '2025-08-07'
        GROUP BY sku_id
    ) pf ON pt.sku_id = pf.sku_id
-- 4. 关联DWD层用户行为（计算支付转化率）
        LEFT JOIN (
        SELECT
            sku_id,
            -- 支付转化率 = 支付用户数 / 总访客数
            ROUND(
                            COUNT(DISTINCT CASE WHEN action_type = 'pay' THEN user_id END) * 1.0
                        / NULLIF(COUNT(DISTINCT user_id), 0), 4
                ) AS pay_conversion_rate
        FROM hive_catalog.jtp_commodity_warehouse.dwd_user_action_detail
        WHERE dt = '2025-08-07'
        GROUP BY sku_id
    ) pc ON pt.sku_id = pc.sku_id
-- 5. 关联DWD层关联商品数据（计算连带热度）
        LEFT JOIN (
        SELECT
            o1.sku_id AS main_sku,  -- 明确main_sku来自o1
            COUNT(DISTINCT o1.user_id) AS related_visitor_count  -- 明确使用o1的user_id
        FROM hive_catalog.jtp_commodity_warehouse.dwd_order_detail o1
                 JOIN hive_catalog.jtp_commodity_warehouse.dwd_order_detail o2
                      ON o1.order_id = o2.order_id
                          AND o1.sku_id != o2.sku_id  -- 排除自身关联
        WHERE o1.dt = '2025-08-07'
        GROUP BY o1.sku_id  -- 使用明确的字段分组
    ) related ON pt.sku_id = related.main_sku
-- 6. 关联DWD层总访客数（用于热度指数分母）
        LEFT JOIN (
        SELECT
            sku_id,
            COUNT(DISTINCT user_id) AS total_visitor_count
        FROM hive_catalog.jtp_commodity_warehouse.dwd_user_action_detail
        WHERE dt = '2025-08-07'
          AND action_type IN ('view', 'click')
        GROUP BY sku_id
    ) pv ON pt.sku_id = pv.sku_id
WHERE
        pt.dt = '2025-08-07'  -- 限定当前日期
  AND p.sku_id IS NOT NULL;  -- 过滤无效商品



SELECT
    *
FROM jtp_gd03_warehouse.dws_price_competition;




