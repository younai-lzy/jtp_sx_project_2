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


-- 1. 首先定义近7天日期范围
WITH date_range AS (SELECT date_sub('2025-08-07', 6) AS start_date,
                           '2025-08-07'              AS end_date),

-- 2. 识别用户在一天内的商品访问组合（连带关系）
     user_product_visits AS (SELECT user_id,
                                    sku_id,
                                    to_date(log_time) AS visit_date
                             FROM hive_catalog.ecommerce_dw.dwd_user_action_detail
                             WHERE dt BETWEEN (SELECT start_date FROM date_range) AND (SELECT end_date FROM date_range)
                               AND action_type IN ('click', 'view')
                             GROUP BY user_id, sku_id, to_date(log_time)),

-- 3. 识别用户在一天内的购买组合（连带关系）
     user_product_purchases AS (SELECT user_id,
                                       sku_id,
                                       to_date(create_time) AS purchase_date
                                FROM hive_catalog.ecommerce_dw.dwd_order_detail
                                WHERE dt BETWEEN (SELECT start_date FROM date_range) AND (SELECT end_date FROM date_range)
                                  AND status = 'paid'
                                GROUP BY user_id, sku_id, to_date(create_time)),

-- 4. 计算连带商品访客数（访问了A又访问了B的独立用户数）
     related_visitors AS (SELECT v1.sku_id                  AS main_sku,
                                 COUNT(DISTINCT v1.user_id) AS related_visitor_count
                          FROM user_product_visits v1
                                   JOIN
                               user_product_visits v2
                               ON
                                           v1.user_id = v2.user_id
                                       AND v1.visit_date = v2.visit_date
                                       AND v1.sku_id != v2.sku_id
                          GROUP BY v1.sku_id),

-- 5. 计算连带支付买家数（购买了A又购买了B的独立用户数）
     related_buyers AS (SELECT p1.sku_id                  AS main_sku,
                               COUNT(DISTINCT p1.user_id) AS related_buyer_count
                        FROM user_product_purchases p1
                                 JOIN
                             user_product_purchases p2
                             ON
                                         p1.user_id = p2.user_id
                                     AND p1.purchase_date = p2.purchase_date
                                     AND p1.sku_id != p2.sku_id
                        GROUP BY p1.sku_id),

-- 6. 计算各商品的总支付买家数
     total_buyers AS (SELECT sku_id,
                             COUNT(DISTINCT user_id) AS total_buyer_count
                      FROM user_product_purchases
                      GROUP BY sku_id)


-- 7. 合并所有指标
        ,
     combined_metrics AS (SELECT pd.product_id,
                                 pd.product_name,
                                 pd.category_id,
                                 pd.category_name,
                                 COALESCE(rv.related_visitor_count, 0) AS related_visitor_count,
                                 COALESCE(rb.related_buyer_count, 0)   AS related_buyer_count,
                                 COALESCE(tb.total_buyer_count, 0)     AS total_buyer_count,
                                 CASE
                                     WHEN COALESCE(tb.total_buyer_count, 0) > 0
                                         THEN ROUND(COALESCE(rv.related_visitor_count, 0) * 1.0 / tb.total_buyer_count,
                                                    3)
                                     ELSE 0
                                     END                               AS related_heat_index
                          FROM hive_catalog.ecommerce_dw.dwd_product_detail pd
                                   LEFT JOIN
                               related_visitors rv ON pd.sku_id = rv.main_sku
                                   LEFT JOIN
                               related_buyers rb ON pd.sku_id = rb.main_sku
                                   LEFT JOIN
                               total_buyers tb ON pd.sku_id = tb.sku_id
                          WHERE pd.dt = '2025-08-07')

-- 8. 最终排名结果（按连带热度指数降序）
SELECT product_id,
       product_name AS 商品名称, category_name AS 类目名称, related_visitor_count AS 近 7天连带商品访客数, related_buyer_count AS 近 7天连带支付买家数, total_buyer_count AS 近 7天总支付买家数, related_heat_index AS 连带热度指数, RANK() OVER (PARTITION BY category_id ORDER BY related_heat_index DESC) AS 类目连带热度排名, CONCAT(
        product_name, '连带访客数排类目第',
        RANK() OVER (PARTITION BY category_id ORDER BY related_visitor_count DESC)) AS 连带访客排名描述, CONCAT(
        product_name, '连带买家数排类目第',
        RANK() OVER (PARTITION BY category_id ORDER BY related_buyer_count DESC)) AS 连带买家排名描述
    FROM
    combined_metrics
ORDER BY related_heat_index DESC;



WITH date_range AS (SELECT date_sub('2025-08-07', 30) AS start_date, -- 使用30天历史数据
                           '2025-08-07'               AS end_date),

-- 获取日期范围变量
     range_values AS (SELECT start_date, end_date
                      FROM date_range
                      LIMIT 1),

-- 获取历史订单中的商品购买组合
     order_product_pairs AS (SELECT o1.order_id,
                                    o1.sku_id AS main_sku,
                                    o2.sku_id AS related_sku,
                                    o1.user_id
                             FROM hive_catalog.ecommerce_dw.dwd_order_detail o1
                                      CROSS JOIN range_values rv -- 引入日期范围变量
                                      JOIN
                                  hive_catalog.ecommerce_dw.dwd_order_detail o2
                                  ON
                                              o1.order_id = o2.order_id
                                          AND o1.sku_id != o2.sku_id -- 确保是不同的商品
                             WHERE o1.dt BETWEEN rv.start_date AND rv.end_date
                               AND o2.dt BETWEEN rv.start_date AND rv.end_date),

-- 计算商品连带购买率
     product_pair_stats AS (SELECT main_sku,
                                   related_sku,
                                   COUNT(DISTINCT order_id) AS co_purchase_count,
                                   COUNT(DISTINCT user_id)  AS co_purchase_user_count
                            FROM order_product_pairs
                            GROUP BY main_sku, related_sku),

-- 计算各商品的总购买次数
     product_purchase_stats AS (SELECT sku_id,
                                       COUNT(DISTINCT order_id) AS total_purchase_count,
                                       COUNT(DISTINCT user_id)  AS total_purchase_user_count
                                FROM hive_catalog.ecommerce_dw.dwd_order_detail o
                                         CROSS JOIN range_values rv
                                WHERE o.dt BETWEEN rv.start_date AND rv.end_date
                                GROUP BY sku_id),

-- 计算预测支付连带件数（基于协同过滤算法简化版）
     predicted_related_sales AS (SELECT pps.main_sku,
                                        pps.related_sku,
                                        -- 连带购买概率 = 共同购买次数 / 主商品购买次数
                                        (pps.co_purchase_count * 1.0 / pp.total_purchase_count) AS purchase_probability,
                                        -- 预测支付连带件数 = 连带购买概率 * 最近7天主商品购买量
                                        (pps.co_purchase_count * 1.0 / pp.total_purchase_count) *
                                        COALESCE(recent_main.buy_count, 0)                      AS predicted_sales_count
                                 FROM product_pair_stats pps
                                          JOIN
                                      product_purchase_stats pp ON pps.main_sku = pp.sku_id
                                          LEFT JOIN (
                                     -- 最近7天各商品购买量
                                     SELECT sku_id,
                                            COUNT(*) AS buy_count
                                     FROM hive_catalog.ecommerce_dw.dwd_order_detail
                                              CROSS JOIN range_values rv
                                     WHERE dt BETWEEN date_sub('2025-08-07', 6) AND '2025-08-07'
                                       AND status = 'paid'
                                     GROUP BY sku_id) recent_main ON pps.main_sku = recent_main.sku_id),

-- 最终推荐数据准备
     recommendation_base AS (SELECT prs.main_sku,
                                    prs.related_sku,
                                    main_pd.product_name     AS main_product_name,
                                    related_pd.product_name  AS related_product_name,
                                    main_pd.category_id      AS main_category_id,
                                    main_pd.category_name    AS main_category_name,
                                    related_pd.category_id   AS related_category_id,
                                    related_pd.category_name AS related_category_name,
                                    prs.purchase_probability,
                                    prs.predicted_sales_count
                             FROM predicted_related_sales prs
                                      JOIN
                                  hive_catalog.ecommerce_dw.dwd_product_detail main_pd
                                  ON
                                      prs.main_sku = main_pd.sku_id AND main_pd.dt = '2025-08-07'
                                      JOIN
                                  hive_catalog.ecommerce_dw.dwd_product_detail related_pd
                                  ON
                                      prs.related_sku = related_pd.sku_id AND related_pd.dt = '2025-08-07')

-- 最终推荐结果（按预测支付连带件数排序）
SELECT main_sku             AS 主商品 ID,
       main_product_name    AS 主商品名称, main_category_name AS 主商品类目, related_sku AS 连带商品 ID,
       related_product_name AS 连带商品名称, related_category_name AS 连带商品类目, purchase_probability AS 连带购买概率, predicted_sales_count AS 预测支付连带件数,
    -- 按预测支付连带件数排序 RANK() OVER (PARTITION BY main_sku ORDER BY predicted_sales_count DESC) AS 推荐优先级,
    -- 按连带购买概率排序 RANK() OVER (PARTITION BY main_sku ORDER BY purchase_probability DESC) AS 自然连带排序
FROM recommendation_base
WHERE predicted_sales_count > 0 -- 只显示有连带销售潜力的组合
ORDER BY main_sku,
    推荐优先级;








