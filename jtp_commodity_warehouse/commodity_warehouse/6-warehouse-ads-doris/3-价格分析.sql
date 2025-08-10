DROP CATALOG IF EXISTS hive_catalog;
CREATE CATALOG IF NOT EXISTS hive_catalog PROPERTIES (
  'type'='hms',
  'hive.metastore.type' = 'hms',
  'hive.version' = '3.1.2',
  'fs.defaultFS' = 'hdfs://node101:8020',
  'hive.metastore.uris' = 'thrift://node101:9083'
);

SHOW DATABASES ;

USE jtp_gd03_warehouse;
-- ==================================================================================================
-- todo 1.价格力商品
-- ==================================================================================================

-- todo 指标1：全天价格力星级（整体）
DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_Allday_price_star_rating;
CREATE TABLE jtp_gd03_warehouse.ads_Allday_price_star_rating (
    `sku_id` DATE COMMENT 'skuid',
    `product_name` VARCHAR(100) COMMENT '商品名称',
    `price_strength_score` DECIMAL(5, 2) COMMENT '价格评分',
    `price_strength_level` VARCHAR(500) COMMENT '价格星级'

)    ENGINE = OLAP
    DUPLICATE KEY (sku_id)
COMMENT '客群分析'
DISTRIBUTED BY HASH(sku_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);

INSERT INTO jtp_gd03_warehouse.ads_Allday_price_star_rating
WITH price_fluctuation AS (
    -- 步骤1：计算商品价格波动
    SELECT
        sku_id,
        MAX(original_price) AS max_price,
        MIN(original_price) AS min_price,
        -- 价格波动幅度（(最高价-最低价)/最低价）
        ROUND(
                        (MAX(original_price) - MIN(original_price)) * 1.0
                    / NULLIF(MIN(original_price), 4),
                        4
            ) AS price_fluctuation_rate
    FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
    WHERE dt BETWEEN '2025-08-03' AND '2025-08-07'  -- 近9天价格区间
    GROUP BY sku_id
),
     product_conversion AS (
         -- 步骤2：复用“商品力核心指标”的转化数据
         SELECT
             sku_id,
             COUNT(DISTINCT if(action_type = 'buy',user_id,null)) AS pay_user_cnt,
             COUNT(DISTINCT if(action_type IN ('click', 'view'),user_id,null)) AS visit_user_cnt
         FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
         WHERE dt = '2025-08-07'  -- 单日转化
         GROUP BY sku_id
     )

-- 步骤3：综合计算价格力星级（价格波动权重0.4，转化权重0.6，可自定义）
SELECT
    pf.sku_id,
    MAX(product_name) AS product_name,  -- 从宽表取商品名
    ROUND(
                    pf.price_fluctuation_rate * 0.4
                + CASE
                      WHEN pc.visit_user_cnt = 0 THEN 0
                      ELSE pc.pay_user_cnt * 1.0 / pc.visit_user_cnt
                      END * 0.6,
                    2
        ) AS price_strength_score,
    -- 星级映射（自定义分段，如 0.8+→5星，0.6-0.8→4星...）
    CASE
        WHEN ROUND(pf.price_fluctuation_rate * 0.4 +
                   CASE WHEN pc.visit_user_cnt=0 THEN 0
                        ELSE pc.pay_user_cnt*1.0/pc.visit_user_cnt
                       END * 0.6, 2) >= 0.8 THEN '5星'
        WHEN ROUND(pf.price_fluctuation_rate * 0.4 +
                   CASE WHEN pc.visit_user_cnt=0 THEN 0
                        ELSE pc.pay_user_cnt*1.0/pc.visit_user_cnt
                       END * 0.6, 2) >= 0.6 THEN '4星'
        WHEN ROUND(pf.price_fluctuation_rate * 0.4 +
                   CASE WHEN pc.visit_user_cnt=0 THEN 0
                        ELSE pc.pay_user_cnt*1.0/pc.visit_user_cnt
                       END * 0.6, 2) >= 0.4 THEN '3星'
        WHEN ROUND(pf.price_fluctuation_rate * 0.4 +
                   CASE WHEN pc.visit_user_cnt=0 THEN 0
                        ELSE pc.pay_user_cnt*1.0/pc.visit_user_cnt
                       END * 0.6, 2) >= 0.2 THEN '2星'
        ELSE '1星'
        END AS price_strength_level
FROM price_fluctuation pf
         JOIN product_conversion pc ON pf.sku_id = pc.sku_id
-- 关联宽表取商品名称（或 JOIN dw_product_detail）
         LEFT JOIN hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide wd ON pf.sku_id = wd.sku_id
GROUP BY pf.sku_id, pc.pay_user_cnt, pc.visit_user_cnt, pf.price_fluctuation_rate
ORDER BY price_strength_score DESC
;



SELECT
    *
FROM jtp_gd03_warehouse.ads_Allday_price_star_rating;



-- todo 指标2：商品力核心指标


DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_indicators_commodity_power;
CREATE TABLE jtp_gd03_warehouse.ads_indicators_commodity_power (
    `sku_id` DATE COMMENT 'skuid',
    `product_name` VARCHAR(100) COMMENT '商品名称',
    `category_id` DECIMAL(5, 2) COMMENT '类别id',
    `category_name` VARCHAR(500) COMMENT '类别名称',
    `pay_conversion_rate` VARCHAR(500) COMMENT '支付转化率',
    `visit_user_cnt` VARCHAR(500) COMMENT '总访客数',
    `total_order_item_cnt` VARCHAR(500) COMMENT '总成交件数',
    `tb_search_exposure_cnt` VARCHAR(500) COMMENT '手机淘宝搜索推荐平台总曝光量'
)    ENGINE = OLAP
    DUPLICATE KEY (sku_id)
COMMENT '客群分析'
DISTRIBUTED BY HASH(sku_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);

INSERT INTO jtp_gd03_warehouse.ads_indicators_commodity_power
-- 商品力核心指标计算：支付转化率、总访客、总成交、曝光量
SELECT
    sku_id,
    product_name,
    category_id,
    category_name,
    -- 1. 支付转化率 = 支付用户数 / 访问用户数（访问定义：click/view 行为）
    CASE
        WHEN visit_user_cnt = 0 THEN 0
        ELSE ROUND(pay_user_cnt * 1.0 / visit_user_cnt, 4)
        END AS pay_conversion_rate,
    -- 2. 总访客数（去重访问用户数）
    visit_user_cnt,
    -- 3. 总成交件数（buy 行为的购买数量总和）
    total_order_item_cnt,
    -- 4. 手机淘宝搜索推荐平台总曝光量（模拟：来源为 app + 设备手机 + view 行为）
    tb_search_exposure_cnt
FROM (
         SELECT
             sku_id,
             -- 关联商品基础信息（从宽表取，也可 JOIN dwd_product_detail 补充）
             MAX(product_name) AS product_name,
             MAX(category_id) AS category_id,
             MAX(category_name) AS category_name,
             -- 支付用户数（buy 行为去重）
             COUNT(DISTINCT if(status = 'paid',user_id,null)) AS pay_user_cnt,
             -- 访问用户数（click/view 行为去重）
             COUNT(DISTINCT if( action_type IN ('click', 'view'),user_id,null)) AS visit_user_cnt,
             -- 总成交件数（buy 行为的 buy_num 求和）
             SUM(if(status = 'paid',buy_num,0)) AS total_order_item_cnt,
             -- 手机淘宝搜索推荐曝光（来源 app + 设备 phone + view 行为，log_id 去重）
             COUNT(DISTINCT if(source_channel = 'app' and device_type = '无线端' and action_type = 'view',log_id,null)) AS tb_search_exposure_cnt
         FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
         WHERE dt = '2025-08-07'  -- 替换实际分区日期
         GROUP BY sku_id
     ) core_metrics
ORDER BY sku_id
;

SELECT
    *
FROM jtp_gd03_warehouse.ads_indicators_commodity_power;



-- todo 3. 价格力商品榜单

DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_List_price_driven_products;
CREATE TABLE jtp_gd03_warehouse.ads_List_price_driven_products (
    `sku_id` bigint COMMENT 'skuid',
    `product_name` VARCHAR(100) COMMENT '商品名称',
    `price_strength_score` DECIMAL(5, 2) COMMENT '价格评分',
    `price_strength_level` VARCHAR(500) COMMENT '价格星级',
    `rank` bigint COMMENT '排名'
)    ENGINE = OLAP
    DUPLICATE KEY (sku_id)
COMMENT '客群分析'
DISTRIBUTED BY HASH(sku_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);

INSERT INTO jtp_gd03_warehouse.ads_List_price_driven_products
-- 价格力商品榜单：取价格力星级 Top10
WITH price_strength AS (
    -- 复用上面“全天价格力星级”查询结果
    SELECT
        sku_id,
        product_name,
        price_strength_score,
        price_strength_level
    FROM (
             -- 此处直接嵌套“全天价格力星级”的查询逻辑，或替换为表存储结果
             WITH price_fluctuation AS (
                 SELECT
                     sku_id,
                     MAX(original_price) AS max_price,
                     MIN(original_price) AS min_price,
                     ROUND(
                                     (MAX(original_price) - MIN(original_price)) * 1.0
                                 / NULLIF(MIN(original_price), 0),
                                     4
                         ) AS price_fluctuation_rate
                 FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
                 WHERE dt BETWEEN '2025-08-01' AND '2025-08-09'
                 GROUP BY sku_id
             ),
                  product_conversion AS (
                      SELECT
                          sku_id,
                          COUNT(DISTINCT CASE WHEN action_type = 'buy' THEN user_id END) AS pay_user_cnt,
                          COUNT(DISTINCT CASE WHEN action_type IN ('click', 'view') THEN user_id END) AS visit_user_cnt
                      FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
                      WHERE dt = '2025-08-07'
                      GROUP BY sku_id
                  )
             SELECT
                 pf.sku_id,
                 MAX(wd.product_name) AS product_name,
                 -- 最终得分 = 价格波动权重(40%) + 转化权重(60%)
                 ROUND(
                                 pf.price_fluctuation_rate * 0.4
                             + CASE
                                   WHEN pc.visit_user_cnt = 0 THEN 0
                                   ELSE pc.pay_user_cnt * 1.0 / pc.visit_user_cnt
                                   END * 0.6,
                                 2
                     ) AS price_strength_score,
                 CASE
                     WHEN ROUND(pf.price_fluctuation_rate * 0.4 +
                                CASE WHEN pc.visit_user_cnt=0 THEN 0
                                     ELSE pc.pay_user_cnt*1.0/pc.visit_user_cnt
                                    END * 0.6, 2) >= 0.8 THEN '5星'
                     WHEN ROUND(pf.price_fluctuation_rate * 0.4 +
                                CASE WHEN pc.visit_user_cnt=0 THEN 0
                                     ELSE pc.pay_user_cnt*1.0/pc.visit_user_cnt
                                    END * 0.6, 2) >= 0.6 THEN '4星'
                     WHEN ROUND(pf.price_fluctuation_rate * 0.4 +
                                CASE WHEN pc.visit_user_cnt=0 THEN 0
                                     ELSE pc.pay_user_cnt*1.0/pc.visit_user_cnt
                                    END * 0.6, 2) >= 0.4 THEN '3星'
                     WHEN ROUND(pf.price_fluctuation_rate * 0.4 +
                                CASE WHEN pc.visit_user_cnt=0 THEN 0
                                     ELSE pc.pay_user_cnt*1.0/pc.visit_user_cnt
                                    END * 0.6, 2) >= 0.2 THEN '2星'
                     ELSE '1星'
                     END AS price_strength_level
             FROM price_fluctuation pf
                      JOIN product_conversion pc ON pf.sku_id = pc.sku_id
                      LEFT JOIN hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide wd ON pf.sku_id = wd.sku_id
             GROUP BY pf.sku_id, pc.pay_user_cnt, pc.visit_user_cnt, pf.price_fluctuation_rate
         ) temp_strength
)
-- 生成 Top10 榜单
SELECT
    sku_id,
    product_name,
    price_strength_score,
    price_strength_level,
    -- 排名
    ROW_NUMBER() OVER (ORDER BY price_strength_score DESC) AS rank
FROM price_strength
LIMIT 30
;  -- 取 Top10，可调整数量


SELECT
    *
FROM jtp_gd03_warehouse.ads_List_price_driven_products;



-- ==================================================================================================
-- todo 价格趋势
-- ==================================================================================================

-- todo 指标1：价格指标（单件）
-- 价格指标（单件）：原始价、成交价、价格波动

DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_Price_indicator;
CREATE TABLE jtp_gd03_warehouse.ads_Price_indicator (
    `sku_id` bigint COMMENT 'skuid',
    `product_name` VARCHAR(100) COMMENT '商品名称',
    `category_id` bigint COMMENT '价格评分',
    `category_name` VARCHAR(500) COMMENT '价格星级',
    `single_original_price` decimal(16,2) COMMENT '排名',
    `single_transaction_price` decimal(16,2) COMMENT '排名',
    `price_fluctuation_rate` decimal(16,2) COMMENT '排名'
)    ENGINE = OLAP
    DUPLICATE KEY (sku_id)
COMMENT '客群分析'
DISTRIBUTED BY HASH(sku_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);


INSERT INTO jtp_gd03_warehouse.ads_Price_indicator
SELECT
    sku_id,
    product_name,
    category_id,
    category_name,
    -- 1. 单件原始价格（取自商品信息）
    MAX(original_price) AS single_original_price,
    -- 2. 单件成交价格（订单维度：item_price 为实际成交价）
    AVG(item_price) AS single_transaction_price,
    -- 3. 价格波动幅度（原始价 vs 成交价）
    CASE
        WHEN MAX(original_price) = 0 THEN 0
        ELSE ROUND(
                        (AVG(item_price) - MAX(original_price)) * 1.0
                    / MAX(original_price),
                        4
            )
        END AS price_fluctuation_rate
FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
WHERE dt = '2025-08-07'  -- 按需替换分区日期
GROUP BY
    sku_id,
    product_name,
    category_id,
    category_name
ORDER BY sku_id
;

SELECT
    *
FROM jtp_gd03_warehouse.ads_Price_indicator;

# todo 指标二:影响指标
-- 影响指标：访客数、支付金额、支付买家数、支付件数
DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_Influencing_indicators;
CREATE TABLE jtp_gd03_warehouse.ads_Influencing_indicators (
    `sku_id` bigint COMMENT 'skuid',
    `product_name` VARCHAR(100) COMMENT '商品名称',
    `category_id` bigint COMMENT '类别id',
    `category_name` VARCHAR(500) COMMENT '类别名称',
    `product_visitor_cnt` decimal(16,2) COMMENT '商品访客数',
    `total_pay_amount` decimal(20,2) COMMENT '支付金额',
    `pay_buyer_cnt` decimal(16,2) COMMENT '支付买家数',
    `total_pay_item_cnt` decimal(16,2) COMMENT '支付件数'
)    ENGINE = OLAP
    DUPLICATE KEY (sku_id)
COMMENT '客群分析'
DISTRIBUTED BY HASH(sku_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);

INSERT INTO jtp_gd03_warehouse.ads_Influencing_indicators
SELECT
    sku_id,
    product_name,
    category_id,
    category_name,
    -- 1. 商品访客数（去重访问用户数，行为类型：click/view）
    COUNT(DISTINCT if(action_type IN ('click', 'view'),user_id,null)) AS product_visitor_cnt,
    -- 2. 支付金额（订单维度：item_total 为单品总金额，求和）
    SUM(if(status = 'paid',item_total,null)) AS total_pay_amount,
    -- 3. 支付买家数（去重支付用户数，行为类型：buy）
    COUNT(DISTINCT if( status = 'paid',user_id,null)) AS pay_buyer_cnt,
    -- 4. 支付件数（订单维度：buy_num 为购买数量，求和）
    SUM(if(status = 'paid', buy_num, 0)) AS total_pay_item_cnt
FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
WHERE dt = '2025-08-07'  -- 按需替换分区日期
GROUP BY
    sku_id,
    product_name,
    category_id,
    category_name
ORDER BY sku_id
;

SELECT
    *
FROM jtp_gd03_warehouse.ads_Influencing_indicators;

-- ==================================================================================================
-- todo 价格竞争
-- ==================================================================================================

# todo 交易增速计算


DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_Calculation_transaction_rate;
CREATE TABLE jtp_gd03_warehouse.ads_Calculation_transaction_rate (
    `sku_id` bigint COMMENT 'skuid',
    `product_name` VARCHAR(100) COMMENT '商品名称',
    `category_id` bigint COMMENT '类别id',
    `category_name` VARCHAR(500) COMMENT '类别名称',
    `week_part` VARCHAR(500) COMMENT '周',
    `total_buy_qty` BIGINT COMMENT '交易额',
    `prev_week_buy_qty` decimal(20,2) COMMENT '周交易量',
    `buy_qty_growth_rate` decimal(16,2) COMMENT '周交易率',
    `price_competitive_growth_rate` decimal(16,2) COMMENT '价格竞争修正后的增速'
)    ENGINE = OLAP
    DUPLICATE KEY (sku_id)
COMMENT '客群分析'
DISTRIBUTED BY HASH(sku_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);

INSERT INTO jtp_gd03_warehouse.ads_Calculation_transaction_rate
WITH
-- 步骤1：先计算窗口函数（类目平均价），避免与 GROUP BY 冲突
base_data AS (
    SELECT
        sku_id,
        product_name,
        category_id,
        category_name,
        dts AS trade_date,
        status,
        buy_num,
        item_total,
        original_price,
        -- 窗口函数：计算类目平均价（放在子查询中，先于 GROUP BY 执行）
        AVG(original_price) OVER (PARTITION BY category_id) AS competitor_avg_price
    FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
    WHERE dt BETWEEN '2025-08-01' AND '2025-08-31'
)
        ,
-- 步骤2：再进行 GROUP BY 聚合（此时已包含窗口函数结果）
trans_base AS (
    SELECT
        sku_id,
        product_name,
        category_id,
        category_name,
        trade_date,
        competitor_avg_price,  -- 直接使用子查询中的窗口函数结果
        -- 交易量（购买行为的件数总和）
        SUM(if(status = 'paid',buy_num,0)) AS total_buy_qty,
        -- 交易额（购买行为的金额总和）
        SUM(if(status = 'paid',item_total,0)) AS total_buy_amt,
        -- 当前商品价格（取当日最大原始价）
        MAX(original_price) AS current_price
    FROM base_data  -- 从子查询获取数据
    GROUP BY
        sku_id, product_name, category_id, category_name, trade_date, competitor_avg_price
),
-- 步骤3：周度交易对比（计算增速）
trans_week AS (
    SELECT
        sku_id,
        product_name,
        category_id,
        category_name,
        CONCAT('WEEK_', YEARWEEK(trade_date, 1)) AS week_part,
        total_buy_qty,
        total_buy_amt,
        current_price,
        competitor_avg_price,
        -- 上周同期交易量，添加默认值0
        LAG(total_buy_qty, 7, 1) OVER (PARTITION BY sku_id ORDER BY trade_date) AS prev_week_buy_qty,
        -- 上周同期交易额，添加默认值0
        LAG(total_buy_amt, 7, 1) OVER (PARTITION BY sku_id ORDER BY trade_date) AS prev_week_buy_amt
    FROM trans_base
)

-- 步骤4：计算交易增速
SELECT
    sku_id,
    product_name,
    category_id,
    category_name,
    week_part,
    total_buy_qty,
    prev_week_buy_qty,
    -- 交易量增速
    CASE
        WHEN prev_week_buy_qty = 0 THEN 0
        ELSE ROUND((total_buy_qty - prev_week_buy_qty) / prev_week_buy_qty, 4)
        END AS buy_qty_growth_rate,
    -- 价格竞争修正后的增速
    CASE
        WHEN prev_week_buy_qty = 0 THEN 0
        ELSE ROUND(
                        (total_buy_qty - prev_week_buy_qty) / prev_week_buy_qty
                    * (current_price / NULLIF(competitor_avg_price, 0)), 4
            )
        END AS price_competitive_growth_rate
FROM trans_week
WHERE week_part IS NOT NULL
ORDER BY price_competitive_growth_rate DESC;


SELECT
    *
FROM jtp_gd03_warehouse.ads_Calculation_transaction_rate;


# todo 需求供给比指数计算
-- 需求供给比指数（需求=用户兴趣行为，供给=商品可售量）

DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_Calculation_demand_supply_ratio;
CREATE TABLE jtp_gd03_warehouse.ads_Calculation_demand_supply_ratio (
    `sku_id` bigint COMMENT 'skuid',
    `product_name` VARCHAR(100) COMMENT '商品名称',
    `category_id` bigint COMMENT '类别id',
    `category_name` VARCHAR(500) COMMENT '类别名称',
    `demand_user_cnt` bigint COMMENT '需求指标',
    `supply_available_qty` BIGINT COMMENT '供给指标',
    `demand_supply_ratio` bigint COMMENT '需求供给比'
)    ENGINE = OLAP
    DUPLICATE KEY (sku_id)
COMMENT '客群分析'
DISTRIBUTED BY HASH(sku_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);


INSERT INTO jtp_gd03_warehouse.ads_Calculation_demand_supply_ratio
SELECT
    sku_id,
    product_name,
    category_id,
    category_name,
    -- 需求指标：用户兴趣行为（点击+浏览+加购去重用户数）
    COUNT(DISTINCT CASE
                       WHEN action_type IN ('click', 'view', 'add_cart')
                           THEN user_id
        END) AS demand_user_cnt,
    -- 供给指标：商品可售量（简化为库存字段，需替换实际库存逻辑，此处用购买量反向推导）
    SUM(if(status = 'paid',buy_num,0)) AS supply_available_qty,
    -- 需求供给比（需求 / 供给，需业务定义合理范围）
    CASE
        WHEN SUM(if(status = 'paid',buy_num,0)) = 0 THEN 0
        ELSE ROUND(
                    COUNT(DISTINCT if (action_type IN ('click', 'view', 'add_cart'),user_id,null)) / SUM(if(status = 'paid',buy_num,0)), 2
            )
        END AS demand_supply_ratio
FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
WHERE dt = '2025-08-07'  -- 单日分析，可扩展周期
GROUP BY
    sku_id, product_name, category_id, category_name
ORDER BY demand_supply_ratio DESC
;

SELECT
    *
FROM jtp_gd03_warehouse.ads_Calculation_demand_supply_ratio;

-- 指标 3：价格力商品榜单

-- 价格力商品榜单（含排名、价格力星级、交易转化指标）
DROP TABLE IF EXISTS jtp_gd03_warehouse.ads_list_price_driven_products;
CREATE TABLE jtp_gd03_warehouse.ads_list_price_driven_products (
    `sku_id` bigint COMMENT 'skuid',
    `product_name` VARCHAR(100) COMMENT '商品名称',
    `category_id` bigint COMMENT '商品名称',
    `category_name` VARCHAR(100) COMMENT '类别id',
    `ranking` bigint COMMENT '排名',
    `price_strength_level` VARCHAR(100) COMMENT '全天价格力量级',
    `max_original_price` DECIMAL(16,2) COMMENT '商品价格',
    `visitor_cnt` bigint COMMENT '访客数',
    `pay_buyer_cnt` bigint COMMENT '支付买家数',
    `pay_item_cnt` BIGINT COMMENT '支付件数',
    `pay_conversion_rate` DECIMAL(16,2) COMMENT '支付转化率'
)    ENGINE = OLAP
    DUPLICATE KEY (sku_id)
COMMENT '客群分析'
DISTRIBUTED BY HASH(sku_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);

INSERT INTO jtp_gd03_warehouse.ads_list_price_driven_products
WITH price_strength AS (
    SELECT
        sku_id,
        product_name,
        category_id,
        category_name,
        -- 价格力核心指标
        MAX(original_price) AS max_original_price,  -- 商品原价
        AVG(item_price) AS avg_trans_price,        -- 成交均价
        -- 交易转化指标
        COUNT(DISTINCT if(status = 'paid',user_id,null)) AS pay_buyer_cnt,  -- 支付买家数
        COUNT(DISTINCT if(action_type IN ('click', 'view'),user_id,null)) AS visitor_cnt,  -- 访客数
        SUM(if(status = 'paid',buy_num,0)) AS pay_item_cnt,  -- 支付件数
        -- 价格波动幅度（原价与成交价对比）
        ROUND(
                    (AVG(item_price) - MAX(original_price))
                    / NULLIF(MAX(original_price), 0), 4
            ) AS price_fluctuation_rate,
        -- 价格力星级（模拟分段逻辑，需业务校准）
        CASE
            WHEN AVG(item_price) >= MAX(original_price) * 1.2 THEN '5星'
            WHEN AVG(item_price) >= MAX(original_price) * 1.1 THEN '4星'
            WHEN AVG(item_price) >= MAX(original_price) * 0.9 THEN '3星'
            WHEN AVG(item_price) >= MAX(original_price) * 0.8 THEN '2星'
            ELSE '1星'
            END AS price_strength_level
    FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
    WHERE dt BETWEEN '2025-08-01' AND '2025-08-31'  -- 分析周期
    GROUP BY
        sku_id, product_name, category_id, category_name
)

-- 生成价格力商品榜单
SELECT
    sku_id,
    product_name,
    category_id,
    category_name,
    -- 排名（按价格力综合得分降序，得分需业务定义，此处用价格波动幅度+转化加权）
    ROW_NUMBER() OVER (ORDER BY
        (price_fluctuation_rate * 0.4 +
         CASE WHEN visitor_cnt = 0 THEN 0
              ELSE pay_buyer_cnt / visitor_cnt
             END * 0.6) DESC
        ) AS ranking,
    price_strength_level,
    MAX(max_original_price),  -- 取原价代表商品价格
    visitor_cnt,
    pay_buyer_cnt,
    pay_item_cnt,
    -- 支付转化率
    ROUND(
            CASE WHEN visitor_cnt = 0 THEN 0
                 ELSE pay_buyer_cnt / visitor_cnt
                END, 4
        ) AS pay_conversion_rate
FROM price_strength
GROUP BY
    sku_id, product_name, category_id, category_name,
    price_strength_level, pay_buyer_cnt, visitor_cnt, pay_item_cnt, price_fluctuation_rate
ORDER BY ranking LIMIT 100
;  -- 取Top100榜单

SELECT
    *
FROM jtp_gd03_warehouse.ads_list_price_driven_products;

