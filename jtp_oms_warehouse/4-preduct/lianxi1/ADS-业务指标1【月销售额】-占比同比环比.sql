

-- ===============================================================================================================
-- todo 离线数仓分析时，三板斧操作
--      列裁剪（SELECT语句指定列名称、表数据列式存储）、分区裁剪（分区过滤数据，针对分区表）、谓词下推（predict pushdown：ppd）
--              |                                       |                           |
--          SELECT 不允许写 *                       WHERE 分区字段 过滤              WHERE 过滤条件
-- ===============================================================================================================


/*
    1. 计算日、月、年销售额、订单量
    2. 计算2024年月销售额占比（销售总额本年占比）、同比（销售总额同比去年同期）和环比（销售总额同比上期）
*/

SELECT * FROM jtp_oms_dwd.dwd_oms_order_incr WHERE dt = '2024-01-01' ;

-- =====================================================================================
-- todo 1. 计算日、月、年销售额、订单量
--  考察知识点：聚合开窗函数，使用sum() over()，其中月度统计按照月份分组、年度统计按照年份分组
-- =====================================================================================

CREATE TABLE IF NOT EXISTS jtp_oms_dws.dws_oms_sales_summary (
    dt STRING COMMENT '日期，可以是天、月或年',
    total_amount DOUBLE COMMENT '总销售额',
    order_count BIGINT COMMENT '总订单量',
    period_type STRING COMMENT '周期类型：日, 月, 年'
)
    COMMENT '日、月、年销售汇总表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 1. 日，月，年销售额
WITH T1 AS (
    SELECT
        dt
         -- 日销售额，
         , SUM(total_amount) AS total_amount
         -- 日订单量
         , count(DISTINCT id) AS order_count
         , '日' AS period_type
    FROM jtp_oms_dwd.dwd_oms_order_incr
    GROUP BY dt
),
T2 AS (
    -- 2.月销售额
    SELECT
        substring(dt, 1, 7) AS dt
         -- 月销售额
         , SUM(total_amount) AS total_amount
         -- 月订单量
         , count(DISTINCT id) AS order_count
         , '月' AS period_type
    FROM jtp_oms_dwd.dwd_oms_order_incr
    GROUP BY substring(dt, 1, 7)
)

, T3 AS (
    -- 3.年销售额
    SELECT
        substring(dt, 1, 4) AS dt
         -- 年销售额
         , SUM(total_amount) AS total_amount
         -- 年订单量
         , count(DISTINCT id) AS order_count
         , '年' AS period_type
    FROM jtp_oms_dwd.dwd_oms_order_incr
    GROUP BY substring(dt, 1, 4)
)
SELECT * FROM T1
UNION ALL
SELECT * FROM T2
UNION ALL
SELECT * FROM T3
ORDER BY total_amount, period_type
;





-- =====================================================================================
-- todo 2. 计算2024年【月销售额】
--      占比（销售总额本年占比）、同比（销售总额同比去年同期）和环比（销售总额同比上期）
--      1). 占比 = month_total_amount / year_total_amount
--      2). 同比 = (当月销售额 - 去年同月销售额) / 去年同月销售额
--      3). 环比 = (当月销售额 - 今年上月销售额) / 今年上月销售额
--  考核知识点：分析开窗函数，使用lag向上取值，用于计算同步和环比；考核概念：占比、同比、环比；
-- =====================================================================================

-- 计算同比，占比，环比
select
    dt_month,
    month_total_amount,
    year_total_amount,
    last_month_total_amount,
    year_month_order_amount,
    -- 占比
    round(month_total_amount / year_total_amount, 4) AS month_amount_ratio,
    -- 同比
    round((month_total_amount - year_month_order_amount) / year_month_order_amount, 4) AS year_amount_ratio,
    -- 环比
    round((month_total_amount - last_month_total_amount) / last_month_total_amount, 4) AS last_month_amount_ratio,
FROM (
         -- s2 获取上月的销售额和去年同月销售额
         SELECT dt_month,
                month_total_amount,
                year_total_amount,
                -- 上月销售额
                lag(month_total_amount, 1, null) OVER (ORDER BY dt_month)  AS last_month_total_amount,
                -- 去年同月销售额
                lag(month_total_amount, 12, null) over (ORDER BY dt_month) as year_month_order_amount
         from (
              -- s1 过滤
             SELECT
        date_format(dt, 'yyyy-MM'),
        month_total_count, year_total_amount
         FROM jtp_oms_dwd.dws_order_day_month_report
         group by date_format(dt, 'yyyy-MM'), month_total_count, year_total_amount
         )T1
    )T2
WHERE substr(dt_month, 1 , 4) = '2024'



-- =====================================================================================
-- todo 3. 计算2024-12月份【日销售额】
--      占比（销售总额本月占比）、同比（销售总额同比上月同期）和环比（销售总额同比上期）
--      1). 占比 = day_total_amount / month_total_amount
--      2). 同比 = (当日销售额 - 上月同日销售额) / 上月同日销售额
--      3). 环比 = (当日销售额 - 本月昨日销售额) / 本月昨日销售额
-- =====================================================================================



