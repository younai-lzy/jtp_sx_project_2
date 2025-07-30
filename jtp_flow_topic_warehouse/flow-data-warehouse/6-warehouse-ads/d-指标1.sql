-- todo 1.每日城市订单统计：各城市每日订单量、GMV、用户数、平均骑行时长

DROP TABLE IF EXISTS ads_city_gmv;
CREATE TABLE IF NOT EXISTS ads_city_gmv
AS
SELECT
    city_id -- 城市ID
    ,city_name -- 城市名称
    ,count(DISTINCT order_id) AS order_count  -- 订单数
    ,count(DISTINCT user_id) AS user_count -- 用户数
    ,sum(actual_pay) AS gmv --总金额
    ,avg(duration) AS avg_duration -- 平均时长
    ,sum(distance) AS avg_distance --总距离
FROM songguo_warehouse.dwd_fact_order_info
GROUP BY dt,city_id,city_name;

SELECT
    *
FROM ads_city_gmv;

-- todo 订单完成率：统计每日订单完成率（完成订单数/创建订单数）
DROP TABLE IF EXISTS ads_date_finish;
CREATE TABLE IF NOT EXISTS ads_date_finish
AS
SELECT
    dt -- 日期
    ,count(IF(end_time = null ,null,1)) AS completed_order -- 完成订单数
    ,count(order_id) AS total_orders -- 创建订单数
    ,count(IF(end_time = null ,null,1))/count(order_id) AS completed_order_rate -- 完成订单率
FROM songguo_warehouse.dwd_fact_order_info
GROUP BY dt;

SELECT
    *
FROM ads_date_finish;





