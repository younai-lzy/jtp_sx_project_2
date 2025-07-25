CREATE DATABASE IF NOT EXISTS songguo_warehouse
    location "hdfs://node101/user/spark/warehouse/songguo_warehouse";
USE songguo_warehouse;


--todo 3.高频用户识别：标记过去30天订单量>10次的用户
DROP TABLE IF EXISTS ads_user_count;
CREATE TABLE IF NOT EXISTS ads_user_count
AS
WITH t1 AS(
    SELECT
        user_id,
        COUNT(DISTINCT order_id) AS order_count_30d
    FROM songguo_warehouse.dwd_fact_order_info
    -- 过去30天分区
    WHERE dt>=date_sub(current_date(),30)
    --只统计已支付订单
    AND pay_status=1
    GROUP BY user_id
)SELECT
     a.user_id,
     a.order_count_30d,
     CASE
         WHEN a.order_count_30d>10 THEN '高频用户'
         ELSE '普通用户'
     END AS user_level
FROM t1 a
ORDER BY a.order_count_30d DESC ;

SELECT * FROM ads_user_count;

--todo 4.指标4：财务指标
--todo 1.每日营收统计：按支付方式统计营收和优惠券抵扣
DROP TABLE IF EXISTS ads_pay_type;
CREATE TABLE IF NOT EXISTS ads_pay_type
AS
SELECT
    dt ,
    CASE
        WHEN pay_type=1 THEN '微信'
        WHEN pay_type=2 THEN '支付宝'
        ELSE '其他'
    END AS pay_type,
    round(SUM(actual_pay),2) AS total_revenue, --总银收额
    round(SUM(coupon_amount),2) AS totoal_coupon_deduction,--优惠券总金额
    round(sum(actual_pay-coupon_amount),2) AS gross_revenue --所有银收
FROM songguo_warehouse.dwd_fact_order_info
WHERE pay_status=1
GROUP BY dt,
         CASE
             WHEN pay_type=1 THEN '微信'
             WHEN pay_type=2 THEN '支付宝'
             ELSE '其他'
         END
ORDER BY dt,pay_type;

SELECT * FROM ads_pay_type;


--todo 2城市毛利率分析：计算各城市毛利（营收-成本，假设成本为每单固定1元）
DROP TABLE IF EXISTS ads_city_amount;
CREATE TABLE IF NOT EXISTS ads_city_amount
AS
SELECT
    city_id,
    city_name,
    --总银收额
    round(sum(actual_pay),2) AS revenue,
    --总成本
    count(order_id) *1 AS cost,
    --毛利=营收-成本
    round(sum(actual_pay)-count(order_id) *1,2) AS profit
FROM songguo_warehouse.dwd_fact_order_info
WHERE pay_status=1 --统计已支付订单
GROUP BY city_id,city_name
ORDER BY city_id;

SELECT * FROM ads_city_amount;



--todo 5.整合核心指标供BI展示
set spark.sql.adaptive.enabled=true;

DROP TABLE IF EXISTS ads_BI;
CREATE TABLE IF NOT EXISTS ads_BI
AS
SELECT
    t2.dt AS stat_date
     --总订单数
     ,count(t1.order_id) AS total_orders
     --总活跃用户数
     ,count(DISTINCT t1.user_id) AS active_users
     --总金额
     ,round(sum(actual_pay),2) AS gmv
     --可用车数量，电量大于20并且没有租出去
     ,sum(if(t2.battery_level>20 AND is_rented=false,1,0)) AS available_bikes
FROM songguo_warehouse.dwd_fact_order_info t1
         left join songguo_warehouse.dwd_fact_bike_status t2
                   ON t1.bike_id = t2.bike_id
                       AND t1.dt = t2.dt
GROUP BY t2.dt;

SELECT * FROM ads_BI;



