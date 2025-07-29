#!/bin/bash

if [ -n "$1" ] ; then
  data_date=$1
else
  data_date=`date -d "-1 days" +%F`
fi

ADS_CITY_GMV_SQL="
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
"

ADS_DAY_WEEK_MONTH_ACTIVE_USERS_SQL="
-- 日周月活跃用户数(DAU/WAU/MAU)
DROP TABLE IF EXISTS songguo_warehouse.ads_day_week_month_active_users;
CREATE TABLE IF NOT EXISTS songguo_warehouse.ads_day_week_month_active_users
(
    report_date DATE COMMENT '统计日期'
    , day_active_users BIGINT COMMENT '日活跃用户数'-- 日活跃用户数
    , week_active_users BIGINT COMMENT '周活跃用户数' -- 周活跃用户数
    , month_active_users BIGINT COMMENT '月活跃用户数'-- 月活跃用户数
) COMMENT '活跃用户数'
    PARTITIONED BY (dt STRING COMMENT '订单日期分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/ads_day_week_month_active_users'
;
-- 查询结果

WITH day_active_users AS (
    -- day
    SELECT
        CAST('${data_date}' AS DATE) AS report_date -- 统计日期
         , count(distinct user_id) AS day_active_users -- 日活跃用户数
    FROM songguo_warehouse.dwd_fact_user_behavior
    WHERE
            CAST(behavior_time AS DATE) = '${data_date}'
      AND behavior_type >= 4
      AND dt = '${data_date}'
    )
    , week_active_users AS (
    -- week
    SELECT
        CAST('${data_date}' AS DATE) AS report_date -- 统计日期
         , count(distinct user_id) AS week_active_users -- 日活跃用户数
    FROM songguo_warehouse.dwd_fact_user_behavior
    WHERE
            CAST(behavior_time AS DATE) = date_sub('${data_date}', 6)
      AND behavior_type >= 4
      AND dt BETWEEN DATE_SUB('${data_date}', 6) AND '${data_date}' -- 分区字段日期当天至前7天
    )
    , month_active_users AS (
    -- month
    SELECT
        CAST('${data_date}' AS DATE) AS report_date -- 统计日期
         , count(distinct user_id) AS month_active_users -- 日活跃用户数
    FROM songguo_warehouse.dwd_fact_user_behavior
    WHERE
            CAST(behavior_time AS DATE) = date_sub('${data_date}', 7)
      AND behavior_type >= 4
      AND dt BETWEEN date_sub('${data_date}', 29) AND '${data_date}' -- 分区字段日期减29天
    )
INSERT INTO songguo_warehouse.ads_day_week_month_active_users PARTITION (dt = '${data_date}')
SELECT dau.report_date
     , dau.day_active_users
     , mau.month_active_users
     , wau.week_active_users
FROM day_active_users AS dau,
     month_active_users AS mau,
     week_active_users AS wau
;

-- 查询结果
SELECT report_date, day_active_users, week_active_users, month_active_users, dt
FROM songguo_warehouse.ads_day_week_month_active_users
;
"

ADS_DATE_FINISH_SQL="
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
"

ADS_USER_COUNT_SQL="
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
"

ADS_PAY_TYPE_SQL="
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
"

ADS_CITY_AMOUNT_SQL="
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
"

ADS_CORE_TARGET_SQL="
DROP TABLE IF EXISTS ads_core_target;
CREATE TABLE IF NOT EXISTS ads_core_target
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

SELECT * FROM ads_core_target;
"

/opt/module/spark/bin/beeline -u jdbc:hive2://node101:10000 -n bwie -e "${ADS_CITY_GMV_SQL}${ADS_DAY_WEEK_MONTH_ACTIVE_USERS_SQL}
${ADS_DATE_FINISH_SQL}${ADS_USER_COUNT_SQL}${ADS_PAY_TYPE_SQL}${ADS_CITY_AMOUNT_SQL}${ADS_CORE_TARGET_SQL}"








