CREATE DATABASE IF NOT EXISTS songguo_warehouse
    location "hdfs://node101/user/spark/warehouse/songguo_warehouse";
USE songguo_warehouse;

DROP TABLE IF EXISTS songguo_warehouse.ads_day_add_users;
CREATE TABLE IF NOT EXISTS songguo_warehouse.ads_day_add_users
(
    report_date DATE COMMENT '日期分区',
    new_users BIGINT
) COMMENT '新增用户数'
    PARTITIONED BY (dt STRING COMMENT '订单日期分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/ads_day_add_users';
;

-- 查询新增用户数
INSERT INTO songguo_warehouse.ads_day_add_users PARTITION (dt = '2025-07-26')
SELECT
    CAST('2025-07-26' AS DATE) AS report_date, -- 统计日期
    COUNT(DISTINCT user_id) AS new_user_count -- 统计唯一用户ID
FROM
    songguo_warehouse.songgou_user_info -- ODS 层的用户表
WHERE
        CAST(register_time AS DATE) = '2025-07-26' -- 注册日期等于统计日期
  AND dt = '2025-07-26';
-- 查询结果
SELECT
    report_date,
    new_users
FROM
    songguo_warehouse.ads_day_add_users
;

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
        CAST('2025-07-26' AS DATE) AS report_date -- 统计日期
         , count(distinct user_id) AS day_active_users -- 日活跃用户数
    FROM songguo_warehouse.dwd_fact_user_behavior
    WHERE
            CAST(behavior_time AS DATE) = '2025-07-26'
      AND behavior_type >= 4
      AND dt = '2025-07-26'
    )
    , week_active_users AS (
    -- week
    SELECT
        CAST('2025-07-26' AS DATE) AS report_date -- 统计日期
         , count(distinct user_id) AS week_active_users -- 日活跃用户数
    FROM songguo_warehouse.dwd_fact_user_behavior
    WHERE
            CAST(behavior_time AS DATE) = date_sub('2025-07-26', 6)
      AND behavior_type >= 4
      AND dt BETWEEN DATE_SUB('2025-07-26', 6) AND '2025-07-26' -- 分区字段日期当天至前7天
    )
    , month_active_users AS (
    -- month
    SELECT
        CAST('2025-07-26' AS DATE) AS report_date -- 统计日期
         , count(distinct user_id) AS month_active_users -- 日活跃用户数
    FROM songguo_warehouse.dwd_fact_user_behavior
    WHERE
            CAST(behavior_time AS DATE) = date_sub('2025-07-26', 7)
      AND behavior_type >= 4
      AND dt BETWEEN date_sub('2025-07-26', 29) AND '2025-07-26' -- 分区字段日期减29天
    )
INSERT INTO songguo_warehouse.ads_day_week_month_active_users PARTITION (dt = '2025-07-26')
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









