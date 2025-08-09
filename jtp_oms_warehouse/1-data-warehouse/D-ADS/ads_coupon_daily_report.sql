CREATE DATABASE IF NOT EXISTS jtp_oms_warehouse location
    'hdfs://node101:8020/warehouse/jtp_oms_warehouse';

USE jtp_oms_warehouse;

-- todo 优惠卷统计
/*
    日期          领取次数       使用次数        使用人数
    2024-12-01     35             5             5
    2024-12-02     19             1             1
*/
DROP TABLE IF EXISTS jtp_oms_warehouse.ads_coupon_daily_report;
CREATE EXTERNAL TABLE IF NOT EXISTS jtp_oms_warehouse.ads_coupon_daily_report
(
    dt                     STRING COMMENT '数据统计日期',
    coupon_get_count       BIGINT COMMENT '每日优惠卷领取次数',
    coupon_used_count      BIGINT COMMENT '每日优惠卷使用次数',
    coupon_used_user_count BIGINT COMMENT '每日优惠卷使用人数'
) COMMENT '每日所有优惠卷统计'
    STORED AS ORC
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_warehouse/ads_coupon_daily_report'
;


SELECT *
FROM jtp_oms_warehouse.ads_coupon_daily_report;

-- todo 首日同步历史数据计算
INSERT OVERWRITE TABLE jtp_oms_warehouse.ads_coupon_daily_report
SELECT nvl(t1.dt, t2.dt)                 AS dt
     , nvl(t1.coupon_get_count, 0)       AS coupon_get_count
     , nvl(t2.coupon_used_count, 0)      AS coupon_used_count
     , nvl(t2.coupon_used_user_count, 0) AS coupon_used_user_count
FROM (
         -- s1-优惠卷：领取
         SELECT dt
              , count(id) AS coupon_get_count
         FROM jtp_oms_warehouse.dwd_coupon_get_incr
         WHERE dt <= '2024-12-31'
         GROUP BY dt) t1
         FULL JOIN (
    -- s2-优惠卷：使用
    SELECT dt
         , count(DISTINCT member_id) AS coupon_used_count
         , count(id)                 AS coupon_used_user_count
    FROM jtp_oms_warehouse.dwd_coupon_used_incr
    WHERE dt <= '2024-12-31'
    GROUP BY dt) t2
                   ON t1.dt = t2.dt
;

-- 增量数据同步
INSERT OVERWRITE TABLE jtp_oms_warehouse.ads_coupon_daily_report
SELECT *
FROM jtp_oms_warehouse.ads_coupon_daily_report
UNION
SELECT t1.dt
     , t1.coupon_get_count
     , t2.coupon_used_count
     , t2.coupon_used_user_count
FROM (
         -- s1-优惠卷：领取
         SELECT '2025-01-01' AS dt
              , count(id)    AS coupon_get_count
         FROM jtp_oms_warehouse.dwd_coupon_get_incr
         WHERE dt = '2025-01-01') t1
         JOIN (
    -- s2-优惠卷：使用
    SELECT '2025-01-01'              AS dt
         , count(id)                 AS coupon_used_count
         , count(DISTINCT member_id) AS coupon_used_user_count
    FROM jtp_oms_warehouse.dwd_coupon_used_incr
    WHERE dt = '2025-01-01') t2
              ON t1.dt = t2.dt
;
