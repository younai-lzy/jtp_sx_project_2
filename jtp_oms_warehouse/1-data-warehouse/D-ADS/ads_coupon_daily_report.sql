CREATE DATABASE IF NOT EXISTS jtp_oms_warehouse location
    'hdfs://node101:8020/warehouse/jtp_oms_warehouse';

USE jtp_oms_warehouse;

-- todo 首日数据加载：ODS层首日获取历史数据，包含很多天数据，
--      所以使用动态分区将数据写入DWD层各个分区表中（每个分区存储当天使用数据）
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE jtp_oms_warehouse.dwd_coupon_used_incr PARTITION (dt)
SELECT
    id
     , coupon_id
     , member_id
     , get_type
     , create_time
     , use_time
     , order_id
     , date_format(use_time, 'yyyy-MM-dd') AS dt
FROM jtp_oms_warehouse.ods_oms_coupon_use_incr
WHERE dt = '2024-12-31'
  AND use_time IS NOT NULL
;

SHOW PARTITIONS jtp_oms_warehouse.dwd_coupon_used_incr ;


-- 每日数据加载
INSERT OVERWRITE TABLE jtp_oms_warehouse.dwd_coupon_used_incr PARTITION (dt = '2025-01-01')
SELECT
    id
     , coupon_id
     , member_id
     , get_type
     , create_time
     , use_time
     , order_id
FROM jtp_oms_warehouse.ods_oms_coupon_use_incr
WHERE dt = '2025-01-01'
  AND use_time IS NOT NULL
;

-- todo 首日同步历史数据计算
INSERT OVERWRITE TABLE jtp_oms_warehouse.ads_coupon_daily_report
SELECT
    nvl(t1.dt, t2.dt) AS dt
     , nvl(t1.coupon_get_count, 0) AS coupon_get_count
     , nvl(t2.coupon_used_count, 0) AS coupon_used_count
     , nvl(t2.coupon_used_user_count, 0) AS coupon_used_user_count
FROM (
         -- s1-优惠卷：领取
         SELECT
             dt
              , count(id) AS coupon_get_count
         FROM jtp_oms_warehouse.dwd_coupon_get_incr
         WHERE dt <= '2024-12-31'
         GROUP BY dt
     ) t1
         FULL JOIN (
    -- s2-优惠卷：使用
    SELECT
        dt
         , count(DISTINCT member_id) AS coupon_used_count
         , count(id) AS coupon_used_user_count
    FROM jtp_oms_warehouse.dwd_coupon_used_incr
    WHERE dt <= '2024-12-31'
    GROUP BY dt
) t2
                   ON t1.dt = t2.dt
;

-- 每日增量
INSERT OVERWRITE TABLE jtp_oms_warehouse.ads_coupon_daily_report
SELECT * FROM jtp_oms_warehouse.ads_coupon_daily_report
UNION
SELECT
    t1.dt
     , t1.coupon_get_count
     , t2.coupon_used_count
     , t2.coupon_used_user_count
FROM (
         -- s1-优惠卷：领取
         SELECT
             '2025-01-01' AS dt
              , count(id) AS coupon_get_count
         FROM jtp_oms_warehouse.dwd_coupon_get_incr
         WHERE dt = '2025-01-01'
     ) t1
         JOIN (
    -- s2-优惠卷：使用
    SELECT
        '2025-01-01' AS dt
         , count(id) AS coupon_used_count
         , count(DISTINCT member_id) AS coupon_used_user_count
    FROM jtp_oms_warehouse.dwd_coupon_used_incr
    WHERE dt = '2025-01-01'
) t2
              ON t1.dt = t2.dt
;
