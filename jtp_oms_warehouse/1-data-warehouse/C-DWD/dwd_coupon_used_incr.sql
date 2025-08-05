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

