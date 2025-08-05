CREATE DATABASE IF NOT EXISTS jtp_oms_warehouse location
    'hdfs://node101:8020/warehouse/jtp_oms_warehouse';

USE jtp_oms_warehouse;

-- todo 业务过程：用户领取优惠卷
-- 1-DWD事实表：优惠卷领取表
DROP TABLE IF EXISTS jtp_oms_warehouse.dwd_coupon_get_incr;
CREATE EXTERNAL TABLE IF NOT EXISTS jtp_oms_warehouse.dwd_coupon_get_incr
(
    id          STRING COMMENT '主键',
    coupon_id   STRING COMMENT '优惠卷ID',
    member_id   STRING COMMENT '会员ID',
    get_type    STRING COMMENT '获取类型：0->后台赠送；1->主动获取',
    create_time STRING COMMENT '优惠卷领取日期时间'
) COMMENT '优惠卷领取事务事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY')
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_warehouse/dwd_coupon_get_incr';

-- 1-DWD事实表：优惠卷领取表
-- todo 首日数据加载：ODS层首日获取历史数据，包含很多天数据，
--     所以使用动态分区将数据写入DWD层各个分区表中（每个分区存储当天领取数据）
SET hive.exec.dynamic.partition=true; -- 启动动态分区功能
SET hive.exec.dynamic.partition.mode=nonstrict; -- 动态分区采用非严格模式
INSERT OVERWRITE TABLE jtp_oms_warehouse.dwd_coupon_get_incr PARTITION (dt)
SELECT id
     , coupon_id
     , member_id
     , get_type
     , create_time
     , date_format(create_time, 'yyyy-MM-dd') AS dt
FROM jtp_oms_warehouse.ods_oms_coupon_use_incr
WHERE dt = '2024-12-31'
;

SHOW PARTITIONS jtp_oms_warehouse.dwd_coupon_get_incr;


-- todo 每日数据加载
INSERT OVERWRITE TABLE jtp_oms_warehouse.dwd_coupon_get_incr PARTITION (dt = '2025-01-01')
SELECT id
     , coupon_id
     , member_id
     , get_type
     , create_time
FROM jtp_oms_warehouse.ods_oms_coupon_use_incr
WHERE dt = '2025-01-01'
  -- 确保仅仅获取今日领取优惠卷数据，排除今日使用优惠卷数据
  AND date_format(create_time, 'yyyy-MM-dd') = '2025-01-01'
;
