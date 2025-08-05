-- 创建数据库
CREATE DATABASE IF NOT EXISTS jtp_oms_warehouse location
    'hdfs://node101:8020/warehouse/jtp_oms_warehouse';

USE jtp_oms_warehouse;

/*
DIM层维度表数据来源于ODS层原始同步数据
*/
-- 创建表：用户维度拉链表
DROP TABLE IF EXISTS jtp_oms_warehouse.dim_oms_coupon_full;
CREATE EXTERNAL TABLE IF NOT EXISTS jtp_oms_warehouse.dim_oms_coupon_full
(
    id               STRING COMMENT '主键ID',
    type             STRING COMMENT '优惠卷类型；0->全场赠券；1->会员赠券；2->购物赠券；3->注册赠券',
    name             STRING COMMENT '优惠卷名称',
    platform         STRING COMMENT '使用平台：0->全部；1->移动；2->PC',
    count            STRING COMMENT '数量',
    amount           STRING COMMENT '金额',
    per_limit        STRING COMMENT '每人限领张数',
    min_point        STRING COMMENT '使用门槛；0表示无门槛',
    start_time       STRING COMMENT '优惠卷开始时间',
    end_time         STRING COMMENT '优惠卷结束时间',
    use_type         STRING COMMENT '使用类型：0->全场通用；1->指定分类；2->指定商品',
    publish_count    STRING COMMENT '发行数量',
    use_count        STRING COMMENT '已使用数量',
    receive_count    STRING COMMENT '领取数量',
    enable_time      STRING COMMENT '可以领取的日期',
    code             STRING COMMENT '优惠码',
    member_level     STRING COMMENT '可领取的会员类型：0->无限时'
) COMMENT '优惠卷信息表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY')
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_warehouse/dim_oms_coupon_full'
;

SHOW PARTITIONS jtp_oms_warehouse.dim_oms_coupon_full ;

/*
    优惠卷信息数据，采用每日全量快照表方式进行存储
*/
INSERT OVERWRITE TABLE jtp_oms_warehouse.dim_oms_coupon_full PARTITION (dt = '2025-01-01')
SELECT id,
       type,
       name,
       platform,
       count,
       amount,
       per_limit,
       min_point,
       start_time,
       end_time,
       use_type,
       publish_count,
       use_count,
       receive_count,
       enable_time,
       code,
       member_level
FROM jtp_oms_warehouse.ods_oms_coupon_full
WHERE dt = '2025-01-01'
;

select *
from dim_oms_coupon_full;