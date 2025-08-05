CREATE DATABASE IF NOT EXISTS jtp_oms_warehouse location
    'hdfs://node101:8020/warehouse/jtp_oms_warehouse';

USE jtp_oms_warehouse;

-- 1-DWD事实表：订单信息表
DROP TABLE IF EXISTS jtp_oms_warehouse.dwd_oms_order_incr;
CREATE EXTERNAL TABLE IF NOT EXISTS jtp_oms_warehouse.dwd_oms_order_incr
(
    `id`                 BIGINT COMMENT '订单id',
    `member_id`          BIGINT COMMENT '会员ID',
    `coupon_id`          BIGINT COMMENT '优惠卷ID',
    `order_sn`           STRING COMMENT '订单编号',
    `create_time`        STRING COMMENT '提交时间',
    `total_amount`       DECIMAL(16, 2) COMMENT '订单总金额',
    `pay_amount`         DECIMAL(16, 2) COMMENT '应付金额（实际支付金额）',
    `freight_amount`     DECIMAL(16, 2) COMMENT '运费金额',
    `promotion_amount`   DECIMAL(16, 2) COMMENT '促销优化金额（促销价、满减、阶梯价）',
    `integration_amount` DECIMAL(16, 2) COMMENT '积分抵扣金额',
    `coupon_amount`      DECIMAL(16, 2) COMMENT '优惠券抵扣金额',
    `discount_amount`    DECIMAL(16, 2) COMMENT '管理员后台调整订单使用的折扣金额',
    `pay_type`           INT COMMENT '支付方式：0->未支付；1->支付宝；2->微信',
    `pay_type_name`      STRING COMMENT '支付方式：0->未支付；1->支付宝；2->微信',
    `source_type`        INT COMMENT '订单来源：0->PC订单；1->app订单',
    `source_type_name`   STRING COMMENT '订单来源：0->PC订单；1->app订单',
    `status`             INT COMMENT '订单状态：0->待付款；1->待发货；2->已发货；3->已完成；4->已关闭；5->无效订单',
    `order_type`         INT COMMENT '订单类型：0->正常订单；1->秒杀订单',
    `order_type_name`    STRING COMMENT '订单类型：0->正常订单；1->秒杀订单',
    `delivery_company`   STRING COMMENT '物流公司',
    `delivery_sn`        STRING COMMENT '物流单号',
    `integration`        INT COMMENT '可以获得的积分',
    `growth`             INT COMMENT '可以活动的成长值',
    `confirm_status`     INT COMMENT '确认收货状态：0->未确认；1->已确认',
    `delete_status`      INT COMMENT '删除状态：0->未删除；1->已删除',
    `use_integration`    INT COMMENT '下单时使用的积分',
    `payment_time`       STRING COMMENT '支付时间',
    `delivery_time`      STRING COMMENT '发货时间',
    `receive_time`       STRING COMMENT '确认收货时间',
    `comment_time`       STRING COMMENT '评价时间',
    `modify_time`        STRING COMMENT '修改时间'
) COMMENT '订单信息表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY')
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_warehouse/dwd_oms_order_incr'
;

/*
    交易订单事务事实表
        分区字段dt，表示下单日期，
        换句话说：dt=2024-12-25时，此分区下存储都是2024-12-25下单的数据
*/
-- todo step1 首日同步数据：2024-12-31

INSERT OVERWRITE TABLE jtp_oms_warehouse.dwd_oms_order_incr PARTITION (dt)
SELECT id
     , member_id
     , coupon_id
     , order_sn
     , create_time
     , total_amount
     , pay_amount
     , freight_amount
     , promotion_amount
     , INTegration_amount
     , coupon_amount
     , discount_amount
     , pay_type
     , CASE
           WHEN pay_type = 0 THEN '未支付'
           WHEN pay_type = 1 THEN '支付宝'
           WHEN pay_type = 2 THEN '微信'
           ELSE CAST(pay_type AS STRING)
    END                                                                                           AS pay_type_name
     , source_type
     , CASE
           WHEN source_type = 0 THEN 'PC订单'
           WHEN pay_type = 1 THEN 'app订单'
           ELSE CAST(source_type AS STRING)
    END                                                                                           AS source_type_name
     , status
     , order_type
     , IF(order_type = 0, '正常订单', if(order_type = 1, '秒杀订单', CAST(order_type AS STRING))) AS order_type_name
     , delivery_company
     , delivery_sn
     , INTegration
     , growth
     , confirm_status
     , delete_status
     , use_INTegration
     , payment_time
     , delivery_time
     , receive_time
     , comment_time
     , modify_time
     , date_format(create_time, 'yyyy-MM-dd')                                                     AS dt
FROM jtp_oms_warehouse.ods_oms_order_incr
WHERE dt = '2024-12-31'
;

SHOW PARTITIONS jtp_oms_warehouse.dwd_oms_order_incr;

SELECT *
FROM jtp_oms_warehouse.dwd_oms_order_incr
-- WHERE dt = '2024-12-25';



