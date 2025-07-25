-- 创建数据库（如果不存在）并选择它
CREATE DATABASE IF NOT EXISTS jtp_sgcx CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE jtp_sgcx;

-- 设置外键检查为0，方便导入大量数据时避免外键约束报错，导入完成后再设回1
SET FOREIGN_KEY_CHECKS = 0;

-- songgou_user_info (用户表)
DROP TABLE IF EXISTS `songgou_user_info`;
CREATE TABLE `songgou_user_info`
(
    `user_id`          VARCHAR(255) PRIMARY KEY COMMENT '用户ID',
    `register_time`    DATETIME COMMENT '注册时间',
    `register_city_id` INT COMMENT '注册城市ID',
    `gender`           TINYINT(1) COMMENT '性别(0未知/1男/2女)',
    `age_range`        INT COMMENT '年龄段(1-6)',
    `is_certified`     TINYINT(1) COMMENT '是否实名认证',
    `last_active_date` DATE COMMENT '最后活跃日期',
    `user_level`       INT COMMENT '用户等级(1-5)'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '用户表';



select user_id, register_time, register_city_id, gender, age_range, is_certified, last_active_date, user_level
from songgou_user_info;

-- songguo_bike_info (车辆表)
DROP TABLE IF EXISTS `songguo_bike_info`;
CREATE TABLE `songguo_bike_info`
(
    `bike_id`          VARCHAR(255) PRIMARY KEY COMMENT '车辆ID',
    `bike_type`        INT COMMENT '车型(1标准/2豪华)',
    `manufacture_date` DATE COMMENT '生产日期',
    `battery_type`     VARCHAR(255) COMMENT '电池类型',
    `initial_city_id`  INT COMMENT '初始投放城市',
    `is_shared`        TINYINT(1) COMMENT '是否共享车辆'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '车辆表';

select bike_id, bike_type, manufacture_date, battery_type, initial_city_id, is_shared
from songguo_bike_info;

-- songguo_city_info (城市表)
DROP TABLE IF EXISTS `songguo_city_info`;
CREATE TABLE `songguo_city_info`
(
    `city_id`       INT PRIMARY KEY COMMENT '城市ID',
    `city_name`     VARCHAR(255) COMMENT '城市名称',
    `province_id`   INT COMMENT '省份ID',
    `province_name` VARCHAR(255) COMMENT '省份名称',
    `region_id`     INT COMMENT '大区ID(华北/华东等)',
    `is_hot_city`   TINYINT(1) COMMENT '是否热门城市'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '城市表';

select city_id, city_name, province_id, province_name, region_id, is_hot_city
from songguo_city_info;

-- songguo_bike_status_type (状态表)
DROP TABLE IF EXISTS `songguo_bike_status_type`;
CREATE TABLE `songguo_bike_status_type`
(
    `status_id`      INT PRIMARY KEY COMMENT '状态ID',
    `status_name`    VARCHAR(255) COMMENT '状态名称(可用/维修/缺电)',
    `is_operational` TINYINT(1) COMMENT '是否可运营',
    `priority`       INT COMMENT '处理优先级'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '状态表';


select status_id, status_name, is_operational, priority
from songguo_bike_status_type;
-- songguo_campaign (活动表)
DROP TABLE IF EXISTS `songguo_campaign`;
CREATE TABLE `songguo_campaign`
(
    `campaign_id`   VARCHAR(255) PRIMARY KEY COMMENT '活动ID',
    `campaign_name` VARCHAR(255) COMMENT '活动名称',
    `start_time`    DATETIME COMMENT '开始时间',
    `end_time`      DATETIME COMMENT '结束时间',
    `campaign_type` INT COMMENT '活动类型(1折扣券/2满减)',
    `target_city_ids` JSON COMMENT '目标城市ID列表'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '活动表';

select campaign_id, campaign_name, start_time, end_time, campaign_type, target_city_ids
from songguo_campaign;

-- songgou_order_info (骑行订单表)
DROP TABLE IF EXISTS `songgou_order_info`;
CREATE TABLE `songgou_order_info`
(
    `order_id`      VARCHAR(255) PRIMARY KEY COMMENT '订单ID',
    `user_id`       VARCHAR(255) COMMENT '用户ID',
    `bike_id`       VARCHAR(255) COMMENT '车辆ID',
    `start_time`    DATETIME COMMENT '开始时间',
    `end_time`      DATETIME COMMENT '结束时间',
    `start_lng`     DOUBLE COMMENT '起点经度',
    `start_lat`     DOUBLE COMMENT '起点纬度',
    `end_lng`       DOUBLE COMMENT '终点经度',
    `end_lat`       DOUBLE COMMENT '终点纬度',
    `distance`      DOUBLE COMMENT '骑行距离(公里)',
    `duration`      INT COMMENT '骑行时长(秒)',
    `base_fee`      DECIMAL(10, 2) COMMENT '基础费用',
    `extra_fee`     DECIMAL(10, 2) COMMENT '附加费用',
    `total_fee`     DECIMAL(10, 2) COMMENT '总费用',
    `coupon_amount` DECIMAL(10, 2) COMMENT '优惠券抵扣',
    `actual_pay`    DECIMAL(10, 2) COMMENT '实付金额',
    `pay_type`      TINYINT(1) COMMENT '支付方式(1-微信,2-支付宝)',
    `pay_status`    TINYINT(1) COMMENT '支付状态(0-未支付,1-已支付,2-已退款)',
    `city_id`       INT COMMENT '城市ID',
    `region_id`     INT COMMENT '区域ID',
    `is_night_ride` TINYINT(1) COMMENT '是否夜间骑行',
    `is_first_ride` TINYINT(1) COMMENT '是否首次骑行'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '骑行订单表';

select order_id, user_id, bike_id, start_time, end_time, start_lng, start_lat, end_lng, end_lat, distance, duration, base_fee, extra_fee, total_fee, coupon_amount, actual_pay, pay_type, pay_status, city_id, region_id, is_night_ride, is_first_ride
from songgou_order_info;

-- songguo_bike_status (车辆状态表) - 注意：原表名为songguo__bike_status，这里改为单下划线
DROP TABLE IF EXISTS `songguo_bike_status`;
CREATE TABLE `songguo_bike_status`
(
    `bike_id`        VARCHAR(255) PRIMARY KEY COMMENT '车辆ID',
    `city_id`        INT COMMENT '城市ID',
    `district_id`    INT COMMENT '区域ID',
    `battery_level`  INT COMMENT '电池电量(0-100)',
    `is_rented`      TINYINT(1) COMMENT '是否被租用',
    `is_maintenance` TINYINT(1) COMMENT '是否维修中',
    `is_damaged`     TINYINT(1) COMMENT '是否损坏',
    `gps_lng`        DOUBLE COMMENT '经度',
    `gps_lat`        DOUBLE COMMENT '纬度',
    `status_time`    DATETIME COMMENT '状态时间'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '车辆状态表';

select bike_id, city_id, district_id, battery_level, is_rented, is_maintenance, is_damaged, gps_lng, gps_lat, status_time
from songguo_bike_status;

-- songguo_bike_operation (运维记录表)
DROP TABLE IF EXISTS `songguo_bike_operation`;
CREATE TABLE `songguo_bike_operation`
(
    `operation_id`   VARCHAR(255) PRIMARY KEY COMMENT '运维记录ID',
    `bike_id`        VARCHAR(255) COMMENT '车辆ID',
    `operator_id`    VARCHAR(255) COMMENT '运维人员ID', -- 假设运维人员ID也是用户ID
    `operation_type` TINYINT(1) COMMENT '操作类型(1-投放,2-回收,3-维修,4-换电)',
    `operation_time` DATETIME COMMENT '操作时间',
    `before_battery` INT COMMENT '操作前电量',
    `after_battery`  INT COMMENT '操作后电量',
    `before_status`  TINYINT(1) COMMENT '操作前状态',
    `after_status`   TINYINT(1) COMMENT '操作后状态',
    `city_id`        INT COMMENT '城市ID',
    `district_id`    INT COMMENT '区域ID'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '运维记录表';

select operation_id, bike_id, operator_id, operation_type, operation_time, before_battery, after_battery, before_status, after_status, city_id, district_id
from songguo_bike_operation;

-- songguo_transaction_record (交易表)
DROP TABLE IF EXISTS `songguo_transaction_record`;
CREATE TABLE `songguo_transaction_record`
(
    `transaction_id`     VARCHAR(255) PRIMARY KEY COMMENT '交易ID',
    `order_id`           VARCHAR(255) COMMENT '关联订单ID',
    `user_id`            VARCHAR(255) COMMENT '用户ID',
    `transaction_type`   TINYINT(1) COMMENT '交易类型(1-支付,2-退款)',
    `transaction_amount` DECIMAL(12, 2) COMMENT '交易金额',
    `transaction_time`   DATETIME COMMENT '交易时间',
    `payment_channel`    TINYINT(1) COMMENT '支付渠道(1-微信,2-支付宝)',
    `payment_status`     TINYINT(1) COMMENT '支付状态',
    `merchant_id`        VARCHAR(255) COMMENT '商户ID',
    `city_id`            INT COMMENT '城市ID'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '交易表';

select transaction_id, order_id, user_id, transaction_type, transaction_amount, transaction_time, payment_channel, payment_status, merchant_id, city_id
from songguo_transaction_record;
-- songguo_user_behavior (用户行为表)
DROP TABLE IF EXISTS `songguo_user_behavior`;
CREATE TABLE `songguo_user_behavior`
(
    `user_id`       VARCHAR(255) COMMENT '用户ID',
    `behavior_type` TINYINT(1) COMMENT '行为类型(1-注册,2-登录,3-开锁,4-支付,5-投诉)',
    `behavior_time` DATETIME COMMENT '行为时间',
    `device_id`     VARCHAR(255) COMMENT '设备ID',
    `app_version`   VARCHAR(50) COMMENT 'APP版本',
    `os_type`       TINYINT(1) COMMENT '操作系统(1-iOS,2-Android)',
    `ip`            VARCHAR(50) COMMENT 'IP地址',
    `city_id`       INT COMMENT '城市ID'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '用户行为表';

select user_id, behavior_type, behavior_time, device_id, app_version, os_type, ip, city_id
from songguo_user_behavior;

-- 重新开启外键检查（导入完成后执行）
-- SET FOREIGN_KEY_CHECKS = 1;