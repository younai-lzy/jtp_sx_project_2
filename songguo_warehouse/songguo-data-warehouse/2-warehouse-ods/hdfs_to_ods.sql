CREATE DATABASE IF NOT EXISTS songguo_warehouse
    LOCATION "hdfs://node101:8020/user/spark/warehouse/songguo_warehouse";
USE songguo_warehouse;

--todo 1.核心业务数据
--订单表
DROP TABLE IF EXISTS songguo_warehouse.songgou_order_info;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.songgou_order_info
(
    order_id      STRING COMMENT '订单ID',
    user_id       STRING COMMENT '用户ID',
    bike_id       STRING COMMENT '车辆ID',
    start_time    TIMESTAMP COMMENT '开始时间',
    end_time      TIMESTAMP COMMENT '结束时间',
    start_lng     DOUBLE COMMENT '起点经度',
    start_lat     DOUBLE COMMENT '起点纬度',
    end_lng       DOUBLE COMMENT '终点经度',
    end_lat       DOUBLE COMMENT '终点纬度',
    distance      DOUBLE COMMENT '骑行距离(公里)',
    duration      INT COMMENT '骑行时长(秒)',
    base_fee      double COMMENT '基础费用',
    extra_fee     double COMMENT '附加费用',
    total_fee     double COMMENT '总费用',
    coupon_amount double COMMENT '优惠券抵扣',
    actual_pay    double COMMENT '实付金额',
    pay_type      TINYINT COMMENT '支付方式(1-微信,2-支付宝)',
    pay_Status    TINYINT COMMENT '支付状态(O-未支付,1-已支付,2-已退款)',
    city_id       INT COMMENT '城市ID',
    region_id     INT COMMENT '区域ID',
    is_night_ride BOOLEAN COMMENT '是否夜间骑行',
    is_first_ride BOOLEAN COMMENT '是否首次骑行'
) COMMENT '订单表'
    PARTITIONED BY (dt STRING COMMENT '订单日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songgou_order_info'
;


select order_id, user_id, bike_id, start_time, end_time, start_lng, start_lat, end_lng, end_lat, distance, duration, base_fee, extra_fee, total_fee, coupon_amount, actual_pay, pay_type, pay_Status, city_id, region_id, is_night_ride, is_first_ride, dt
from songgou_order_info;

--车辆状态表
DROP TABLE IF EXISTS songguo_warehouse.songguo_bike_status;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.songguo_bike_status
(
    bike_id        STRING COMMENT '车辆ID',
    city_id        INT COMMENT '城市ID',
    district_id    INT COMMENT '区域ID',
    battery_level  INT COMMENT '电池电量(0-100)',
    is_rented      BOOLEAN COMMENT '是否被租用',
    is_maintenance BOOLEAN COMMENT '是否维修中',
    is_damaged     BOOLEAN COMMENT '是否损坏',
    gps_lng        DOUBLE COMMENT '经度',
    gps_lat        DOUBLE COMMENT '纬度',
    status_time    TIMESTAMP COMMENT '状态时间'
) COMMENT '车辆状态表'
    PARTITIONED BY (dt STRING)
    STORED AS TEXTFILE ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_bike_status';

select *
from songguo_warehouse.songguo_bike_status;
--车辆运维表
DROP TABLE IF EXISTS songguo_warehouse.songguo_bike_operation;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.songguo_bike_operation
(
    operation_id   STRING COMMENT '运维记录ID',
    bike_id        STRING COMMENT '车辆ID',
    operator_id    STRING COMMENT '运维人员ID',
    operation_type TINYINT COMMENT '操作类型(1-投放,2-回收,3-维修,4-换电)',
    operation_time TIMESTAMP COMMENT '操作时间',
    before_battery INT COMMENT '操作前电量',
    after_battery  INT COMMENT '操作后电量',
    before_status  TINYINT COMMENT '操作前状态',
    after_status   TINYINT COMMENT '操作后状态',
    city_id        INT COMMENT '城市ID',
    district_id    INT COMMENT '区域ID'
) COMMENT '车辆运维表'
    PARTITIONED BY (dt STRING)
    STORED AS TEXTFILE ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_bike_operation';

select *
from songguo_warehouse.songguo_bike_operation;
--用户行为日志表
DROP TABLE IF EXISTS songguo_warehouse.songguo_user_behavior;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.songguo_user_behavior
(
    user_id       STRING COMMENT '用户ID',
    behavior_type int COMMENT '行为类型(1-注册,2-登录,3-开锁,4-支付,5-投诉)',
    behavior_time TIMESTAMP COMMENT '行为时间',
    device_id     STRING COMMENT '设备ID',
    app_Version   STRING COMMENT 'APP版本',
    oS_typeTINY   INT COMMENT '操作系统(1-iOS,2-Android)',
    ip            STRING COMMENT 'IP地址',
    city_id       INT COMMENT '城市ID'
) COMMENT '用户行为日志表'
    PARTITIONED BY (dt STRING)
    STORED AS TEXTFILE ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_user_behavior';
ALTER TABLE songguo_warehouse.songguo_user_behavior
    ADD IF NOT EXISTS PARTITION (dt = '2025-07-26');
--交易流水表
DROP TABLE IF EXISTS songguo_warehouse.songguo_transaction_record;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.songguo_transaction_record
(
    transaction_id     STRING COMMENT '交易ID',
    order_id           STRING COMMENT '关联订单ID',
    user_id            STRING COMMENT '用户ID',
    transaction_type   TINYINT COMMENT '交易类型(1-支付,2-退款)',
    transaction_amount DECIMAL(12, 2) COMMENT '交易金额',
    transaction_time   TIMESTAMP COMMENT '交易时间',
    payment_channel    TINYINT COMMENT '支付渠道(1-微信,2-支付宝)',
    payment_status     TINYINT COMMENT '支付状态',
    merchant_id        STRING COMMENT '商户ID',
    city_id            INT COMMENT '城市ID'
) COMMENT '交易流水表'
    PARTITIONED BY (dt STRING)
    STORED AS TEXTFILE ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_transaction_record';
--todo  2.相关维度表结构
--会员信息表
DROP TABLE IF EXISTS songguo_warehouse.songgou_user_info;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.songgou_user_info
(
    user_id          STRING COMMENT '用户ID',
    register_time    TIMESTAMP COMMENT '注册时间',
    register_city_id INT COMMENT '注册城市ID',
    gender           INT COMMENT '性别(0未知/1男/2女)',
    age_range        INT COMMENT '年龄段(1-6)',
    is_certified     BOOLEAN COMMENT '是否实名认证',
    last_active_date STRING COMMENT '最后活跃日期',
    user_level       INT COMMENT '用户等级(1-5)'
) COMMENT '会员信息表'
    PARTITIONED BY (dt STRING)
    STORED AS TEXTFILE ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songgou_user_info';
--车辆信息表
DROP TABLE IF EXISTS songguo_warehouse.songguo_bike_info;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.songguo_bike_info
(
    bike_id          STRING COMMENT '车辆ID',
    bike_type        INT COMMENT '车型(1标准/2豪华)',
    manufacture_date STRING COMMENT '生产日期',
    battery_type     STRING COMMENT '电池类型',
    initial_city_id  INT COMMENT '初始投放城市',
    is_shared        BOOLEAN COMMENT '是否共享车辆'
) COMMENT '车辆信息表'
    PARTITIONED BY (dt STRING)
    STORED AS TEXTFILE ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_bike_info';
--城市信息表
DROP TABLE IF EXISTS songguo_warehouse.songguo_city_info;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.songguo_city_info
(
    city_id       INT COMMENT '城市ID',
    city_name     STRING COMMENT '城市名称',
    province_id   INT COMMENT '省份ID',
    province_name STRING COMMENT '省份名称',
    region_id     INT COMMENT '大区ID(华北/华东等)',
    is_hot_city   BOOLEAN COMMENT '是否热门城市'
) COMMENT '城市信息表'
    PARTITIONED BY (dt STRING)
    STORED AS TEXTFILE ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_city_info';

select *
from songguo_warehouse.songguo_city_info;
--营销活动信息表
DROP TABLE IF EXISTS songguo_warehouse.songguo_campaign;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.songguo_campaign
(
    campaign_Id     STRING COMMENT '活动ID',
    campaign_name   STRING COMMENT '活动名称',
    start_time      TIMESTAMP COMMENT '开始时间',
    end_time        TIMESTAMP COMMENT '结束时间',
    campaign_type   INT COMMENT '活动类型(1折扣券/2满减)',
    target_city_ids STRING COMMENT '目标城市ID列表'
) COMMENT '营销活动信息表'
    PARTITIONED BY (dt STRING)
    STORED AS TEXTFILE ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_campaign';

select *
from songguo_campaign;
--车辆状态类型表
DROP TABLE IF EXISTS songguo_warehouse.songguo_bike_status_type;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.songguo_bike_status_type
(
    status_id      INT COMMENT '状态ID',
    status_name    STRING COMMENT '状态名称(可用/维修/缺电)',
    is_operational BOOLEAN COMMENT '是否可运营',
    priority       INT COMMENT '处理优先级'
) COMMENT '车辆状态类型表'
    PARTITIONED BY (dt STRING)
    STORED AS TEXTFILE ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_bike_status_type';
select status_id, status_name, is_operational, priority, dt
from songguo_bike_status_type;
