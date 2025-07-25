CREATE DATABASE IF NOT EXISTS songguo_warehouse
    location "hdfs://node101/user/spark/warehouse/songguo_warehouse";
USE songguo_warehouse;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS songguo_warehouse.dwd_fact_order_info;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.dwd_fact_order_info
(
    order_id           STRING COMMENT '订单ID',
    user_id            STRING COMMENT '用户ID',
    bike_id            STRING COMMENT '车辆ID',
    start_time         TIMESTAMP COMMENT '订单开始时间',
    end_time           TIMESTAMP COMMENT '订单结束时间',
    start_lng          DOUBLE COMMENT '起点经度',
    start_lat          DOUBLE COMMENT '起点纬度',
    end_lng            DOUBLE COMMENT '终点经度',
    end_lat            DOUBLE COMMENT '终点纬度',
    distance           DOUBLE COMMENT '骑行距离(公里)',
    duration           INT COMMENT '骑行时长(秒)',
    base_fee           DOUBLE COMMENT '基础费用',
    extra_fee          DOUBLE COMMENT '附加费用',
    total_fee          DOUBLE COMMENT '总费用',
    coupon_amount      DOUBLE COMMENT '优惠券抵扣',
    actual_pay         DOUBLE COMMENT '实付金额',
    pay_type           TINYINT COMMENT '支付方式(1-微信,2-支付宝)',
    pay_status         TINYINT COMMENT '支付状态(0-未支付,1-已支付,2-已退款)',
    city_id            INT COMMENT '城市ID',
    region_id          INT COMMENT '区域ID',
    is_night_ride      BOOLEAN COMMENT '是否夜间骑行',
    is_first_ride      BOOLEAN COMMENT '是否首次骑行',

    -- ** 以下是拉宽的维度字段 **
    -- 用户维度字段 (从 songgou_user_info 拉宽)
    user_gender        STRING COMMENT '用户性别(0未知/1男/2女)',
    user_age_range     INT COMMENT '用户年龄段(1-6)',
    user_is_certified  BOOLEAN COMMENT '用户是否实名认证',
    user_level         INT COMMENT '用户等级(1-5)',

    -- 车辆维度字段 (从 songguo_bike_info 拉宽)
    bike_type_name     STRING COMMENT '车型名称(标准/豪华)', -- 假设有映射
    bike_battery_type  STRING COMMENT '电池类型',
    bike_is_shared     BOOLEAN COMMENT '是否共享车辆',

    -- 城市维度字段 (从 songguo_city_info 拉宽)
    city_name          STRING COMMENT '城市名称',
    province_name      STRING COMMENT '省份名称',
    city_is_hot        BOOLEAN COMMENT '是否热门城市'
) COMMENT '订单明细事实表 (拉宽)'
    PARTITIONED BY (dt STRING COMMENT '订单日期分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/dwd_fact_order_info';

-- ALTER TABLE songguo_warehouse.dwd_fact_order_info ADD IF NOT EXISTS PARTITION (dt = '2025-07-26');
-- 从 ODS 层加载数据到 DWD 层，并拉宽维度字段
INSERT OVERWRITE TABLE songguo_warehouse.dwd_fact_order_info PARTITION (dt)
SELECT
    oi.order_id,
    oi.user_id,
    oi.bike_id,
    oi.start_time,
    oi.end_time,
    oi.start_lng,
    oi.start_lat,
    oi.end_lng,
    oi.end_lat,
    oi.distance,
    oi.duration,
    oi.base_fee,
    oi.extra_fee,
    oi.total_fee,
    oi.coupon_amount,
    oi.actual_pay,
    oi.pay_type,
    oi.pay_status,
    oi.city_id,
    oi.region_id,
    oi.is_night_ride,
    oi.is_first_ride,

    -- ** 拉宽用户维度信息 **
    CASE WHEN du.gender = 0 THEN "未知"
        WHEN du.gender = 1 THEN "男"
        WHEN du.gender = 2 THEN "女"
    END AS user_gender,
    du.age_range AS user_age_range,
    du.is_certified AS user_is_certified,
    du.user_level AS user_level,

    -- ** 拉宽车辆维度信息 **
    CASE bi.bike_type
        WHEN 1 THEN '标准'
        WHEN 2 THEN '豪华'
        ELSE '未知'
    END AS bike_type_name, -- 示例：将车型ID转换为名称
    bi.battery_type AS bike_battery_type,
    bi.is_shared AS bike_is_shared,

    -- ** 拉宽城市维度信息 **
    dc.city_name, -- 没数据
    dc.province_name, -- 没数据
    dc.is_hot_city AS city_is_hot, -- 没数据
    oi.dt -- DWD 表的分区字段
FROM
    songguo_warehouse.songgou_order_info oi -- ODS 层的订单表
        LEFT JOIN
    songguo_warehouse.songgou_user_info du -- 用户信息表
    ON
                oi.user_id = du.user_id AND du.dt = '2025-07-26' -- 注意：维度表也可能有分区，需要指定
        LEFT JOIN
    songguo_warehouse.songguo_bike_info bi
    ON
                oi.bike_id = bi.bike_id AND bi.dt = '2025-07-26'
        LEFT JOIN
    songguo_warehouse.songguo_city_info dc
    ON
                oi.city_id = dc.city_id AND dc.dt = '2025-07-26'
WHERE
        oi.dt = '2025-07-26'; -- 选择 ODS 层指定分区数据进行处理



-- 验证拉宽后的数据
SELECT * FROM songguo_warehouse.dwd_fact_order_info WHERE dt = '2025-07-26' LIMIT 10;


-- DWD层 - 车辆状态明细事实表 (dwd_fact_bike_status)
-- 拉宽字段：城市名称、省份名称、是否热门城市；车辆类型、电池类型、是否共享
DROP TABLE IF EXISTS songguo_warehouse.dwd_fact_bike_status;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.dwd_fact_bike_status
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
    status_time    TIMESTAMP COMMENT '状态时间',

    -- ** 以下是拉宽的维度字段 **
    -- 车辆维度字段 (从 songguo_bike_info 拉宽)
    bike_type_name     STRING COMMENT '车型名称(标准/豪华)',
    bike_manufacture_date DATE COMMENT '车辆生产日期',
    bike_battery_type  STRING COMMENT '电池类型',
    bike_is_shared     BOOLEAN COMMENT '是否共享车辆',

    -- 城市维度字段 (从 songguo_city_info 拉宽)
    city_name          STRING COMMENT '城市名称',
    province_name      STRING COMMENT '省份名称',
    city_is_hot        BOOLEAN COMMENT '是否热门城市',

    -- 状态类型维度字段 (从 songguo_bike_status_type 拉宽) - 假设可以根据 is_rented, is_maintenance, is_damaged 映射到 status_name
    -- 这里直接根据当前状态字段派生一个 status_name，或者如果 ODS 层有 status_id 字段，可以 join songguo_bike_status_type
    derived_status_name STRING COMMENT '派生车辆状态名称' -- 示例派生
) COMMENT '车辆状态明细事实表 (拉宽)'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/dwd_fact_bike_status';

-- 添加分区
-- ALTER TABLE songguo_warehouse.dwd_fact_bike_status ADD IF NOT EXISTS PARTITION (dt = '2025-07-26');

INSERT OVERWRITE TABLE songguo_warehouse.dwd_fact_bike_status PARTITION (dt)
SELECT
    bs.bike_id,
    bs.city_id,
    bs.district_id,
    bs.battery_level,
    bs.is_rented,
    bs.is_maintenance,
    bs.is_damaged,
    bs.gps_lng,
    bs.gps_lat,
    bs.status_time,

    -- ** 拉宽车辆维度信息 (从 songguo_bike_info) **
    CASE bi.bike_type
        WHEN 1 THEN '标准'
        WHEN 2 THEN '豪华'
        ELSE '未知'
        END AS bike_type_name,
    CAST(bi.manufacture_date AS DATE) AS bike_manufacture_date,
    bi.battery_type AS bike_battery_type,
    bi.is_shared AS bike_is_shared,

    -- ** 拉宽城市维度信息 (从 songguo_city_info) **
    ci.city_name,
    ci.province_name,
    ci.is_hot_city AS city_is_hot,

    -- ** 派生车辆状态名称 **
    CASE
        WHEN bs.is_rented = TRUE THEN '被租用'
        WHEN bs.is_maintenance = TRUE THEN '维修中'
        WHEN bs.is_damaged = TRUE THEN '已损坏'
        WHEN bs.battery_level < 20 THEN '缺电'
        ELSE '可用'
        END AS derived_status_name,
    bs.dt
FROM
    songguo_warehouse.songguo_bike_status bs -- ODS 层的车辆状态表
        LEFT JOIN
    songguo_warehouse.songguo_bike_info bi
    ON
                bs.bike_id = bi.bike_id AND bi.dt = '2025-07-26'
        LEFT JOIN
    songguo_warehouse.songguo_city_info ci
    ON
                bs.city_id = ci.city_id AND ci.dt = '2025-07-26'
WHERE
        bs.dt = '2025-07-26';

-- 验证数据
SELECT * FROM songguo_warehouse.dwd_fact_bike_status WHERE dt = '2025-07-26' LIMIT 10;


-- 整张表没数据
-- DWD层 - 车辆运维明细事实表 (dwd_fact_bike_operation)
-- 拉宽字段：车辆类型、电池类型、是否共享；运维人员性别、年龄、等级；城市名称、省份名称
DROP TABLE IF EXISTS songguo_warehouse.dwd_fact_bike_operation;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.dwd_fact_bike_operation
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
    district_id    INT COMMENT '区域ID',

    -- ** 以下是拉宽的维度字段 **
    -- 车辆维度字段 (从 songguo_bike_info 拉宽)
    bike_type_name     STRING COMMENT '车型名称(标准/豪华)',
    bike_battery_type  STRING COMMENT '电池类型',
    bike_is_shared     BOOLEAN COMMENT '是否共享车辆',

    -- 运维人员维度字段 (从 songgou_user_info 拉宽)
    operator_gender    INT COMMENT '运维人员性别',
    operator_age_range INT COMMENT '运维人员年龄段',
    operator_user_level INT COMMENT '运维人员等级',

    -- 城市维度字段 (从 songguo_city_info 拉宽)
    city_name          STRING COMMENT '城市名称',
    province_name      STRING COMMENT '省份名称',
    city_is_hot        BOOLEAN COMMENT '是否热门城市',

    -- 状态类型维度字段 (从 songguo_bike_status_type 拉宽)
    before_status_name STRING COMMENT '操作前状态名称',
    after_status_name  STRING COMMENT '操作后状态名称'
) COMMENT '车辆运维明细事实表 (拉宽)'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/dwd_fact_bike_operation';

-- ALTER TABLE songguo_warehouse.dwd_fact_bike_operation ADD IF NOT EXISTS PARTITION (dt = '2025-07-26');


INSERT OVERWRITE TABLE songguo_warehouse.dwd_fact_bike_operation PARTITION (dt)
SELECT
    bo.operation_id,
    bo.bike_id,
    bo.operator_id,
    bo.operation_type,
    bo.operation_time,
    bo.before_battery,
    bo.after_battery,
    bo.before_status,
    bo.after_status,
    bo.city_id,
    bo.district_id,

    -- ** 拉宽车辆维度信息 (从 songguo_bike_info) **
    CASE bi.bike_type
        WHEN 1 THEN '标准'
        WHEN 2 THEN '豪华'
        ELSE '未知'
        END AS bike_type_name,
    bi.battery_type AS bike_battery_type,
    bi.is_shared AS bike_is_shared,

    -- ** 拉宽运维人员维度信息 (从 songgou_user_info) **
    ui.gender AS operator_gender,
    ui.age_range AS operator_age_range,
    ui.user_level AS operator_user_level,

    -- ** 拉宽城市维度信息 (从 songguo_city_info) **
    ci.city_name,
    ci.province_name,
    ci.is_hot_city AS city_is_hot,

    -- ** 拉宽状态类型维度信息 (从 songguo_bike_status_type) **
    bst_before.status_name AS before_status_name,
    bst_after.status_name AS after_status_name,
    bo.dt
FROM
    songguo_warehouse.songguo_bike_operation bo -- ODS 层的车辆运维表
        LEFT JOIN
    songguo_warehouse.songguo_bike_info bi
    ON
                bo.bike_id = bi.bike_id AND bi.dt = '2025-07-26'
        LEFT JOIN
    songguo_warehouse.songgou_user_info ui
    ON
                bo.operator_id = ui.user_id AND ui.dt = '2025-07-26'
        LEFT JOIN
    songguo_warehouse.songguo_city_info ci
    ON
                bo.city_id = ci.city_id AND ci.dt = '2025-07-26'
        LEFT JOIN
    songguo_warehouse.songguo_bike_status_type bst_before
    ON
                bo.before_status = bst_before.status_id AND bst_before.dt = '2025-07-26'
        LEFT JOIN
    songguo_warehouse.songguo_bike_status_type bst_after
    ON
                bo.after_status = bst_after.status_id AND bst_after.dt = '2025-07-26'
WHERE
        bo.dt = '2025-07-26';

-- 验证数据
SELECT * FROM songguo_warehouse.dwd_fact_bike_operation WHERE dt = '2025-07-26' LIMIT 10;


-- 整张表没数据
-- DWD层 - 用户行为明细事实表 (dwd_fact_user_behavior)
-- 拉宽字段：用户性别、年龄、等级；城市名称、省份名称
DROP TABLE IF EXISTS songguo_warehouse.dwd_fact_user_behavior;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.dwd_fact_user_behavior
(
    user_id       STRING COMMENT '用户ID',
    behavior_type INT COMMENT '行为类型(1-注册,2-登录,3-开锁,4-支付,5-投诉)',
    behavior_time TIMESTAMP COMMENT '行为时间',
    device_id     STRING COMMENT '设备ID',
    app_Version   STRING COMMENT 'APP版本',
    os_type       TINYINT COMMENT '操作系统(1-iOS,2-Android)', -- 注意：原字段名为oS_typeTINY，已修正为os_type
    ip            STRING COMMENT 'IP地址',
    city_id       INT COMMENT '城市ID',

    -- ** 以下是拉宽的维度字段 **
    -- 用户维度字段 (从 songgou_user_info 拉宽)
    user_gender        INT COMMENT '用户性别(0未知/1男/2女)',
    user_age_range     INT COMMENT '用户年龄段(1-6)',
    user_is_certified  BOOLEAN COMMENT '用户是否实名认证',
    user_level         INT COMMENT '用户等级(1-5)',

    -- 城市维度字段 (从 songguo_city_info 拉宽)
    city_name          STRING COMMENT '城市名称',
    province_name      STRING COMMENT '省份名称',
    city_is_hot        BOOLEAN COMMENT '是否热门城市'
) COMMENT '用户行为明细事实表 (拉宽)'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/dwd_fact_user_behavior';

-- ALTER TABLE songguo_warehouse.dwd_fact_user_behavior ADD IF NOT EXISTS PARTITION (dt = '2025-07-26');


INSERT OVERWRITE TABLE songguo_warehouse.dwd_fact_user_behavior PARTITION (dt)
SELECT
    ub.user_id,
    ub.behavior_type,
    ub.behavior_time,
    ub.device_id,
    ub.app_Version,
    ub.oS_typeTINY, -- 使用原始字段名
    ub.ip,
    ub.city_id,

    -- ** 拉宽用户维度信息 (从 songgou_user_info) **
    ui.gender AS user_gender,
    ui.age_range AS user_age_range,
    ui.is_certified AS user_is_certified,
    ui.user_level AS user_level,

    -- ** 拉宽城市维度信息 (从 songguo_city_info) **
    ci.city_name,
    ci.province_name,
    ci.is_hot_city AS city_is_hot,
    ub.dt
FROM
    songguo_warehouse.songguo_user_behavior ub -- ODS 层的用户行为日志表
        LEFT JOIN
    songguo_warehouse.songgou_user_info ui
    ON
                ub.user_id = ui.user_id AND ui.dt = '2025-07-26'
        LEFT JOIN
    songguo_warehouse.songguo_city_info ci
    ON
                ub.city_id = ci.city_id AND ci.dt = '2025-07-26'
WHERE
        ub.dt = '2025-07-26';



-- 验证数据
SELECT * FROM songguo_warehouse.dwd_fact_user_behavior WHERE dt = '2025-07-26' LIMIT 10;


-- DWD层 - 交易流水明细事实表 (dwd_fact_transaction_record)
-- 拉宽字段：订单相关信息（时长、距离、总费用）、用户性别、年龄、等级；城市名称、省份名称

-- todo 整张表没数据
DROP TABLE IF EXISTS songguo_warehouse.dwd_fact_transaction_record;
CREATE EXTERNAL TABLE IF NOT EXISTS songguo_warehouse.dwd_fact_transaction_record
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
    city_id            INT COMMENT '城市ID',

    -- ** 以下是拉宽的维度字段 **
    -- 订单维度字段 (从 songgou_order_info 拉宽)
    order_duration     INT COMMENT '关联订单骑行时长(秒)',
    order_distance     DOUBLE COMMENT '关联订单骑行距离(公里)',
    order_total_fee    DOUBLE COMMENT '关联订单总费用',
    order_pay_type     TINYINT COMMENT '关联订单支付方式',
    order_is_first_ride BOOLEAN COMMENT '关联订单是否首次骑行',

    -- 用户维度字段 (从 songgou_user_info 拉宽)
    user_gender        INT COMMENT '用户性别(0未知/1男/2女)',
    user_age_range     INT COMMENT '用户年龄段(1-6)',
    user_level         INT COMMENT '用户等级(1-5)',

    -- 城市维度字段 (从 songguo_city_info 拉宽)
    city_name          STRING COMMENT '城市名称',
    province_name      STRING COMMENT '省份名称',
    city_is_hot        BOOLEAN COMMENT '是否热门城市'
) COMMENT '交易流水明细事实表 (拉宽)'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/dwd_fact_transaction_record';

-- ALTER TABLE songguo_warehouse.dwd_fact_transaction_record ADD IF NOT EXISTS PARTITION (dt = '2025-07-26');


INSERT OVERWRITE TABLE songguo_warehouse.dwd_fact_transaction_record PARTITION (dt)
SELECT
    tr.transaction_id,
    tr.order_id,
    tr.user_id,
    tr.transaction_type,
    tr.transaction_amount,
    tr.transaction_time,
    tr.payment_channel,
    tr.payment_status,
    tr.merchant_id,
    tr.city_id,

    -- ** 拉宽订单维度信息 (从 songgou_order_info) **
    oi.duration AS order_duration,
    oi.distance AS order_distance,
    oi.total_fee AS order_total_fee,
    oi.pay_type AS order_pay_type,
    oi.is_first_ride AS order_is_first_ride,

    -- ** 拉宽用户维度信息 (从 songgou_user_info) **
    ui.gender AS user_gender,
    ui.age_range AS user_age_range,
    ui.user_level AS user_level,

    -- ** 拉宽城市维度信息 (从 songguo_city_info) **
    ci.city_name,
    ci.province_name,
    ci.is_hot_city AS city_is_hot,
    tr.dt
FROM
    songguo_warehouse.songguo_transaction_record tr -- ODS 层的交易流水表
        LEFT JOIN
    songguo_warehouse.songgou_order_info oi
    ON
                tr.order_id = oi.order_id AND oi.dt = '2025-07-26'
        LEFT JOIN
    songguo_warehouse.songgou_user_info ui
    ON
                tr.user_id = ui.user_id AND ui.dt = '2025-07-26'
        LEFT JOIN
    songguo_warehouse.songguo_city_info ci
    ON
                tr.city_id = ci.city_id AND ci.dt = '2025-07-26'
WHERE
        tr.dt = '2025-07-26';

-- 验证数据
SELECT * FROM songguo_warehouse.dwd_fact_transaction_record WHERE dt = '2025-07-26' LIMIT 10;