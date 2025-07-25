CREATE DATABASE IF NOT EXISTS songguo_warehouse DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE songguo_warehouse;

-- 订单表songgou_order_info
CREATE TABLE IF NOT EXISTS songguo_warehouse.songgou_order_info
(
    order_id      STRING COMMENT "订单ID",
    user_id       STRING COMMENT "用户ID",
    bike_id       STRING COMMENT "车辆ID",
    start_time    TIMESTAMP COMMENT "开始时间",
    end_time      TIMESTAMP COMMENT "结束时问",
    start_Ing     DOUBLE COMMENT "起点经度",
    start_lat     DOUBLE COMMENT "起点纬度",
    end_1ng       DOUBLE COMMENT "终点经度",
    end_lat       DOUBLE COMMENT "终点纬度",
    distance      DOUBLE COMMENT "骑行距离(公里)",
    duration      INT COMMENT "骑行时长(秒)",
    base_fee      DECIMAL(10, 2) COMMENT "基础费用",
    extra_fee     DECIMAL(10, 2) COMMENT "附加费用",
    total_fee     DECIMAL(10, 2) COMMENT "总费用 ",
    coupon_amount DECIMAL(10, 2) COMMENT "优惠券抵扣",
    actual_pay    DECIMAL(10, 2) COMMENT "实付金额 ",
    pay_typeTINYINT COMMENT "支付方式(1-微信,2-支付宝) ",
    pay_status    TINYINT COMMENT "支付状态(0-未支付,1-已支付,2-已退款) ",
    city_id       INT COMMENT "城市ID",
    region_id     INT COMMENT "区域ID",
    is_night_ride BOOLEAN COMMENT "是否夜间骑行",
    is_first_ride BOOLEAN COMMENT "是否首次骑行"
)
    COMMENT "订单表";

-- 车辆状态信息表songguo_bike_status
CREATE TABLE IF NOT EXISTS songguo_warehouse.songguo_bike_status
(
    bike_id        STRING COMMENT "车辆ID",
    city_id        INT COMMENT "城市ID",
    district_id    INT COMMENT "区域ID",
    battery_level  INT COMMENT "电池电量(0-100)",
    is_rented      BOOLEAN COMMENT "是否被租用",
    is_maintenance BOOLEAN COMMENT "是否维修中",
    is_damaged     BOOLEAN COMMENT "是否损坏",
    gps_Ing        DOUBLE COMMENT "经度",
    gps_lat        DOUBLE COMMENT "纬度",
    status_time    TIMESTAMP COMMENT "状态时间"
) COMMENT "车辆状态信息表"
;

-- 车辆运维记录表songguo_bike_operation
CREATE TABLE IF NOT EXISTS songguo_warehouse.songguo_bike_operation
(
    operation_id   STRING COMMENT "运维记录ID",
    bike_id        STRING COMMENT "车辆 ID ",
    operator_id    STRING COMMENT "运维人员 ID ",
    operation_type TINYINT COMMENT " 操作类型(1-投放,2-回收,3-维修,4-换电) ",
    operation_time TIMESTAMP COMMENT "操作时间",
    before_battery INT COMMENT "操作前电量",
    after_batteryINT COMMENT "操作后电量",
    before_statusTINYINT COMMENT "操作前状态",
    after_statusTINYINT COMMENT "操作后状态",
    city_id        INT COMMENT "城市ID",
    district_id    INT COMMENT "区域ID"
) COMMENT "车辆运维记录表"
;

