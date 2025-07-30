-- todo 1.车辆利用率：计算每辆车的日均使用时长（小时）
DROP TABLE IF EXISTS ads_car_use_time;
CREATE TABLE IF NOT EXISTS ads_car_use_time
AS
SELECT
    dt -- 日期
    ,bike_id -- 车辆ID
    ,city_id -- 城市ID
    ,sum(duration) /60/60 AS usage_hours -- 车辆日均使用时长（小时）
    ,count(*) AS ride_times -- 车辆总使用次数
FROM songguo_warehouse.dwd_fact_order_info
GROUP BY bike_id,city_id,dt;

SELECT
    *
FROM ads_car_use_time;

-- todo 2.低电量车辆预警：识别电量低于20%且未被租用的车辆
DROP TABLE IF EXISTS ads_low_battery_warning;
CREATE TABLE IF NOT EXISTS ads_low_battery_warning
AS
SELECT
    bike_id -- 车辆ID
    ,city_id -- 城市ID
    ,battery_level -- 电量
    ,gps_lng -- 经度
    ,gps_lat -- 纬度
FROM songguo_warehouse.dwd_fact_bike_status
WHERE battery_level < 20
AND is_rented = false;

SELECT
    *
FROM ads_low_battery_warning;










