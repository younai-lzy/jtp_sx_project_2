CREATE DATABASE IF NOT EXISTS `jtp_vivo_warehouse` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `jtp_vivo_warehouse`;

-- 用户行为日志表
CREATE TABLE IF NOT EXISTS `jtp_vivo_warehouse.vivo_log_info`
(
    log_id STRING COMMENT "日志ID",
    user_id STRING COMMENT "用户ID ",
    device_id STRING COMMENT "设备ID ",
    event_time TIMESTAMP COMMENT "事件时间 ",
    event_type STRING COMMENT " 事件类型",
    event_detail STRING COMMENT " 事件详情 ",
    app_name STRING COMMENT " 应用名称",
    apP_version STRING COMMENT "应用版本",
    │os_version STRING COMMENT "系统版本 ",
    network_type STRING COMMENT "网络类型",
    ip_address STRING COMMENT "IP地址",
    province STRING COMMENT "省份",
    city STRING COMMENT " 城市 ",
    extra_info STRING COMMENT " 额外信息"
) COMMENT "广告信息表";

-- 用户信息表vivo_user_info
CREATE TABLE IF NOT EXISTS jtp_vivo_warehouse.vivo_user_info
(
    user_id STRING COMMENT "用户ID",
    register_date    DATE COMMENT "注册日期",
    age              INT COMMENT "年龄",
    gender STRING COMMENT "性别",
    vip_level        INT COMMENT "VIP等级",
    vip_expire_date  DATE COMMENT "VIP到期日",
    phone_brand STRING COMMENT "手机品牌",
    phone_model STRING COMMENT "手机型号",
    oS_Version STRING COMMENT "系统版本",
    first_login_date DATE COMMENT "首次登录日期",
    last_login_date  DATE COMMENT "最后登录日期",
    province STRING COMMENT "省份",
    city STRING COMMENT "城市",
    district STRING COMMENT "区县",
    is_active        BOOLEAN COMMENT "是否活跃用户",
    user_tag ARRAY< STRING > COMMENT "用户标签",
    update_time      TIMESTAMP COMMENT "更新时问"
) COMMENT "用户信息表";

-- 设备信息表vivo_device_info
CREATE TABLE IF NOT EXISTS jtp_vivo_warehouse.vivo_device_info
(
    device_id STRING COMMENT "设备ID",
    user_id STRING COMMENT "关联用户ID",
    │device_type STRING COMMENT "设各类型(手机/平板/手表)",
    device_model STRING COMMENT " 设备型号",
    manufacture_date DATE COMMENT "生产日期",
    purchase_date    DATE COMMENT "购买日期",
    screen_sizeFLOAT COMMENT "屏幕尺寸(英寸)",
    resolution STRING COMMENT "屏幕分辨率",
    memory_size      INT COMMENT "内存大小(GB)",
    storage_size     INT COMMENT "存储大小(GB)",
    os_type STRING COMMENT "操作系统类型",
    os_VersIon STRING COMMENT "操作系统版本",
    network_type STRING COMMENT "主要网络类型",
    is_rooted        BOOLEAN COMMENT "是否root",
    update_timeTIMESTAMP COMMENT "更新时间"
) COMMENT "设备信息表"
;

-- 应用信息表vivo_app_info
CREATE TABLE IF NOT EXISTS jtp_vivo_warehouse.vivo_app_info
(
    app_id STRING COMMENT "应用ID",
    app_name STRING COMMENT "应用名称",
    app_category STRING cOMMENT " 应用分类 ",
    app_subcategory STRING COMMENT " 应川子分类 ",
    developer STRING COMMENT " 开发者 ",
    publish_date     DATE COMMENT " 发布日期 ",
    is_official      BOOLEAN COMMENT " 是否官方应用 ",
    is_preinstalled BOOLEANCOMMENT " 是否预装应用 ",
    ｜min_Os_VeRsion STRING COMMENT " 最低支持系统版本 ",
    price            DECIMAL(10, 2) COMMENT " 价格 ",
    avg_rating       FLOAT COMMENT " 平均评分 ",
    download_count   BIGINT COMMENT " 下载量 ",
    update_frequencySTRING COMMENT " 史新频率(高频/中频/低频) ",
    last_update_date DATE COMMENT " 最后更新日期",
    description STRING COMMENT " 应用描述 ",
    update_time      TIMESTAMP COMMENT " 更新时间"
) COMMENT "应用信息表"
;

-- 地理位置信息表
CREATE TABLE IF NOT EXISTS jtp_vivo_warehouse.vivo_geography_info
(
    location_id STRING COMMENT "位置ID",
    country STRING COMMENT "国家",
    province STRING COMMENT "省份",
    city STRING COMMENT "城市",
    district STRING COMMENT "区县",
    longitude  DECIMAL(10, 6) COMMENT "经度",
    latitude   DECIMAL(10, 6) COMMENT "纬度",
    timezone STRING COMMENT "时区",
    gdp        DECIMAL(15, 2) COMMENT "GDP(亿元)",
    population BIGINT COMMENT "人口数量",
    is_first_tier_city BOOLEAN COMMENT "是否一线城市",
    is_tourist_city BOOLEAN COMMENT "是否旅游城市",
    development_level STRING COMMENT "发展水平(发达/发展中/欠发达)",
    update_timeTIMESTAMP COMMENT "更新时间"
) COMMENT "地理位置信息表"
;

