-- 创建Catalog，集成Hive
/*
client_province 省份
            client_os_type 操作系统类型
            client_browser_type 浏览器类型
        2. 指标
            点击量
            曝光量
        3. 时间
            每日每小时数据
*/

-- todo Doris创建数据库
CREATE DATABASE IF NOT EXISTS jtp_ads_warehouse ;
USE jtp_ads_warehouse ;


-- =========================================================================
-- 1. Doris：创建汇总表
-- =========================================================================
DROP TABLE IF EXISTS dws_hour_common_ads_agg ;
CREATE TABLE IF NOT EXISTS dws_hour_common_ads_agg
(
    dt                  DATE COMMENT '日期',
    hr                  VARCHAR COMMENT '小时',
    ads_id               BIGINT COMMENT '广告id',
    ads_name            VARCHAR COMMENT '广告名称',
    platform_id         VARCHAR COMMENT '推广平台id',
    platform_name_zh    VARCHAR COMMENT '推广平台名称(中文)',
    client_province     VARCHAR COMMENT '客户端所处省份',
    client_city         VARCHAR COMMENT '客户端所处城市',
    client_os_type      VARCHAR COMMENT '客户端操作系统类型',
    client_browser_type VARCHAR COMMENT '客户端浏览器类型',
    is_invalid_traffic  BOOLEAN COMMENT '是否是异常流量',
    click_count         BIGINT REPLACE COMMENT '点击次数',
    impression_count    BIGINT REPLACE COMMENT '曝光次数'
)
    -- 聚合字段 作为主键
    AGGREGATE KEY(
      dt, hr, ads_id, ads_name, platform_id, platform_name_zh, client_province, client_city, client_os_type, client_browser_type, is_invalid_traffic
    )
    COMMENT '公共粒度汇总广告投放日志数据'
    -- 分区
    PARTITION BY RANGE(dt)(
      PARTITION p240101 VALUES LESS THAN("2024-01-01"),
      PARTITION p250101 VALUES LESS THAN("2025-01-01"),
      PARTITION p260102 VALUES LESS THAN("2026-01-01")
    )
    -- 分桶
    DISTRIBUTED BY HASH(hr) BUCKETS 5
    -- 属性
    PROPERTIES (
      "replication_allocation" = "tag.location.default:1"
    )
;


-- =========================================================================
-- 2. 编写SQL，对DWD层事实表进行汇总
-- =========================================================================
INSERT INTO jtp_ads_warehouse.dws_hour_common_ads_agg
SELECT
    '2024-10-01' AS dt
     , hour(from_unixtime(event_time / 1000)) AS hr
     , ads_id,ads_name
     , platform_id, platform_name_zh
     , client_province, client_city
     , client_os_type
     , client_browser_type
     , is_invalid_traffic
     -- 点击次数和曝光次数
     , count(if(event_type = 'click', ads_id, NULL)) AS click_count
     , count(ads_id) AS impression_count
FROM hive_catalog.jtp_ads_warehouse.dwd_ads_event_log_inc
WHERE dt = '2024-10-01'
  AND event_type IN ('click', 'impression')
GROUP BY hour(from_unixtime(event_time / 1000))
       , ads_id,ads_name
       , platform_id, platform_name_zh
       , client_province, client_city
       , client_os_type
       , client_browser_type
       , is_invalid_traffic
;


-- 查询Doris中汇结果表
SELECT
    hr, ads_id, ads_name, platform_id, platform_name_zh, client_province, client_city, client_os_type, client_browser_type, is_invalid_traffic, click_count, impression_count
FROM jtp_ads_warehouse.dws_hour_common_ads_agg
WHERE dt = '2024-10-01'
;