CREATE DATABASE IF NOT EXISTS jtp_oms_warehouse location
    'hdfs://node101:8020/warehouse/jtp_oms_warehouse';

USE jtp_oms_warehouse;

-- todo 优惠卷统计数据
/*
    累计领取张数    累计使用张数    累计使用率
      6               0           0.00%
*/
DROP TABLE IF EXISTS jtp_oms_warehouse.ads_coupon_ratio_report;
CREATE EXTERNAL TABLE IF NOT EXISTS jtp_oms_warehouse.ads_coupon_ratio_report(
                                                                               dt STRING COMMENT '数据统计日期',
                                                                               coupon_get_count BIGINT COMMENT '优惠卷累计领取张数',
                                                                               coupon_used_count BIGINT COMMENT '优惠卷累计使用张数',
                                                                               coupon_used_ratio DECIMAL(16, 4) COMMENT '优惠卷累计使用率'
) COMMENT '所有优惠卷累计统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_warehouse/ads_coupon_ratio_report' ;

INSERT OVERWRITE TABLE jtp_oms_warehouse.ads_coupon_ratio_report
SELECT dt, coupon_get_count, coupon_used_count, coupon_used_ratio FROM jtp_oms_warehouse.ads_coupon_ratio_report
UNION
SELECT
    '2024-12-31' AS dt
     , sum(coupon_get_count) AS coupon_get_count
     , sum(coupon_used_count) AS coupon_used_count
     , round((sum(coupon_used_count) * 1.0 / sum(coupon_get_count)), 4) AS coupon_used_ratio
FROM jtp_oms_warehouse.ads_coupon_daily_report
WHERE dt <= '2024-12-31'
;


SELECT
    '2025-01-01' AS dt
     , sum(if(metrics_type = 'get', metrics_count, 0)) AS coupon_get_count
     , sum(if(metrics_type = 'used', metrics_count, 0)) AS coupon_used_count
     -- 使用率
     , concat(
            round(
                        sum(if(metrics_type = 'used', metrics_count, 0)) / sum(if(metrics_type = 'get', metrics_count, 0))
                , 4
                ) * 100
    , '%'
    ) AS conpon_used_rate
FROM(
        -- 1. 领取
        SELECT
            '2025-01-01' AS dt
             , 'get' AS metrics_type
             , count(id) AS metrics_count
        FROM jtp_oms_warehouse.dwd_coupon_get_incr
        UNION ALL
        -- 2. 使用
        SELECT
            '2025-01-01' AS dt
             , 'used' AS metrics_type
             , count(id) AS metrics_count
        FROM jtp_oms_warehouse.dwd_coupon_used_incr
    ) t1
;