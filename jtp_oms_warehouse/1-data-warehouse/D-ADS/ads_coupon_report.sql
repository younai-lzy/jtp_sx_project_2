CREATE DATABASE IF NOT EXISTS jtp_oms_warehouse location
    'hdfs://node101:8020/warehouse/jtp_oms_warehouse';

USE jtp_oms_warehouse;
-- todo 每个优惠卷：累计统计
DROP TABLE IF EXISTS jtp_oms_warehouse.ads_coupon_report;
CREATE EXTERNAL TABLE IF NOT EXISTS jtp_oms_warehouse.ads_coupon_report
(
    dt                     STRING COMMENT '数据统计日期',
    coupon_name            STRING COMMENT '优惠卷名称',
    coupon_get_count       BIGINT COMMENT '优惠卷累计领取张数',
    coupon_used_count      BIGINT COMMENT '优惠卷累计使用张数',
    coupon_used_ratio      DECIMAL(16, 4) COMMENT '优惠卷累计使用率',
    coupon_used_user_count BIGINT COMMENT '优惠卷累计使用人数'
) COMMENT '每个优惠卷累计统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_warehouse/ads_coupon_report';

-- 数据加载
WITH
    t1 AS (
        -- s1-优惠卷领取
        SELECT
            coupon_id
             , count(coupon_id) AS coupon_get_count
        FROM jtp_oms_warehouse.dwd_coupon_get_incr
        WHERE dt <= '2024-12-31'
        GROUP BY coupon_id
    )
   , t2 AS (
    -- s2-优惠卷使用
    SELECT
        coupon_id
         , count(coupon_id) AS coupon_used_count
         , count(distinct member_id) AS coupon_used_user_count
    FROM jtp_oms_warehouse.dwd_coupon_used_incr
    WHERE dt <= '2024-12-31'
    GROUP BY coupon_id
)
   , t3 AS (
    -- s3-优惠卷信息表
    SELECT id, name AS coupon_name  FROM jtp_oms_warehouse.dim_oms_coupon_full WHERE dt = '2024-12-31'
)
INSERT OVERWRITE TABLE jtp_oms_warehouse.ads_coupon_report
SELECT * FROM jtp_oms_warehouse.ads_coupon_report
UNION
-- s4-关联数据
SELECT
    '2024-12-31' AS dt
     , t3.coupon_name
     , t1.coupon_get_count
     , nvl(t2.coupon_used_count, 0) AS coupon_used_count
     , round(
                nvl(t2.coupon_used_count, 0) * 1.0 / t1.coupon_get_count , 4
    ) AS coupon_used_ratio
     , nvl(coupon_used_user_count, 0) AS coupon_used_user_count
FROM t1
         LEFT JOIN t2 ON t1.coupon_id = t2.coupon_id
         LEFT JOIN t3 ON t1.coupon_id = t3.id
;

