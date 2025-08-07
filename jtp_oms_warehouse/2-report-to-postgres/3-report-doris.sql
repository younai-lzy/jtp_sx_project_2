CREATE DATABASE IF NOT EXISTS jtp_oms_warehouse;
USE jtp_oms_warehouse;

DROP TABLE IF EXISTS jtp_oms_warehouse.ads_coupon_daily_report;
CREATE TABLE IF NOT EXISTS jtp_oms_warehouse.ads_coupon_daily_report
(
    dt                     DATETIME       COMMENT '数据统计日期',
    coupon_get_count       BIGINT       COMMENT '每日优惠卷领取次数',
    coupon_used_count      BIGINT       COMMENT '每日优惠卷使用次数',
    coupon_used_user_count VARCHAR(20)       COMMENT '每日优惠卷使用人数'
)
UNIQUE KEY(dt) -- 将模型改为 UNIQUE KEY，保证dt的唯一性，新数据会覆盖旧数据
COMMENT '每日所有优惠卷统计'
DISTRIBUTED BY HASH(dt) BUCKETS 1 -- 根据dt列进行哈希分桶，桶数可根据数据量和集群规模调整
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1"
);

select *
from ads_coupon_daily_report;

SELECT HEX(coupon_used_user_count) FROM jtp_oms_warehouse.ads_coupon_daily_report;
