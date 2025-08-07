create database jtp_oms;

CREATE TABLE IF NOT EXISTS ads_coupon_daily_report
(
    dt                     DATE,
    coupon_get_count       BIGINT,
    coupon_used_count      BIGINT,
    coupon_used_user_count BIGINT,
    PRIMARY KEY (dt)
);

select *
from ads_coupon_daily_report;
-- COMMENT ON TABLE ads_coupon_daily_report IS '每日优惠卷统计';
-- COMMENT ON COLUMN jtp_oms.ads_coupon_daily_report.dt IS '数据统计日期';
-- COMMENT ON COLUMN jtp_oms.ads_coupon_daily_report.coupon_get_count IS '每日优惠卷领取次数';
-- COMMENT ON COLUMN jtp_oms.ads_coupon_daily_report.coupon_used_count IS '每日优惠卷使用次数';
-- COMMENT ON COLUMN jtp_oms.ads_coupon_daily_report.coupon_used_user_count IS '每日优惠卷使用人数';