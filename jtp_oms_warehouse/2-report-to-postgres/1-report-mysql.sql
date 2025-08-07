-- 创建表1：每日优惠卷统计
DROP TABLE IF EXISTS jtp_oms.ads_coupon_daily_report;
CREATE TABLE IF NOT EXISTS jtp_oms.ads_coupon_daily_report
(
    dt                     DATE COMMENT '数据统计日期',
    coupon_get_count       BIGINT COMMENT '每日优惠卷领取次数',
    coupon_used_count      BIGINT COMMENT '每日优惠卷使用次数',
    coupon_used_user_count BIGINT COMMENT '每日优惠卷使用人数',
    PRIMARY KEY (dt)
) COMMENT '每日优惠卷统计' ENGINE = InnoDB
  CHARACTER
SET = utf8
    COLLATE = utf8_general_ci
    ROW_FORMAT = DYNAMIC;


-- 创建表2：优惠卷累计统计
DROP TABLE IF EXISTS jtp_oms.ads_coupon_ratio_report;
CREATE TABLE IF NOT EXISTS jtp_oms.ads_coupon_ratio_report
(
    dt                DATE COMMENT '数据统计日期',
    coupon_get_count  BIGINT COMMENT '优惠卷累计领取张数',
    coupon_used_count BIGINT COMMENT '优惠卷累计使用张数',
    coupon_used_ratio DECIMAL(16, 2) COMMENT '优惠卷累计使用率',
    PRIMARY KEY (dt)
) COMMENT '优惠卷累计统计' ENGINE = InnoDB
    CHARACTER
SET = utf8
    COLLATE = utf8_general_ci
    ROW_FORMAT = DYNAMIC;


-- 创建表3：每个优惠卷累计统计
DROP TABLE IF EXISTS jtp_oms.ads_coupon_report;
CREATE TABLE IF NOT EXISTS jtp_oms.ads_coupon_report
(
    dt                     DATE COMMENT '数据统计日期',
    coupon_name            VARCHAR(255) COMMENT '优惠卷名称',
    coupon_get_count       BIGINT COMMENT '优惠卷累计领取张数',
    coupon_used_count      BIGINT COMMENT '优惠卷累计使用张数',
    coupon_used_ratio      DECIMAL(16, 2) COMMENT '优惠卷累计使用率',
    coupon_used_user_count BIGINT COMMENT '优惠卷累计使用人数',
    PRIMARY KEY (dt, coupon_name)
) COMMENT '每个优惠卷累计统计' ENGINE = InnoDB
    CHARACTER
SET = utf8
    COLLATE = utf8_general_ci
    ROW_FORMAT = DYNAMIC;