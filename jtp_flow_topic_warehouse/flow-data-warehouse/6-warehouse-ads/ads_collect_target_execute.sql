-- ADS层表结构
--
-- ads_page_traffic_analysis_daily: 每日页面流量分析汇总表
-- 聚合每日页面流量数据，提供分析看板所需的核心指标。

CREATE DATABASE IF NOT EXISTS jtp_flow_topic_warehouse
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse';
USE jtp_flow_topic_warehouse;
CREATE EXTERNAL TABLE IF NOT EXISTS ads_page_traffic_analysis_daily
(
    page_id               STRING COMMENT '页面ID',
    page_name             STRING COMMENT '页面名称',
    page_type             STRING COMMENT '页面类型',
    total_clicks          BIGINT COMMENT '总点击量',
    total_page_views      BIGINT COMMENT '总访问量',
    unique_visitors       BIGINT COMMENT '独立访客数',
    unique_clickers       BIGINT COMMENT '独立点击人数',
    guided_payment_amount DOUBLE COMMENT '引导支付金额',
    conversion_rate       DOUBLE COMMENT '页面点击转化率 (独立点击人数 / 独立访客数)',
    bounce_rate           DOUBLE COMMENT '跳失率 (单页面访问会话数 / 总会话数)'
)
    COMMENT '每日页面流量分析汇总表'
    PARTITIONED BY (dt STRING COMMENT '分区日期，格式YYYYMMDD')
    STORED AS PARQUET
    LOCATION '/user/hive/warehouse/ads.db/ads_page_traffic_analysis_daily';