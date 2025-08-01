CREATE DATABASE IF NOT EXISTS jtp_flow_topic_warehouse
    location 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse';
use jtp_flow_topic_warehouse;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS dim_product;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_product
(
    product_id   STRING COMMENT '产品ID',
    product_name STRING COMMENT '产品名称',
    category     STRING COMMENT '商品类别'
) COMMENT '产品维度表'
    PARTITIONED BY (dt string comment '日期分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/dim_product';
;

-- 插入数据
INSERT OVERWRITE TABLE jtp_flow_topic_warehouse.dim_product PARTITION (dt)
SELECT product_id,
       product_name,
       category,
       '2025-07-31' AS dt
FROM jtp_flow_topic_warehouse.ods_product_info;

DROP TABLE IF EXISTS dim_page;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_page
(
    page_id   STRING PRIMARY KEY COMMENT '页面ID',
    page_name STRING COMMENT '页面名称',
    page_type STRING COMMENT '页面类型'
) COMMENT '页面维度表'
    PARTITIONED BY (dt string comment '日期分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/dim_page';
;

-- 插入数据
INSERT OVERWRITE TABLE jtp_flow_topic_warehouse.dim_page PARTITION (dt)
SELECT page_id,
       page_name,
       page_type,
       '2025-07-31' AS dt
FROM jtp_flow_topic_warehouse.ods_page_info;

DROP TABLE IF EXISTS dim_user;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_user
(
    user_id   STRING PRIMARY KEY COMMENT '用户ID',
    age_group STRING COMMENT '年龄段',
    country STRING COMMENT '国家',
    province  STRING COMMENT '省份',
    city      STRING COMMENT '城市'
) COMMENT '用户维度表'
    PARTITIONED BY (dt string comment '日期分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/dim_user'
;

-- 插入数据
INSERT OVERWRITE TABLE jtp_flow_topic_warehouse.dim_user PARTITION (dt)
SELECT user_id,
       age_group,
       country,
       province,
       city,
       '2025-07-31' AS dt
FROM jtp_flow_topic_warehouse,ods_user_action_log


SELECT *,
       default.(client_user_agent) AS browser_map
FROM jtp_flow_topic_warehouse.ods_user_action_log

























-- CREATE TABLE fact_sales
-- (
--     order_id   STRING PRIMARY KEY COMMENT '订单ID',
--     product_id STRING COMMENT '产品ID',
--     user_id    STRING COMMENT '用户ID',
--     date_id    CHAR(8) COMMENT '日期ID',
--     quantity   INT COMMENT '购买数量',
--     amount     DECIMAL(10, 2) COMMENT '销售金额',
--     FOREIGN KEY (product_id) REFERENCES dim_product (product_id),
--     FOREIGN KEY (user_id) REFERENCES dim_user (user_id),
--     FOREIGN KEY (date_id) REFERENCES dim_date (date_id)
-- ) COMMENT='销售事实表';
--
-- CREATE TABLE fact_page_view
-- (
--     log_id      STRING PRIMARY KEY COMMENT '日志ID',
--     page_id     STRING COMMENT '页面ID',
--     user_id     STRING COMMENT '用户ID',
--     date_id     CHAR(8) COMMENT '日期ID',
--     view_count  INT COMMENT '浏览次数',
--     click_count INT COMMENT '点击次数',
--     FOREIGN KEY (page_id) REFERENCES dim_page (page_id),
--     FOREIGN KEY (user_id) REFERENCES dim_user (user_id),
--     FOREIGN KEY (date_id) REFERENCES dim_date (date_id)
-- ) COMMENT='页面访问事实表';