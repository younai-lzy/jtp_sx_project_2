CREATE DATABASE IF NOT EXISTS jtp_flow_topic_warehouse
    location 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse';
use jtp_flow_topic_warehouse;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- 增加并行度
--SET spark.executor.instances = 6;   -- 每个虚拟机2个Executor
-- 合理分配内存
--SET spark.executor.memory = 2.5g;   -- 每个Executor分配2.5GB堆内存

-- 优化数据混洗（Shuffle）的并行度
-- SET spark.executor.cores = 2;       -- 假设你的VM有4个核，分2个给每个Executor
SET spark.sql.shuffle.partitions = 400; -- 适当增加

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
    page_id   STRING COMMENT '页面ID',
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
    user_id  STRING COMMENT '用户ID',
    country  STRING COMMENT '国家',
    province STRING COMMENT '省份',
    city     STRING COMMENT '城市'
) COMMENT '用户维度表'
    PARTITIONED BY (dt string comment '日期分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/dim_user'
;

-- 插入数据
INSERT OVERWRITE TABLE jtp_flow_topic_warehouse.dim_user PARTITION (dt)
SELECT user_id,
       ip_map['country']  AS country,
       ip_map['province'] AS province,
       ip_map['city']     AS city,
       '2025-07-31'       AS dt
FROM (SELECT *,
             default.parse_ip_location(ip_address) AS ip_map
      FROM jtp_flow_topic_warehouse.ods_user_action_log) t1
;

select *
from dim_user;

/*
 为什么会出现 "0,0,内网IP"？
内网IP地址 (Private IP Addresses)：
ip2region 库的主要功能是解析公网IP地址的地理位置信息。
你数据中的 ip_address 如果是以下这些 IP 地址，它们就是内网IP，也称为私有IP地址：

10.0.0.0 到 10.255.255.255

172.16.0.0 到 172.31.255.255

192.168.0.0 到 192.168.255.255

以及回环地址 127.0.0.1

这些 IP 地址是为局域网（LAN）内部使用而保留的，不会在公共互联网上路由。因此，ip2region 无法为它们提供具体的国家、省份和城市信息，而是会将其识别为 0|0|0|内网IP|0 这样的结果。你的 UDF 会将这个结果解析成 ip_map['country'] = '0', ip_map['province'] = '0', ip_map['city'] = '内网IP'。

 */


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