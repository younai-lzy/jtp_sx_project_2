-- 1. 创建数据库
DROP DATABASE IF EXISTS jtp_oms_dwd CASCADE;
DROP DATABASE IF EXISTS jtp_oms_dws CASCADE;
DROP DATABASE IF EXISTS jtp_oms_ads CASCADE;
CREATE DATABASE IF NOT EXISTS jtp_oms_dwd
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_dwd';
CREATE DATABASE IF NOT EXISTS jtp_oms_dws
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_dws';
CREATE DATABASE IF NOT EXISTS jtp_oms_ads
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_ads';

-- 2. 创建表
DROP TABLE IF EXISTS jtp_oms_dwd.dwd_oms_order_incr;
CREATE TABLE jtp_oms_dwd.dwd_oms_order_incr
(
    id           BIGINT COMMENT '主键ID',
    total_amount DECIMAL(16, 2) COMMENT '订单总金额',
    create_time  STRING COMMENT '下单日期时间'
) COMMENT 'OMS系统交易订单表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_dwd/dwd_oms_order_incr'
;

-- 查看分区和数据
SHOW PARTITIONS jtp_oms_dwd.dwd_oms_order_incr;

SELECT *
FROM jtp_oms_dwd.dwd_oms_order_incr
WHERE dt = '2024-01-01'
LIMIT 10;
SELECT count(id) AS cnt
FROM jtp_oms_dwd.dwd_oms_order_incr;


-- ==================================================================================
-- 3. 生成模拟数据
-- ==================================================================================
SET spark.sql.storeAssignmentPolicy=LEGACY;
SET hive.exec.dynamic.partition.mode=nonstrict;
WITH simulated_data AS (SELECT
                            -- 生成主键 id，使用 row_number() 函数生成自增的行号作为主键
                            row_number() OVER (ORDER BY i)   AS id,
                            -- 生成下单日期时间，范围从 '2023-01-01 00:00:00' 到 '2024-12-31 23:59:59'
                            from_unixtime(
                                        unix_timestamp('2023-01-01 00:00:00') + floor(rand() *
                                                                                      (unix_timestamp('2024-12-31 23:59:59') -
                                                                                       unix_timestamp('2023-01-01 00:00:00')))
                                )                            AS create_time,
                            -- 生成订单总金额，范围从 10 到 1000 的随机数
                            floor(rand() * (1000 - 10) + 10) AS total_amount
                        FROM (
                                 -- 生成足够多的行里生成 1000 行数据
                                 SELECT posexplode(split(space(1000000), ' ')) AS (i, dummy)) t)
INSERT
OVERWRITE
TABLE
jtp_oms_dwd.dwd_oms_order_incr
PARTITION
(
dt
)
SELECT id,
       CAST(total_amount AS DECIMAL(16, 2))   AS total_amount,
       create_time,
       date_format(create_time, 'yyyy-MM-dd') AS dt
FROM simulated_data;


