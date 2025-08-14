
-- 1. 创建数据库
CREATE DATABASE IF NOT EXISTS jtp_oms_dwd LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_dwd';
CREATE DATABASE IF NOT EXISTS jtp_oms_dws LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_dws';
CREATE DATABASE IF NOT EXISTS jtp_oms_ads LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_ads';

-- 2. 创建表
DROP TABLE IF EXISTS jtp_oms_dwd.dwd_oms_order_item_incr ;
CREATE TABLE jtp_oms_dwd.dwd_oms_order_item_incr
(
    id                  BIGINT COMMENT '主键ID',
    member_id           STRING COMMENT '会员ID',
    product_id          STRING COMMENT '商品ID',
    product_brand       STRING COMMENT '品牌ID',
    product_category_id STRING COMMENT '品类ID',
    product_quantity    BIGINT COMMENT '商品数量',
    product_price       DECIMAL(16, 2) COMMENT '商品价格',
    create_time         STRING COMMENT '下单日期时间'
) COMMENT 'OMS系统交易订单商品明细表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_dwd/dwd_oms_order_item_incr'
;


-- 查看分区和数据
SHOW PARTITIONS jtp_oms_dwd.dwd_oms_order_item_incr;
SELECT * FROM jtp_oms_dwd.dwd_oms_order_item_incr LIMIT 10;
SELECT count(id) AS cnt FROM jtp_oms_dwd.dwd_oms_order_item_incr ;


-- ==================================================================================
-- 3. 生成模拟数据
-- ==================================================================================
SET spark.sql.storeAssignmentPolicy=LEGACY;
SET hive.exec.dynamic.partition.mode=nonstrict;
WITH simulated_data AS (
    SELECT
        -- 生成主键 id，使用 row_number() 函数生成自增的行号作为主键
        row_number() OVER (ORDER BY i) AS id,
        -- 生成下单日期时间，范围从 '2020-01-01 00:00:00' 到 '2024-12-31 23:59:59'
        from_unixtime(
                unix_timestamp('2024-07-01 00:00:00') + floor(rand() * (unix_timestamp('2024-12-31 23:59:59') - unix_timestamp('2024-07-01 00:00:00')))
        ) AS create_time,
        -- 生成商品 ID，范围从 1 到 500 的随机整数
        floor(rand() * 500) + 1 AS product_id,
        -- 生成品牌 ID，范围从 1 到 30 的随机整数
        floor(rand() * 30) + 1 AS brand_id,
        -- 生成品类 ID，范围从 1 到 20 的随机整数
        floor(rand() * 20) + 1 AS category_id,
        -- 生成用户 ID，范围从 1 到 2000 的随机整数
        floor(rand() * 2000) + 1 AS member_id,
        -- 生成商品数量，范围从 1 到 10 的随机整数
        floor(rand() * 10) + 1 AS product_quantity,
        -- 生成商品单价，范围从 10 到 100 的随机浮点数，保留两位小数
        round((rand() * (100 - 10) + 10), 2) AS unit_price
    FROM
        (
            -- 生成足够多的行，生成 1000 行数据
            SELECT posexplode(split(space(1000000), ' ')) AS (i, dummy)
        ) t
)
INSERT OVERWRITE TABLE jtp_oms_dwd.dwd_oms_order_item_incr PARTITION (dt)
SELECT
    id
     , concat('u_', member_id) AS member_id
     , concat('item_', product_id) AS product_id
     , concat('brand_', brand_id) AS brand_id
     , concat('category_', category_id) AS category_id
     , product_quantity
     , unit_price
     , create_time
     , date_format(create_time, 'yyyy-MM-dd') AS dt
FROM
    simulated_data;


