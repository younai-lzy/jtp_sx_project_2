-- ====================================================================================================
-- DIM层（维度层）建表和数据加载脚本
-- 修复了分区变量未设置导致的插入错误
-- ====================================================================================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS jtp_commodity_warehouse
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse';
USE jtp_commodity_warehouse;

-- 设置动态分区模式和并行度
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET spark.sql.shuffle.partitions = 400;

-- ====================================================================================================
-- DIM层（维度层）建表脚本
-- ====================================================================================================

-- 1. 用户维度表 (dim_user)
DROP TABLE IF EXISTS dim_user;
CREATE TABLE IF NOT EXISTS `dim_user`
(
    `user_id`           INT COMMENT '用户ID，唯一标识一个用户，来自ods_user_full',
    `username`          STRING COMMENT '用户名',
    `registration_time` TIMESTAMP COMMENT '用户注册时间',
    `gender`            STRING COMMENT '用户性别',
    `birth_date`        DATE COMMENT '用户出生日期',
    `age`               INT COMMENT '用户年龄，可从出生日期计算得到',
    `city`              STRING COMMENT '用户所在城市',
    `hobby`             STRING COMMENT '用户兴趣爱好'
)
    COMMENT '用户维度表'
    PARTITIONED BY (`dt` STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/dim_user';


-- 2. 商品维度表 (dim_product)
DROP TABLE IF EXISTS dim_product;
CREATE TABLE IF NOT EXISTS `dim_product`
(
    `sku_id`         INT COMMENT 'SKU ID，商品的最小销售单元，来自ods_product_info_full',
    `product_id`     INT COMMENT '商品ID',
    `product_name`   STRING COMMENT '商品名称',
    `category_id`    INT COMMENT '商品所属类目ID',
    `category_name`  STRING COMMENT '商品所属类目名称',
    `brand_id`       INT COMMENT '商品所属品牌ID',
    `brand_name`     STRING COMMENT '商品所属品牌名称',
    `original_price` DECIMAL(10, 2) COMMENT '商品原始价格',
    `create_time`    TIMESTAMP COMMENT '商品在业务系统中的创建时间'
)
    COMMENT '商品维度表'
    PARTITIONED BY (`dt` STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/dim_product';


-- 3. 渠道维度表 (dim_channel)
DROP TABLE IF EXISTS dim_channel;
CREATE TABLE IF NOT EXISTS `dim_channel`
(
    `channel_id`   INT COMMENT '渠道ID，自增主键',
    `channel_name` STRING COMMENT '渠道名称'
)
    COMMENT '渠道维度表'
    PARTITIONED BY (`dt` STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/dim_channel';


-- ====================================================================================================
-- DIM层（维度层）数据加载脚本
-- 修复：在插入前设置biz_date变量
-- ====================================================================================================

-- 插入数据之前，需要设置分区变量 `biz_date`
-- 你可以根据实际的业务日期进行修改，例如：'2025-01-01'


-- 插入数据到 dim_user (Hive SQL)
INSERT OVERWRITE TABLE `dim_user` PARTITION(dt='2025-08-07')
SELECT
    user_id,
    username,
    registration_time,
    gender,
    birth_date,
    -- 在Hive中计算年龄，DATEDIFF函数可以用于计算天数
    CAST(DATEDIFF(CURRENT_DATE(), birth_date) / 365.25 AS INT) AS age,
    city,
    hobby
FROM
    ods_user_full;


-- 插入数据到 dim_product (Hive SQL)
INSERT OVERWRITE TABLE `dim_product` PARTITION(dt='2025-08-07')
SELECT
    sku_id,
    product_id,
    product_name,
    category_id,
    category_name,
    brand_id,
    brand_name,
    original_price,
    create_time
FROM
    ods_product_info_full;


-- 插入数据到 dim_channel (Hive SQL)
INSERT OVERWRITE TABLE `dim_channel` PARTITION(dt='2025-08-07')
SELECT
    row_number() OVER (ORDER BY source_channel) AS channel_id,
    source_channel
FROM
    (SELECT DISTINCT source_channel FROM ods_user_action_log) AS a;
