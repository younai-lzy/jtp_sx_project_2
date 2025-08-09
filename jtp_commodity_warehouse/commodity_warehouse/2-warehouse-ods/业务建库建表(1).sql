CREATE DATABASE IF NOT EXISTS jtp_commodity_warehouse
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse';
USE jtp_commodity_warehouse;

-- DDL for ods_user_full
-- 用户全量信息表
DROP TABLE IF EXISTS ods_user_full;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_user_full
(
    user_id           BIGINT COMMENT '用户ID，唯一标识一个用户',
    username          STRING COMMENT '用户名',
    registration_time TIMESTAMP COMMENT '用户的注册时间',
    gender            STRING COMMENT '用户性别',
    birth_date        DATE COMMENT '用户出生日期',
    city              STRING COMMENT '用户所在城市',
    hobby             STRING COMMENT '用户兴趣爱好'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    COMMENT '全量用户信息表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_user_full';
;

ALTER TABLE ods_user_full
    ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');

-- DDL for ods_product_info_full
-- 全量商品信息表
DROP TABLE IF EXISTS ods_product_info_full;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_info_full
(
    id             BIGINT COMMENT '自增主键',
    sku_id         BIGINT COMMENT 'SKU ID',
    product_id     BIGINT COMMENT '商品ID',
    product_name   STRING COMMENT '商品名称',
    category_id    BIGINT COMMENT '商品所属类目ID',
    category_name  STRING COMMENT '商品所属类目名称',
    brand_id       BIGINT COMMENT '商品所属品牌ID',
    brand_name     STRING COMMENT '商品所属品牌名称',
    original_price DECIMAL(10, 2) COMMENT '商品原始价格',
    create_time    TIMESTAMP COMMENT '商品在业务系统中的创建时间',
    color_category STRING COMMENT '颜色分类',
    ts             TIMESTAMP COMMENT '数据同步时间戳'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    COMMENT '全量商品信息表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_product_info_full';
;

ALTER TABLE ods_product_info_full ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');

-- DDL for ods_order_incr
-- 全量订单信息表
DROP TABLE IF EXISTS ods_order_incr;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_order_incr
(
    id           BIGINT COMMENT '自增主键',
    order_id     BIGINT COMMENT '订单ID',
    user_id      BIGINT COMMENT '下单用户ID',
    total_amount DECIMAL(12, 2) COMMENT '订单总金额',
    status       STRING COMMENT '订单状态',
    create_time  TIMESTAMP COMMENT '订单创建时间',
    pay_time     TIMESTAMP COMMENT '订单支付时间'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    COMMENT '全量订单信息表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_order_incr';
;

ALTER TABLE ods_order_incr ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');

-- DDL for ods_order_detail_full
-- 全量订单明细表
DROP TABLE IF EXISTS ods_order_detail_full;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_order_detail_full
(
    id         BIGINT COMMENT '自增主键',
    order_id   BIGINT COMMENT '订单ID',
    sku_id     BIGINT COMMENT '购买的SKU ID',
    buy_num    INT COMMENT '购买数量',
    item_price DECIMAL(10, 2) COMMENT '商品单价'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    COMMENT '全量订单明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_order_detail_full';
;

ALTER TABLE ods_order_detail_full ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');

-- DDL for ods_price_trend_log
-- 商品价格变动日志表
DROP TABLE IF EXISTS ods_price_trend_log;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_price_trend_log
(
    id           BIGINT COMMENT '自增主键',
    sku_id       BIGINT COMMENT 'SKU ID',
    price_before DECIMAL(10, 2) COMMENT '价格变动前',
    price_after  DECIMAL(10, 2) COMMENT '价格变动后',
    change_time  TIMESTAMP COMMENT '价格变动发生的时间'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    COMMENT '商品价格变动日志表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_price_trend_log';
;

ALTER TABLE ods_price_trend_log ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');
-- DDL for ods_product_review_incr
-- 全量商品评价表
DROP TABLE IF EXISTS ods_product_review_incr;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_review_incr
(
    id             BIGINT COMMENT '自增主键',
    sku_id         BIGINT COMMENT '评价的SKU ID',
    user_id        BIGINT COMMENT '评价用户ID',
    order_id       BIGINT COMMENT '评价关联的订单ID',
    score          INT COMMENT '评分（1-5分）',
    review_content STRING COMMENT '评价内容',
    review_time    TIMESTAMP COMMENT '评价时间',
    is_positive    TINYINT COMMENT '是否为正面评价（1:是, 0:否）'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    COMMENT '全量商品评价表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_product_review_incr';
;
ALTER TABLE ods_product_review_incr ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');

-- DDL for ods_user_action_log
-- 用户行为日志表，按 dt 字段分区
DROP TABLE IF EXISTS ods_user_action_log;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_user_action_log
(
    log_id         BIGINT COMMENT '日志ID，唯一标识一条行为记录',
    session_id     STRING COMMENT '会话ID',
    user_id        INT COMMENT '用户ID',
    sku_id         INT COMMENT '用户行为相关的SKU ID',
    content_id     INT COMMENT '用户行为相关的内容ID',
    content_type   STRING COMMENT '内容类型：live, short_video, graphic',
    action_type    STRING COMMENT '行为类型',
    source_channel STRING COMMENT '流量来源渠道',
    log_timestamp  BIGINT COMMENT '行为发生时的时间戳'
)
    COMMENT '用户行为日志表'
    PARTITIONED BY (dt STRING COMMENT '日志日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_user_action_log';
;
ALTER TABLE ods_user_action_log ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');

