-- 创建数据库
CREATE DATABASE IF NOT EXISTS jtp_commodity_warehouse
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse';
USE jtp_commodity_warehouse;

-- 1. 全量商品信息表
DROP TABLE IF EXISTS ods_product_info_full;
CREATE TABLE IF NOT EXISTS ods_product_info_full (
    id INT COMMENT '自增主键',
    sku_id INT NOT NULL COMMENT 'SKU ID，商品的最小销售单元',
    product_id INT NOT NULL COMMENT '商品ID，用于关联同一款商品的不同SKU',
    product_name STRING NOT NULL COMMENT '商品名称',
    category_id INT NOT NULL COMMENT '商品所属类目ID',
    category_name STRING NOT NULL COMMENT '商品所属类目名称',
    brand_id INT NOT NULL COMMENT '商品所属品牌ID',
    brand_name STRING NOT NULL COMMENT '商品所属品牌名称',
    original_price DECIMAL(10, 2) NOT NULL COMMENT '商品原始价格',
    create_time TIMESTAMP NOT NULL COMMENT '商品在业务系统中的创建时间',
    ts TIMESTAMP NOT NULL COMMENT '数据同步时间戳'
)
COMMENT '全量商品信息表'
PARTITIONED BY (dt STRING COMMENT '订单日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_product_info_full';
ALTER TABLE ods_product_info_full ADD IF NOT EXISTS PARTITION (dt = '2025-08-06');
ALTER TABLE ods_product_info_full ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');



-- 2. 全量用户信息表
DROP TABLE IF EXISTS ods_user_full;
DROP TABLE IF EXISTS ods_user_full;
CREATE TABLE IF NOT EXISTS ods_user_full (
    user_id INT NOT NULL COMMENT '用户ID，唯一标识一个用户',
    username STRING NOT NULL COMMENT '用户名',
    registration_time TIMESTAMP NOT NULL COMMENT '用户的注册时间',
    gender STRING COMMENT '用户性别',
    birth_date DATE COMMENT '用户出生日期',
    city STRING COMMENT '用户所在城市',
    hobby STRING COMMENT '用户兴趣爱好'
)
COMMENT '全量用户信息表'
PARTITIONED BY (dt STRING COMMENT '订单日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_user_full';
ALTER TABLE ods_user_full ADD IF NOT EXISTS PARTITION (dt = '2025-08-06');
ALTER TABLE ods_user_full ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');

-- 3. 全量订单信息表（按创建日期分区）
DROP TABLE IF EXISTS ods_order_incr;
CREATE TABLE IF NOT EXISTS ods_order_incr (
    id INT COMMENT '自增主键',
    order_id INT NOT NULL COMMENT '订单ID',
    user_id INT NOT NULL COMMENT '下单用户ID',
    total_amount DECIMAL(12, 2) NOT NULL COMMENT '订单总金额',
    status STRING NOT NULL COMMENT '订单状态：paid, unpaid, closed',
    create_time TIMESTAMP NOT NULL COMMENT '订单创建时间',
    pay_time TIMESTAMP COMMENT '订单支付时间'
)
COMMENT '全量订单信息表'
PARTITIONED BY (dt STRING COMMENT '订单日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_order_incr';
ALTER TABLE ods_order_incr ADD IF NOT EXISTS PARTITION (dt = '2025-08-06');
ALTER TABLE ods_order_incr ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');

-- 4. 全量订单明细表（按订单日期分区）
DROP TABLE IF EXISTS ods_order_detail_full;
CREATE TABLE IF NOT EXISTS ods_order_detail_full (
    id INT COMMENT '自增主键',
    order_id INT NOT NULL COMMENT '订单ID',
    sku_id INT NOT NULL COMMENT '购买的SKU ID',
    buy_num INT NOT NULL COMMENT '购买数量',
    item_price DECIMAL(10, 2) NOT NULL COMMENT '商品单价'
)
COMMENT '全量订单明细表'
PARTITIONED BY (dt STRING COMMENT '订单日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_order_detail_full';
ALTER TABLE ods_order_detail_full ADD IF NOT EXISTS PARTITION (dt = '2025-08-06');
ALTER TABLE ods_order_detail_full ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');

select *
from ods_order_detail_full;

-- 5. 全量商品评价表（按评价日期分区）
DROP TABLE IF EXISTS ods_product_review_incr;
CREATE TABLE IF NOT EXISTS ods_product_review_incr (
    id INT COMMENT '自增主键',
    sku_id INT NOT NULL COMMENT '评价的SKU ID',
    user_id INT NOT NULL COMMENT '评价用户ID',
    order_id INT NOT NULL COMMENT '评价关联的订单ID',
    score INT NOT NULL COMMENT '评分（1-5分）',
    review_content STRING COMMENT '评价内容',
    review_time TIMESTAMP NOT NULL COMMENT '评价时间',
    is_positive INT NOT NULL COMMENT '是否为正面评价（1:是, 0:否）'
)
COMMENT '全量商品评价表'
PARTITIONED BY (dt STRING COMMENT '订单日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/ods_product_review_incr';
ALTER TABLE ods_product_review_incr ADD IF NOT EXISTS PARTITION (dt = '2025-08-06');
ALTER TABLE ods_product_review_incr ADD IF NOT EXISTS PARTITION (dt = '2025-08-07');
