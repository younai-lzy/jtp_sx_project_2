-- 设置 Hive Catalog
DROP CATALOG IF EXISTS hive_catalog;
CREATE CATALOG IF NOT EXISTS hive_catalog PROPERTIES (
    'type'='hms', -- required
    'hive.metastore.type' = 'hms', -- optional
    'hive.version' = '3.1.2', -- optional
    'fs.defaultFS' = 'hdfs://node101:8020', -- optional
    'hive.metastore.uris' = 'thrift://node101:9083'
);

-- 切换到 Hive Catalog
SWITCH hive_catalog;
SHOW CATALOGS;

-- 使用正确的数据库
CREATE DATABASE IF NOT EXISTS bodhidharma_disc;
USE bodhidharma_disc;

-- 交易订单明细事实表
DROP TABLE IF EXISTS bodhidharma_disc.`dwd_transaction_order_info`;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.`dwd_transaction_order_info`
(
    `dt`                DATE COMMENT '日期分区',
    `order_id`          BIGINT COMMENT '订单ID',
    `user_id`           BIGINT COMMENT '用户ID',
    `order_time`        DATETIME COMMENT '下单时间',
    `pay_time`          DATETIME COMMENT '支付时间',
    `pay_amount`        DECIMAL(10, 2) COMMENT '支付金额',
    `pay_status`        BIGINT COMMENT '支付状态:0-未支付,1-已支付,2-支付失败',
    `delivery_time`     DATETIME COMMENT '发货时间',
    `size_choice`       VARCHAR(255) COMMENT '尺码选择',
    `learning_products` VARCHAR(255) COMMENT '智能硬件（体重秤、健康类商品、）',
    `is_member`         BOOLEAN COMMENT '是否会员:0-非会员,1-会员',
    `activity_id`       BIGINT COMMENT '活动ID'
)
-- 修正 DUPLICATE KEY，使用 dt 和 order_id
    DUPLICATE KEY(dt, order_id)
COMMENT '交易订单明细事实表'
-- 修正 PARTITION BY，使用新增的 dt 列
PARTITION BY RANGE (dt) (
    -- 修正分区定义，使其能够包含 '2025-08-10' 的数据
    PARTITION p202508 VALUES LESS THAN ('2025-08-11')
)
-- 修正 DISTRIBUTED BY，使用 order_id
DISTRIBUTED BY HASH(order_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);

-- 导入数据到 dwd_transaction_order_info 表
INSERT INTO bodhidharma_disc.`dwd_transaction_order_info`
SELECT
    -- 增加对 dt, order_id, user_id, pay_status 和 activity_id 的显式类型转换
    STR_TO_DATE(NULLIF(dt, ''), '%Y-%m-%d') as dt,
    CAST(NULLIF(order_id, '') AS BIGINT) as order_id,
    CAST(NULLIF(user_id, '') AS BIGINT) as user_id,
    -- 使用 NULLIF 确保空字符串转为NULL，然后STR_TO_DATE可以处理
    STR_TO_DATE(NULLIF(order_time, ''), '%Y-%m-%d %H:%i:%s') as order_time,
    STR_TO_DATE(NULLIF(pay_time, ''), '%Y-%m-%d %H:%i:%s') as pay_time,
    -- 增加对pay_amount字段的显式类型转换
    CAST(NULLIF(pay_amount, '') AS DECIMAL(10, 2)) as pay_amount,
    CAST(NULLIF(pay_status, '') AS BIGINT) as pay_status,
    STR_TO_DATE(NULLIF(delivery_time, ''), '%Y-%m-%d %H:%i:%s') as delivery_time,
    size_choice,
    learning_products,
    -- 使用CASE语句更稳健地处理is_member字段
    CASE WHEN is_member = TRUE THEN TRUE ELSE FALSE END as is_member,
    CAST(NULLIF(activity_id, '') AS BIGINT) as activity_id
FROM hive_catalog.bodhidharma_disc.ods_order_info
WHERE dt = '2025-08-10'
;
