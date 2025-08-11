CREATE DATABASE IF NOT EXISTS bodhidharma_disc;
USE bodhidharma_disc;

-- 用户加购表
DROP TABLE IF EXISTS bodhidharma_disc.dwd_user_add_cart;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.dwd_user_add_cart
(
    `dt`                DATE            COMMENT '日期分区',
    `log_id`            BIGINT          COMMENT '日志唯一ID',
    `user_id`           BIGINT          COMMENT '用户唯一ID',
    `item_id`           BIGINT          COMMENT '商品ID',
    `category_id`       INT             COMMENT '商品类目ID',
    `category_name`     VARCHAR(255)          COMMENT '商品类目名称',
    `brand_id`          INT             COMMENT '品牌ID',
    `brand_name`        VARCHAR(255)          COMMENT '品牌名称',
    `price`             DECIMAL(10,2)   COMMENT '商品价格',
    `quantity`          INT             COMMENT '购买数量',
    `add_cart_time`     DATETIME        COMMENT '加购时间',
    `platform`          VARCHAR(255)          COMMENT '平台类型',
    `os_version`        VARCHAR(255)          COMMENT '操作系统版本',
    `device_model`      VARCHAR(255)          COMMENT '设备型号',
    `network_type`      VARCHAR(255)          COMMENT '网络类型',
    `ip_address`        VARCHAR(255)          COMMENT 'IP地址',
    `province`          VARCHAR(255)          COMMENT '省份',
    `city`              VARCHAR(255)          COMMENT '城市',
    `channel`           VARCHAR(255)          COMMENT '渠道来源',
    `referer`           VARCHAR(255)          COMMENT '来源页面URL',
    `search_keyword`    VARCHAR(255)          COMMENT '搜索关键词'
)
-- 修正 DUPLICATE KEY，使用 dt 和 log_id
    DUPLICATE KEY(dt, log_id)
COMMENT '用户加购表'
PARTITION BY RANGE (dt) (
    -- 调整分区定义以涵盖2025年8月
    PARTITION p202508 VALUES LESS THAN ('2025-09-01'),
    PARTITION p202509 VALUES LESS THAN ('2025-10-01'),
    PARTITION p202510 VALUES LESS THAN ('2025-11-01')
)
DISTRIBUTED BY HASH(log_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);

-- 导入数据到 dwd_user_add_cart 表
-- 从 ods_user_behavior_log 表中筛选出行为类型为“加购”（behavior_type = 4）的数据
INSERT INTO bodhidharma_disc.dwd_user_add_cart
SELECT
    -- 对dt字段也使用 STR_TO_DATE 进行更安全的转换
    STR_TO_DATE(NULLIF(log.dt, ''), '%Y-%m-%d') as dt,
    CAST(NULLIF(log.log_id, '') AS BIGINT) as log_id,
    CAST(NULLIF(log.user_id, '') AS BIGINT) as user_id,
    CAST(NULLIF(log.item_id, '') AS BIGINT) as item_id,
    -- 通过 JOIN 引入 ods_product_info 表中的 category_id
    CAST(NULLIF(prod.category_id, '') AS INT) as category_id,
    -- category_name 字段也从 ods_product_info 表中获取
    prod.category_name,
    CAST(NULLIF(log.brand_id, '') AS INT) as brand_id,
    log.brand_name,
    CAST(NULLIF(log.price, '') AS DECIMAL(10, 2)) as price,
    CAST(NULLIF(log.quantity, '') AS INT) as quantity,
    -- 将时间戳（BIGINT）转换为DATETIME格式
    FROM_UNIXTIME(CAST(NULLIF(log.behavior_time, '') AS BIGINT)) as add_cart_time,
    log.platform,
    log.os_version,
    log.device_model,
    log.network_type,
    log.ip_address,
    log.province,
    log.city,
    log.channel,
    log.referer,
    log.search_keyword
FROM hive_catalog.bodhidharma_disc.ods_user_behavior_log AS log
         LEFT JOIN hive_catalog.bodhidharma_disc.ods_product_info AS prod
-- 修正了 JOIN 条件，增加了 log.dt = prod.dt，以确保数据一致性
                   ON log.item_id = prod.item_id AND log.dt = prod.dt
WHERE log.dt = '2025-08-10'
  AND log.behavior_type = 4;

