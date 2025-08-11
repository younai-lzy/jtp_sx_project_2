DROP
CATALOG IF EXISTS hive_catalog;
CREATE
CATALOG IF NOT EXISTS hive_catalog PROPERTIES (
    'type'='hms', -- required
    'hive.metastore.type' = 'hms', -- optional
    'hive.version' = '3.1.2', -- optional
    'fs.defaultFS' = 'hdfs://node101:8020', -- optional
    'hive.metastore.uris' = 'thrift://node101:9083'
);

USE bodhidharma_disc;

-- 首先删除旧表（如果存在）
DROP TABLE IF EXISTS bodhidharma_disc.dws_user_height_tag;

-- 创建Doris用户身高标签表
CREATE TABLE IF NOT EXISTS bodhidharma_disc.dws_user_height_tag
(
    user_id       BIGINT COMMENT '用户ID',
    height_value  INT COMMENT '精确身高值，单位：厘米',
    height_source VARCHAR(255) COMMENT '身高数据来源',
    update_time   DATETIME COMMENT '数据更新时间'
)
    ENGINE = OLAP
-- 使用DUPLICATE KEY模型，允许导入重复数据，由业务逻辑处理更新
    DUPLICATE KEY (user_id)
-- 按照user_id进行HASH分桶，确保相同用户的数据分布在同一个桶中，方便查询
DISTRIBUTED BY HASH(user_id) BUCKETS 5
-- 使用ZSTD进行压缩，以节省存储空间
PROPERTIES (
    "replication_num" = "1",
    "compression" = "ZSTD"
);

-- 根据 ods_user_info 表数据，提取并清洗身高值后插入到 dws_user_height_tag 表
-- 注意：这个SQL只实现了简单的过滤逻辑，复杂的加权和补全等需在上游ETL中实现。
INSERT INTO bodhidharma_disc.dws_user_height_tag (user_id, height_value, height_source, update_time)
SELECT
    t1.user_id,
    -- 过滤异常值（<100cm 或 >250cm）和特殊数字（如 666, 888）
    CASE
        WHEN t1.user_height > 100 AND t1.user_height < 250 AND t1.user_height NOT IN (666, 888) THEN t1.user_height
        ELSE NULL
        END AS height_value,
    -- 假设 user_info 表为实名认证数据来源
    '实名认证' AS height_source,
    NOW() AS update_time
FROM
    hive_catalog.bodhidharma_disc.ods_user_info t1
WHERE
    t1.user_height IS NOT NULL AND t1.user_height > 0;