DROP CATALOG IF EXISTS hive_catalog;
CREATE CATALOG IF NOT EXISTS hive_catalog PROPERTIES (
    'type'='hms', -- required
    'hive.metastore.type' = 'hms', -- optional
    'hive.version' = '3.1.2', -- optional
    'fs.defaultFS' = 'hdfs://node101:8020', -- optional
    'hive.metastore.uris' = 'thrift://node101:9083'
);

USE bodhidharma_disc;

DROP TABLE IF EXISTS bodhidharma_disc.dws_user_age_tag
CREATE TABLE IF NOT EXISTS bodhidharma_disc.dws_user_age_tag
(
    user_id BIGINT COMMENT '用户ID',
    age_tag VARCHAR(255) COMMENT '年龄',
    age_range VARCHAR(255) COMMENT '年龄范围'
)ENGINE = OLAP
    DUPLICATE KEY (user_id)
DISTRIBUTED BY HASH (user_id) BUCKETS 5
PROPERTIES (
    "replication_num" = "1"
);

-- 淘宝平台用户行为日志数据（包括但不限于点击、浏览、搜索、
-- 收藏、加购、下单、支付、商品评价等）、商品类目属性数据
-- 年龄段分为 6 类：18-24 岁、25-29 岁、30-34 岁、35-39 岁、40-49 岁、50 岁以上。
-- 用户年龄标签
INSERT INTO bodhidharma_disc.dws_user_age_tag
SELECT
    t1.user_id,
    -- 计算年龄
    YEAR(CURRENT_DATE()) - YEAR(t1.user_birth_of_date) AS age_tag,
    -- 根据年龄计算年龄段标签
    CASE
        WHEN YEAR(CURRENT_DATE()) - YEAR(t1.user_birth_of_date) BETWEEN 18 AND 24 THEN '18-24 岁'
        WHEN YEAR(CURRENT_DATE()) - YEAR(t1.user_birth_of_date) BETWEEN 25 AND 29 THEN '25-29 岁'
        WHEN YEAR(CURRENT_DATE()) - YEAR(t1.user_birth_of_date) BETWEEN 30 AND 34 THEN '30-34 岁'
        WHEN YEAR(CURRENT_DATE()) - YEAR(t1.user_birth_of_date) BETWEEN 35 AND 39 THEN '35-39 岁'
        WHEN YEAR(CURRENT_DATE()) - YEAR(t1.user_birth_of_date) BETWEEN 40 AND 49 THEN '40-49 岁'
        WHEN YEAR(CURRENT_DATE()) - YEAR(t1.user_birth_of_date) >= 50 THEN '50 岁以上'
        ELSE '未知'
        END AS age_range
FROM
    hive_catalog.bodhidharma_disc.user_info t1;

