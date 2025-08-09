DROP CATALOG IF EXISTS hive_catalog;
CREATE CATALOG IF NOT EXISTS hive_catalog PROPERTIES (
  'type'='hms',
  'hive.metastore.type' = 'hms',
  'hive.version' = '3.1.2',
  'fs.defaultFS' = 'hdfs://node101:8020',
  'hive.metastore.uris' = 'thrift://node101:9083'
);

SHOW DATABASES ;

USE jtp_gd03_warehouse;

SHOW TABLES ;

DROP TABLE IF EXISTS jtp_gd03_warehouse.service_experience_analysis;
CREATE TABLE jtp_gd03_warehouse.service_experience_analysis (
    -- 基础维度
    dt DATE COMMENT '数据日期（关联dts）',
    sku_id INT COMMENT '商品SKU ID',
    product_name VARCHAR(200) COMMENT '商品名称',
    -- 商品评分分析指标
    overall_rating VARCHAR(200) COMMENT '整体评分（1-5分）',
    old_buyer_rating VARCHAR(200) COMMENT '老买家评分（1-5分）',
    rating_distribution VARCHAR(200) COMMENT '不同分数层评价数（如"1分:5,2分:10"）',
    old_buyer_rating_distribution VARCHAR(200) COMMENT '老买家不同分数层评价数',
    -- 分区与存储
    dt_part VARCHAR(200) COMMENT '分区日期（YYYY-MM-DD）'
)   ENGINE = OLAP
    DUPLICATE KEY (dt,sku_id)
    COMMENT '客群分析'
    DISTRIBUTED BY HASH(dt,sku_id) BUCKETS 10
    PROPERTIES (
        "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
        "storage_format" = "V2"   -- 推荐的高效存储格式
    );

INSERT INTO jtp_gd03_warehouse.service_experience_analysis
-- 步骤2.1：商品评分分析（整体+老买家）
SELECT
    CAST(dts AS DATE) AS dt,
    sku_id,
    product_name,
    dts AS dt_part,
    AVG(score) AS overall_rating,
    AVG(CASE WHEN is_old_buyer = 1 THEN score ELSE NULL END) AS old_buyer_rating,
    CONCAT(
            '1分:', COUNT(CASE WHEN score=1 THEN 1 END), ',',
            '2分:', COUNT(CASE WHEN score=2 THEN 1 END), ',',
            '3分:', COUNT(CASE WHEN score=3 THEN 1 END), ',',
            '4分:', COUNT(CASE WHEN score=4 THEN 1 END), ',',
            '5分:', COUNT(CASE WHEN score=5 THEN 1 END)
        ) AS rating_distribution,
    CONCAT(
            '1分:', COUNT(CASE WHEN score=1 AND is_old_buyer=1 THEN 1 END), ',',
            '2分:', COUNT(CASE WHEN score=2 AND is_old_buyer=1 THEN 1 END), ',',
            '3分:', COUNT(CASE WHEN score=3 AND is_old_buyer=1 THEN 1 END), ',',
            '4分:', COUNT(CASE WHEN score=4 AND is_old_buyer=1 THEN 1 END), ',',
            '5分:', COUNT(CASE WHEN score=5 AND is_old_buyer=1 THEN 1 END)
        ) AS old_buyer_rating_distribution
FROM hive_catalog.jtp_gd03_warehouse.dwd_aggregated_wide
WHERE  sku_id IS NOT NULL
GROUP BY CAST(dts AS DATE), sku_id, product_name, dts;


SELECT * FROM jtp_gd03_warehouse.service_experience_analysis;


DROP TABLE IF EXISTS jtp_gd03_warehouse.service_experience_analysis2;
CREATE TABLE jtp_gd03_warehouse.service_experience_analysis2 (
    -- 基础维度
    dt DATE COMMENT '数据日期（关联dts）',
    sku_id INT COMMENT '商品SKU ID',
    -- 评价指标趋势
    positive_review_count INT COMMENT '正面评价数',
    negative_review_count INT COMMENT '负面评价数',
    -- 分区与存储
    dt_part VARCHAR(200) COMMENT '分区日期（YYYY-MM-DD）'
)   ENGINE = OLAP
    DUPLICATE KEY (dt,sku_id)
    COMMENT '客群分析'
    DISTRIBUTED BY HASH(dt,sku_id) BUCKETS 10
    PROPERTIES (
        "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
        "storage_format" = "V2"   -- 推荐的高效存储格式
    );

INSERT INTO jtp_gd03_warehouse.service_experience_analysis2
-- 步骤2.2：评价指标趋势（正面/负面）
SELECT
    CAST(dts AS DATE) AS dt,
    sku_id,
    dts AS dt_part,
    COUNT(CASE WHEN is_positive=1 THEN 1 END) AS positive_review_count,
    COUNT(CASE WHEN is_positive=0 THEN 1 END) AS negative_review_count
FROM hive_catalog.jtp_gd03_warehouse.dwd_aggregated_wide
WHERE sku_id IS NOT NULL
GROUP BY CAST(dts AS DATE), sku_id, dts;

SELECT * FROM jtp_gd03_warehouse.service_experience_analysis2;



DROP TABLE IF EXISTS jtp_gd03_warehouse.service_experience_analysis3;
CREATE TABLE jtp_gd03_warehouse.service_experience_analysis3 (
    -- 基础维度
    dt DATE COMMENT '数据日期（关联dts）',
    sku_id INT COMMENT '商品SKU ID',
    -- 评价内容分析
    color_category VARCHAR(200) COMMENT '评价内容颜色分类（如"红色:20,蓝色:15"）',
    review_content_text VARCHAR(500) COMMENT '评价内容文本（拼接Top10高频评价）',
    -- 分区与存储
    dt_part VARCHAR(200) COMMENT '分区日期（YYYY-MM-DD）'
)   ENGINE = OLAP
    DUPLICATE KEY (dt,sku_id)
    COMMENT '客群分析'
    DISTRIBUTED BY HASH(dt,sku_id) BUCKETS 10
    PROPERTIES (
        "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
        "storage_format" = "V2"   -- 推荐的高效存储格式
    );

INSERT INTO jtp_gd03_warehouse.service_experience_analysis3
-- 步骤2.3：评价内容分析（颜色+内容）
SELECT
    CAST(dts AS DATE) AS dt,
    sku_id,
    dts AS dt_part,
    CONCAT(
            '红色:', COUNT(CASE WHEN review_content RLIKE '红色|RED' THEN 1 END), ',',
            '蓝色:', COUNT(CASE WHEN review_content RLIKE '蓝色|BLUE' THEN 1 END), ',',
            '黑色:', COUNT(CASE WHEN review_content RLIKE '黑色|BLACK' THEN 1 END), ',',
            '其他:', COUNT(CASE WHEN review_content NOT RLIKE '红色|BLUE|BLACK' THEN 1 END)
        ) AS color_category,
    -- Doris中GROUP_CONCAT的分隔符通过第二个参数指定，而非SEPARATOR关键字
    SUBSTRING(GROUP_CONCAT(review_content, ' | '), 1, 500) AS review_content_text
FROM hive_catalog.jtp_gd03_warehouse.dwd_aggregated_wide
WHERE sku_id IS NOT NULL
GROUP BY CAST(dts AS DATE), sku_id, dts;

SELECT * FROM jtp_gd03_warehouse.service_experience_analysis3;



