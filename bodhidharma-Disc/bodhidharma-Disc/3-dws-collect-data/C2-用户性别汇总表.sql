DROP TABLE IF EXISTS bodhidharma_disc.dws_user_sex_tag;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.dws_user_sex_tag
(
    user_id BIGINT COMMENT '用户id',
    gender  VARCHAR(255) REPLACE_IF_NOT_NULL COMMENT '性别'
)ENGINE = OLAP
    AGGREGATE KEY (user_id)
DISTRIBUTED BY HASH (user_id) BUCKETS 5
PROPERTIES (
    "replication_num" = "1"
);


-- 用户行为权重
-- 性别匹配
INSERT INTO bodhidharma_disc.dws_user_sex_tag
SELECT user_id
     , CASE
           WHEN category_name LIKE '%女装%' OR category_name LIKE '%女士精品%' OR category_name LIKE '%女鞋%'
               OR category_name LIKE '%美容护肤%' OR category_name LIKE '%美体%' OR category_name LIKE '%精油%'
               OR category_name LIKE '%女士内衣%' OR category_name LIKE '%男士内衣%' OR category_name LIKE '%家居服%'
               OR category_name LIKE '%饰品%' OR category_name LIKE '%流行首饰%' OR category_name LIKE '%时尚饰品新%'
               OR category_name LIKE '%孕妇装%' OR category_name LIKE '%孕产妇用品%' OR category_name LIKE '%营养%'
               OR category_name LIKE '%童鞋%' OR category_name LIKE '%婴儿鞋%' OR category_name LIKE '%亲子鞋%'
               OR category_name LIKE '%童装%' OR category_name LIKE '%婴儿装%' OR category_name LIKE '%亲子装%'
               OR category_name LIKE '%尿片%' OR category_name LIKE '%洗护%' OR category_name LIKE '%喂哺%'
               OR category_name LIKE '%推车床%' THEN '女'
           WHEN category_name LIKE '%男装%' OR category_name LIKE '%流行男鞋%' OR category_name LIKE '%男鞋%'
               OR category_name LIKE '%运动服%' OR category_name LIKE '%休闲服装%' OR category_name LIKE '%运动鞋%'
               OR category_name LIKE '%网游装备%' OR category_name LIKE '%游戏币%' OR category_name LIKE '%帐号%'
               OR category_name LIKE '%代练%' OR category_name LIKE '%电玩%' OR category_name LIKE '%配件%'
               OR category_name LIKE '%游戏%' OR category_name LIKE '%攻略%' THEN '男'
           WHEN category_name LIKE '%居家日用%' OR category_name LIKE '%厨房电器%' OR category_name LIKE '%生活电器%'
               OR category_name LIKE '%收纳整理%' OR category_name LIKE '%居家布艺%' OR category_name LIKE '%家庭保健%'
               OR category_name LIKE '%儿童用品%' OR category_name LIKE '%家居用品%' THEN '家庭'
           ELSE '未知'
    END AS gender_type
FROM bodhidharma_disc.dwd_user_behavior_log
WHERE dt = '2025-08-10'
;

