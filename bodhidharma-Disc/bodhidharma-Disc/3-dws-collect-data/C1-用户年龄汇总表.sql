DROP TABLE IF EXISTS bodhidharma_disc.dws_user_age_tag;
CREATE TABLE bodhidharma_disc.dws_user_age_tag
(
    user_id   BIGINT COMMENT '用户ID',
    age_range VARCHAR(255) REPLACE_IF_NOT_NULL COMMENT '年龄段'
)ENGINE = OLAP
    AGGREGATE KEY (user_id)
DISTRIBUTED BY HASH (user_id) BUCKETS 5
PROPERTIES (
    "replication_num" = "1"
);


-- 若用户资料中存在有效出生年份，直接按当前年份-出生年份计算年龄并归类。
INSERT INTO bodhidharma_disc.dws_user_age_tag
SELECT user_id,
       CASE
           WHEN (YEAR(CURRENT_DATE) - YEAR(user_birth_of_date)) BETWEEN 18 AND 24 THEN '18-24岁'
           WHEN (YEAR(CURRENT_DATE) - YEAR(user_birth_of_date)) BETWEEN 25 AND 29 THEN '25-29岁'
           WHEN (YEAR(CURRENT_DATE) - YEAR(user_birth_of_date)) BETWEEN 30 AND 34 THEN '30-34岁'
           WHEN (YEAR(CURRENT_DATE) - YEAR(user_birth_of_date)) BETWEEN 35 AND 39 THEN '35-39岁'
           WHEN (YEAR(CURRENT_DATE) - YEAR(user_birth_of_date)) BETWEEN 40 AND 49 THEN '40-49岁'
           WHEN (YEAR(CURRENT_DATE) - YEAR(user_birth_of_date)) >= 50 THEN '50岁以上'
           ELSE '未知'
           END AS age_range
FROM bodhidharma_disc.dwd_user_behavior_log
WHERE YEAR(user_birth_of_date) IS NOT NULL
;


WITH behavior_age_weight AS (SELECT '潮流服饰' AS behavior_key, '18-24岁' AS age_group, 0.9 AS weight
                             UNION ALL
                             SELECT '家居用品', '18-24岁', 0.2
                             UNION ALL
                             SELECT '健康食品', '18-24岁', 0.1
                             UNION ALL
                             SELECT '潮流服饰' AS behavior_key, '25-29岁' AS age_group, 0.8 AS weight
                             UNION ALL
                             SELECT '家居用品', '25-29岁', 0.4
                             UNION ALL
                             SELECT '健康食品', '25-29岁', 0.2
                             UNION ALL
                             SELECT '潮流服饰' AS behavior_key, '30-34岁' AS age_group, 0.6 AS weight
                             UNION ALL
                             SELECT '家居用品', '30-34岁', 0.6
                             UNION ALL
                             SELECT '健康食品', '30-34岁', 0.4
                             UNION ALL
                             SELECT '潮流服饰' AS behavior_key, '35-39岁' AS age_group, 0.4 AS weight
                             UNION ALL
                             SELECT '家居用品', '35-39岁', 0.8
                             UNION ALL
                             SELECT '健康食品', '35-39岁', 0.6
                             UNION ALL
                             SELECT '潮流服饰' AS behavior_key, '40-49岁' AS age_group, 0.2 AS weight
                             UNION ALL
                             SELECT '家居用品', '40-49岁', 0.9
                             UNION ALL
                             SELECT '健康食品', '40-49岁', 0.8
                             UNION ALL
                             SELECT '潮流服饰' AS behavior_key, '50岁以上' AS age_group, 0.1 AS weight
                             UNION ALL
                             SELECT '家居用品', '50岁以上', 0.7
                             UNION ALL
                             SELECT '健康食品', '50岁以上', 0.9);


