-- 1. 表结构创建
DROP TABLE IF EXISTS bodhidharma_disc.ads_tag_value_bitmap;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.ads_tag_value_bitmap
(
    `tag_code`  VARCHAR(45) NULL COMMENT "标签ID",
    `tag_value` VARCHAR(45) NULL COMMENT "标签值",
    -- bitmap_union 该标签值用户集合的并集
    `user_ids`  STRING COMMENT "所有用户ID"
) ENGINE = OLAP
    DUPLICATE KEY(`tag_code`,`tag_value`)
DISTRIBUTED BY HASH(`tag_code`) BUCKETS 2
    PROPERTIES (
      "replication_num" = "1"
);

-- 年龄标签
INSERT INTO `bodhidharma_disc`.ads_tag_value_bitmap(tag_code, tag_value, user_ids)
SELECT 'age'                                                AS tag_code
     , age                                                  AS tag_value
     , group_concat(DISTINCT CAST(user_id AS VARCHAR), ',') AS user_id_bitmap
FROM bodhidharma_disc.ads_user_profile_detail
GROUP BY age
;

-- 性别标签
INSERT INTO `bodhidharma_disc`.ads_tag_value_bitmap(tag_code, tag_value, user_ids)
SELECT 'gender'                                             AS tag_code
     , gender                                               AS tag_value
     , group_concat(DISTINCT CAST(user_id AS VARCHAR), ',') AS user_id_bitmap
FROM bodhidharma_disc.ads_user_profile_detail
GROUP BY gender
;

-- 身高标签
INSERT INTO `bodhidharma_disc`.ads_tag_value_bitmap(tag_code, tag_value, user_ids)
SELECT 'height'                                             AS tag_code
     , height                                               AS tag_value
     , group_concat(DISTINCT CAST(user_id AS VARCHAR), ',') AS user_id_bitmap
FROM bodhidharma_disc.ads_user_profile_detail
GROUP BY height
;

-- 体重标签
INSERT INTO `bodhidharma_disc`.ads_tag_value_bitmap(tag_code, tag_value, user_ids)
SELECT 'weight'                                             AS tag_code
     , weight                                               AS tag_value
     , group_concat(DISTINCT CAST(user_id AS VARCHAR), ',') AS user_id_bitmap
FROM bodhidharma_disc.ads_user_profile_detail
GROUP BY weight
;

-- 星座标签
INSERT INTO `bodhidharma_disc`.ads_tag_value_bitmap(tag_code, tag_value, user_ids)
SELECT 'constellation'                                      AS tag_code
     , constellation                                        AS tag_value
     , group_concat(DISTINCT CAST(user_id AS VARCHAR), ',') AS user_id_bitmap
FROM bodhidharma_disc.ads_user_profile_detail
GROUP BY constellation
;


-- todo 查询：条件组合查询
SELECT user_ids
FROM bodhidharma_disc.ads_tag_value_bitmap
WHERE tag_code = 'constellation'
  AND tag_value = '金牛座'
;


SELECT user_ids
FROM bodhidharma_disc.ads_tag_value_bitmap
WHERE tag_code = 'gender'
  AND tag_value = '女'
;

