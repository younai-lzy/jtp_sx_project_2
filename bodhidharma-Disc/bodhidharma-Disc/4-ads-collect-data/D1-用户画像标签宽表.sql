-- 创建表
DROP TABLE IF EXISTS bodhidharma_disc.ads_user_profile_detail;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.ads_user_profile_detail
(
    user_id       BIGINT COMMENT '用户ID',
    age           VARCHAR(255) REPLACE_IF_NOT_NULL DEFAULT NULL COMMENT '具体年龄值标签',
    gender        VARCHAR(20) REPLACE_IF_NOT_NULL DEFAULT NULL COMMENT '性别标签',
    height        VARCHAR(255) REPLACE_IF_NOT_NULL DEFAULT NULL COMMENT '身高标签',
    weight        DOUBLE REPLACE_IF_NOT_NULL DEFAULT NULL COMMENT '体重标签',
    constellation VARCHAR(255) REPLACE_IF_NOT_NULL DEFAULT NULL COMMENT '星座标签'
) ENGINE = OLAP AGGREGATE KEY(user_id)
        COMMENT '电商用户画像宽表'
        DISTRIBUTED BY HASH(user_id) BUCKETS 4
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "false"
        );


-- 用户画像宽表
INSERT INTO bodhidharma_disc.ads_user_profile_detail
SELECT t1.user_id
     , age_range
     , gender
     , height
     , user_weight
     , constellation
FROM bodhidharma_disc.dws_user_age_tag t1
         LEFT JOIN bodhidharma_disc.dws_user_sex_tag t2 ON t1.user_id = t2.user_id
         LEFT JOIN bodhidharma_disc.dws_user_height_tag t3 ON t1.user_id = t3.user_id
         LEFT JOIN bodhidharma_disc.dws_user_weight_tag t4 ON t1.user_id = t4.user_id
         LEFT JOIN bodhidharma_disc.dws_user_constellation_tag t5 ON t1.user_id = t5.user_id
;

