DROP TABLE IF EXISTS bodhidharma_disc.dws_user_weight_tag;
CREATE TABLE bodhidharma_disc.dws_user_weight_tag
(
    user_id     BIGINT COMMENT '用户ID',
    user_weight DOUBLE REPLACE_IF_NOT_NULL COMMENT '用户体重'
)ENGINE = OLAP
    AGGREGATE KEY (user_id)
    DISTRIBUTED BY HASH (user_id) BUCKETS 5
    PROPERTIES (
    "replication_num" = "1"
               );


-- 用户体重标签
INSERT INTO bodhidharma_disc.dws_user_weight_tag
SELECT user_id
     , if(user_weight < 30 OR user_weight > 200, NULL, user_weight) AS user_weight
FROM bodhidharma_disc.dwd_user_behavior_log
WHERE dt = '2025-08-10'
;