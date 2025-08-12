DROP TABLE IF EXISTS bodhidharma_disc.dws_user_height_tag;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.dws_user_height_tag
(
    user_id BIGINT COMMENT '用户id',
    height  VARCHAR(255) REPLACE_IF_NOT_NULL COMMENT '身高'
) ENGINE = OLAP AGGREGATE KEY (user_id)
    DISTRIBUTED BY HASH (user_id) BUCKETS 5
    PROPERTIES (
    "replication_num" = "1"
               );


INSERT INTO bodhidharma_disc.dws_user_height_tag
SELECT user_id
     , user_height
FROM bodhidharma_disc.dwd_user_behavior_log
WHERE dt = '2025-08-10'
;