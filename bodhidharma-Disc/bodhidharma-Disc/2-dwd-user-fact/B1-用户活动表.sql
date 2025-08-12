DROP TABLE IF EXISTS bodhidharma_disc.dwd_user_activity_log;
CREATE TABLE bodhidharma_disc.dwd_user_activity_log
(
    dt            DATE COMMENT '日期',
    activity_id   BIGINT COMMENT '活动ID',
    activity_name VARCHAR(255) COMMENT '活动名称',
    coupon_id     BIGINT COMMENT '优惠券ID',
    activity_type VARCHAR(255) COMMENT '活动类型 (例如: 促销, 节日, 新品发布)',
    start_time    VARCHAR(255) COMMENT '活动开始时间',
    end_time      VARCHAR(255) COMMENT '活动结束时间',
    creation_time VARCHAR(255) COMMENT '活动创建时间'
)DUPLICATE KEY(dt,activity_id)
COMMENT '用户活动表'
PARTITION BY RANGE (dt) (
    PARTITION p202510 VALUES LESS THAN ('2025-08-10'),
    PARTITION p202511 VALUES LESS THAN ('2025-08-11')
    -- 可根据实际需求添加更多分区
)
DISTRIBUTED BY HASH(activity_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);


INSERT INTO bodhidharma_disc.dwd_user_activity_log
SELECT dt,
       activity_id,
       activity_name,
       coupon_id,
       activity_type,
       start_time,
       end_time,
       creation_time
FROM hive_catalogs.bodhidharma_disc.ods_activity_info
WHERE dt = '2025-08-10';