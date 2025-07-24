create database if not exists jtp_tmp location '/user/spark/warehouse/jtp_tmp';
use jtp_tmp;

DROP TABLE IF EXISTS jtp_tmp.ods_t_order;
CREATE TABLE IF NOT EXISTS jtp_tmp.ods_t_order
(
    id           BIGINT COMMENT '订单主键',
    amt          DECIMAL(16, 2) COMMENT '订单金额',
    order_status INT COMMENT '订单状态，1：待付款，2：待发货，3：待收货，4：已完成，5：已取消',
    user_id      BIGINT COMMENT '用户ID',
    create_time  STRING COMMENT '数据插入时间',
    modify_time  STRING COMMENT '数据最后一次修改时间'
) COMMENT 'OMS系统-核心表-交易订单表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_tmp/ods_t_order'
;

SELECT
    *
FROM ods_t_order
;



SHOW PARTITIONS ods_t_order;

select *
from tmp_order_report;
