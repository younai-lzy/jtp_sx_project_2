create database if not exists jtp_oms_warehouse location
    'hdfs://node101:8020/warehouse/jtp_oms_warehouse';

use jtp_oms_warehouse;

DROP TABLE IF EXISTS jtp_oms_warehouse.ods_ums_member;
CREATE EXTERNAL TABLE IF NOT EXISTS jtp_oms_warehouse.ods_ums_member
(
    id                     BIGINT COMMENT '主键，用户唯一标识',
    member_level_id        BIGINT COMMENT '用户等级ID',
    username               STRING COMMENT '用户名',
    password               STRING COMMENT '密码',
    nickname               STRING COMMENT '昵称',
    phone                  STRING COMMENT '手机号码',
    status                 INT COMMENT '帐号启用状态:0->禁用；1->启用',
    create_time            STRING COMMENT '注册时间',
    icon                   STRING COMMENT '头像',
    gender                 INT COMMENT '性别：0->未知；1->男；2->女',
    birthday               STRING COMMENT '生日',
    city                   STRING COMMENT '所做城市',
    job                    STRING COMMENT '职业',
    personalized_signature STRING COMMENT '个性签名',
    source_type            INT COMMENT '用户来源',
    integration            INT COMMENT '积分',
    growth                 INT COMMENT '成长值',
    luckey_count           INT COMMENT '剩余抽奖次数',
    history_integration    INT COMMENT '历史积分数量',
    modify_time            STRING COMMENT '数据更新修改时间'
) COMMENT 'OMS系统对接会员信息表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION 'hdfs://node101:8020/warehouse/jtp_oms_warehouse/ods_ums_member'
;


-- 查询数据(全量)
SELECT
    *
FROM jtp_oms_warehouse.ods_ums_member
where dt = '2025-01-01'
;

show partitions jtp_oms_warehouse.ods_ums_member;

ALTER TABLE jtp_oms_warehouse.ods_ums_member ADD IF NOT EXISTS PARTITION (dt = '2024-12-31');

ALTER TABLE jtp_oms_warehouse.ods_ums_member ADD IF NOT EXISTS PARTITION (dt = '2025-01-01');
