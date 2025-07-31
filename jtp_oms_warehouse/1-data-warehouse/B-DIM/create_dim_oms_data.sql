CREATE DATABASE IF NOT EXISTS jtp_oms_warehouse location
    'hdfs://node101:8020/warehouse/jtp_oms_warehouse';

USE jtp_oms_warehouse;

/*
DIM层维度表数据来源于ODS层原始同步数据
    会员维度拉链表数据，有些字进行脱敏处理，
        比如手机号码phone、出生日期birthday、身份标识shengfenzheng_id
        1)、186********
        2）、1997-00-00
        3）、a4d2bc39d1a29ae36b0864b79b6ef330
*/
-- 创建表：用户维度拉链表
DROP TABLE IF EXISTS jtp_oms_warehouse.dim_ums_member_zip;
CREATE EXTERNAL TABLE IF NOT EXISTS jtp_oms_warehouse.dim_ums_member_zip
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
    modify_time            STRING COMMENT '数据更新修改时间',
    start_date             STRING COMMENT '用户数据生命周期开始日期',
    end_date               STRING COMMENT '用户数据生命周期结束日期'
) COMMENT '会员信息拉链表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY')
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_oms_warehouse/dim_ums_member_zip'
;

/*
    会员信息数据，采用拉链表方式进行存储
        1）、会员信息表：数据量比较多（百万级别），新增和变化很少（几十条，上百条） -- 缓慢变化维
        2）、使用拉链表存储，大大减少存储空间
        3）、拉链表思想：每条数据加上生命周期start_date和end_date，默认情况下end_date为9999-12-31，表示数据未过期
*/
-- =====================================================================================
-- todo 1首次数据（历史数据）同步加载     --  dt=2024-12-31
-- =====================================================================================
INSERT OVERWRITE TABLE jtp_oms_warehouse.dim_ums_member_zip PARTITION (dt = '9999-12-31')
SELECT id,
       member_level_id,
       username,
       password,
       md5(nickname)                            AS nickname,
       concat(substr(phone, 1, 3), '********')  AS phone,
       status,
       create_time,
       icon,
       gender,
       concat(substr(birthday, 1, 4), '-01-01') AS birthday,
       city,
       job,
       personalized_signature,
       source_type,
       integration,
       growth,
       luckey_count,
       history_integration,
       modify_time,
       date_format(create_time, 'yyyy-MM-dd')   AS start_date,
       '9999-12-31'                             AS end_date
FROM jtp_oms_warehouse.ods_ums_member
WHERE dt = '2024-12-31'
;


SELECT *
FROM jtp_oms_warehouse.dim_ums_member_zip
;

INSERT OVERWRITE TABLE jtp_oms_warehouse.dim_ums_member_zip PARTITION (dt)
SELECT id,
       member_level_id,
       username,
       password,
       nickname,
       phone,
       status,
       create_time,
       icon,
       gender,
       birthday,
       city,
       job,
       personalized_signature,
       source_type,
       integration,
       growth,
       luckey_count,
       history_integration,
       modify_time,
       start_date,
       if(rk = 1, end_date, date_sub('2025-01-01', 1)) AS end_date,
       if(rk = 1, end_date, date_sub('2025-01-01', 1)) AS dt
FROM (
         -- s4：每条数据加上序号
         SELECT *,
                -- 加序号
                row_number() over (PARTITION BY id ORDER BY start_date DESC ) AS rk
         FROM (
                  -- s1: 前1日：9999-12-31 数据
                  SELECT id,
                         member_level_id,
                         username,
                         password,
                         nickname,
                         phone,
                         status,
                         create_time,
                         icon,
                         gender,
                         birthday,
                         city,
                         job,
                         personalized_signature,
                         source_type,
                         integration,
                         growth,
                         luckey_count,
                         history_integration,
                         modify_time,
                         start_date,
                         end_date
                  FROM jtp_oms_warehouse.dim_ums_member_zip
                  WHERE dt = '9999-12-31'
                        -- s3：合并所有数据
                  UNION
                  -- s2：当日增量数据（新增和修改）
                  SELECT id,
                         member_level_id,
                         username,
                         password,
                         md5(nickname)                            AS nickname,
                         concat(substr(phone, 1, 3), '********')  AS phone,
                         status,
                         create_time,
                         icon,
                         gender,
                         concat(substr(birthday, 1, 4), '-01-01') AS birthday,
                         city,
                         job,
                         personalized_signature,
                         source_type,
                         integration,
                         growth,
                         luckey_count,
                         history_integration,
                         modify_time,
                         date_format(modify_time, 'yyyy-MM-dd')   AS start_date,
                         '9999-12-31'                             AS end_date
                  FROM jtp_oms_warehouse.ods_ums_member
                  WHERE dt = '2025-01-01') t1) t2
;
SELECT *
FROM jtp_oms_warehouse.dim_ums_member_zip
;