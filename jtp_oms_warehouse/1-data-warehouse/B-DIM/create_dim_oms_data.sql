create database if not exists jtp_oms_warehouse location
    'hdfs://node101:8020/warehouse/jtp_oms_warehouse';

use jtp_oms_warehouse;

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
    start_date             STRING COMMENT '用户生命周期开始日期',
    end_date               STRING COMMENT '用户声明周期结束日期'
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