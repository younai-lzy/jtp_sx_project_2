CREATE DATABASE IF NOT EXISTS jtp_vivo_warehouse location "hdfs://node101:8020/user/spark/warehouse/jtp_vivo_warehouse";
USE jtp_vivo_warehouse;

CREATE TABLE IF NOT EXISTS `jtp_vivo_warehouse`.`vivo_user_info`
(
    user_id          STRING COMMENT "用户ID",
    register_date    DATE COMMENT "注册日期",
    age              INT COMMENT "年龄",
    gender           STRING COMMENT "性别",
    vip_level        INT COMMENT "VIP等级",
    vip_expire_date  DATE COMMENT "VIP到期日",
    phone_brand      STRING COMMENT "手机品牌",
    phone_model      STRING COMMENT "手机型号",
    oS_Version       STRING COMMENT "系统版本",
    first_login_date DATE COMMENT "首次登录日期",
    last_login_date  DATE COMMENT "最后登录日期",
    province         STRING COMMENT "省份",
    city             STRING COMMENT "城市",
    district         STRING COMMENT "区县",
    is_active        BOOLEAN COMMENT "是否活跃用户",
    user_tag         STRING COMMENT "用户标签",
    update_time      TIMESTAMP COMMENT "更新时问"
) COMMENT 'vivo日志表'
    PARTITIONED BY(dt STRING COMMENT '日期分区')
    STORED AS ORC
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_vivo_warehouse/vivo_user_info';
;





