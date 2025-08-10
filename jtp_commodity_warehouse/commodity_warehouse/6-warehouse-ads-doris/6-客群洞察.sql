DROP CATALOG IF EXISTS hive_catalog;
CREATE CATALOG IF NOT EXISTS hive_catalog PROPERTIES (
  'type'='hms',
  'hive.metastore.type' = 'hms',
  'hive.version' = '3.1.2',
  'fs.defaultFS' = 'hdfs://node101:8020',
  'hive.metastore.uris' = 'thrift://node101:9083'
);

USE hive_catalog.jtp_gd03_warehouse;

SHOW TABLES ;
USE jtp_gd03_warehouse;

DROP TABLE IF EXISTS jtp_gd03_warehouse.customer_insight_metrics;
CREATE TABLE jtp_gd03_warehouse.customer_insight_metrics (
    `dt` DATE COMMENT '数据日期，用于按天分区，格式如2025-08-07',
    `access_crowd_type` VARCHAR(100) COMMENT '访问人群分类标识，比如可区分不同渠道、不同业务线等访问人群',
    `new_old_ratio` DECIMAL(5, 2) COMMENT '指标1：新老占比，如0.3表示新客占比30%',
    `predicted_age_distribution` VARCHAR(500) COMMENT '指标2：预测年龄分布，可存储如"18-25:20%,26-35:30%"等结构化字符串或对应 JSON 格式',
    `predicted_gender_ratio` DECIMAL(5, 2) COMMENT '指标3：预测性别占比，如0.6表示男性占比60%',
    `hobby_ratio` VARCHAR(500) COMMENT '指标4：兴趣爱好占比，可存储如"运动:15%,音乐:20%"等结构化字符串或对应 JSON 格式'

)    ENGINE = OLAP
    DUPLICATE KEY (dt,access_crowd_type)
COMMENT '客群分析'
DISTRIBUTED BY HASH(dt,access_crowd_type) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",  -- 生产环境建议设为 3 以保证高可用
    "storage_format" = "V2"   -- 推荐的高效存储格式
);

INSERT INTO jtp_gd03_warehouse.customer_insight_metrics
WITH
-- 步骤1：清洗宽表数据，统一用户ID关联、计算年龄等基础信息
cleaned_data AS (
    SELECT
        -- 行为日期（转换为DATE类型）
        CAST(dts AS DATE) AS dt,
        -- 人群分类：用流量来源+设备类型组合（可根据业务调整，比如按source_channel单独分）
        CONCAT(source_channel, '_', device_type) AS access_crowd_type,
        -- 用户ID（优先取行为表的user_id，关联用户明细表验证）
        COALESCE(user_id, user_id_u) AS final_user_id,
        -- 计算年龄（用当前日期 - 生日，需处理空值）
        CASE
            WHEN birth_date IS NOT NULL
                THEN TIMESTAMPDIFF(YEAR, birth_date, CURRENT_DATE)
            ELSE NULL
            END AS user_age,
        gender,
        hobby,
        -- 判断新老客：注册时间在行为日期当天视为新客
        CASE
            WHEN DATE(registration_time) = CAST(dts AS DATE)
                THEN 1  -- 新客
            ELSE 0  -- 老客
            END AS is_new_visitor
    FROM hive_catalog.jtp_commodity_warehouse.dwd_aggregated_wide
         -- 过滤无效数据（可根据业务补充）
    WHERE user_id IS NOT NULL
      AND dts ='2025-08-07'  -- 校验日期格式
),

-- 步骤2：按人群+日期分组，统计基础指标
grouped_metrics AS (
    SELECT
        dt,
        access_crowd_type,
        -- 统计总人数、新客数
        COUNT(DISTINCT final_user_id) AS total_users,
        SUM(is_new_visitor) AS new_user_count,

        -- 年龄分布统计（分桶）
        SUM(CASE WHEN user_age BETWEEN 18 AND 25 THEN 1 ELSE 0 END) AS age_18_25,
        SUM(CASE WHEN user_age BETWEEN 26 AND 35 THEN 1 ELSE 0 END) AS age_26_35,
        SUM(CASE WHEN user_age BETWEEN 36 AND 45 THEN 1 ELSE 0 END) AS age_36_45,
        SUM(CASE WHEN user_age >= 46 THEN 1 ELSE 0 END) AS age_46_plus,

        -- 性别统计
        SUM(CASE WHEN gender = '男' THEN 1 ELSE 0 END) AS male_count,
        SUM(CASE WHEN gender = '女' THEN 1 ELSE 0 END) AS female_count,

        -- 兴趣爱好统计（拆分hobby字段，假设存的是逗号分隔，如"运动,音乐"）
        SUM(CASE WHEN hobby LIKE '%运动%' THEN 1 ELSE 0 END) AS hobby_sport,
        SUM(CASE WHEN hobby LIKE '%音乐%' THEN 1 ELSE 0 END) AS hobby_music,
        SUM(CASE WHEN hobby LIKE '%旅游%' THEN 1 ELSE 0 END) AS hobby_travel,
        SUM(CASE WHEN hobby LIKE '%美食%' THEN 1 ELSE 0 END) AS hobby_food
    FROM cleaned_data
    GROUP BY
        dt,
        access_crowd_type
)

-- 步骤3：计算最终指标并拼接结果
SELECT
    dt,
    access_crowd_type,

    -- 指标1：新老占比 = 新客数 / 总人数（处理分母为0）
    CASE
        WHEN total_users = 0 THEN 0.00
        ELSE ROUND(new_user_count / total_users, 2)
        END AS new_old_ratio,

    -- 指标2：年龄分布拼接（格式："18-25:20%,26-35:30%"）
    CONCAT(
            '18-25:', ROUND(CASE WHEN total_users=0 THEN 0 ELSE age_18_25*100/total_users END, 2), '%, ',
            '26-35:', ROUND(CASE WHEN total_users=0 THEN 0 ELSE age_26_35*100/total_users END, 2), '%, ',
            '36-45:', ROUND(CASE WHEN total_users=0 THEN 0 ELSE age_36_45*100/total_users END, 2), '%, ',
            '46+:',  ROUND(CASE WHEN total_users=0 THEN 0 ELSE age_46_plus*100/total_users END, 2), '%'
        ) AS predicted_age_distribution,

    -- 指标3：性别占比（男性占比，可同理算女性）
    CASE
        WHEN total_users = 0 THEN 0.00
        ELSE ROUND(male_count / total_users, 2)
        END AS predicted_gender_ratio,

    -- 指标4：兴趣爱好占比拼接（格式："运动:15%,音乐:20%"）
    CONCAT(
            '运动:', ROUND(CASE WHEN total_users=0 THEN 0 ELSE hobby_sport*100/total_users END, 2), '%, ',
            '音乐:', ROUND(CASE WHEN total_users=0 THEN 0 ELSE hobby_music*100/total_users END, 2), '%, ',
            '旅游:', ROUND(CASE WHEN total_users=0 THEN 0 ELSE hobby_travel*100/total_users END, 2), '%, ',
            '美食:', ROUND(CASE WHEN total_users=0 THEN 0 ELSE hobby_food*100/total_users END, 2), '%'
        ) AS hobby_ratio
FROM grouped_metrics;

SELECT  * FROM jtp_gd03_warehouse.customer_insight_metrics LIMIT 200;

