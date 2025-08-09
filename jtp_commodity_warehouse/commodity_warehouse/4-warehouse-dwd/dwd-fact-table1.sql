CREATE DATABASE IF NOT EXISTS jtp_commodity_warehouse
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse';
USE jtp_commodity_warehouse;

DROP TABLE IF EXISTS jtp_commodity_warehouse.dwd_product_detail;
CREATE TABLE IF NOT EXISTS jtp_commodity_warehouse.dwd_product_detail (
   sku_id         INT    NOT NULL COMMENT 'SKUID',
   product_id     INT    NOT NULL COMMENT '商品ID',
   product_name   STRING NOT NULL COMMENT '商品名称',
   category_id    INT    NOT NULL COMMENT '类目ID',
   category_name  STRING NOT NULL COMMENT '类目名称',
   brand_id       INT    NOT NULL COMMENT '品牌ID',
   brand_name     STRING NOT NULL COMMENT '品牌名称',
   original_price DECIMAL(10, 2) COMMENT '原始价格',
   create_time    TIMESTAMP COMMENT '商品创建时间',
   color_category STRING COMMENT '颜色',
   price_before   DECIMAL(10, 2) COMMENT '变动前价格',
   price_after    DECIMAL(10, 2) COMMENT '变动后价格',
   change_time    TIMESTAMP COMMENT '价格变动时间',
   dts            STRING COMMENT '分区字段，格式：YYYY-MM-DD'
)
    COMMENT '商品明细事实表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/dwd_product_detail'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- 2. 安全写入脚本（处理NULL值情况）
INSERT OVERWRITE TABLE jtp_commodity_warehouse.dwd_product_detail PARTITION (dt='2025-08-07')
SELECT
    -- 核心字段确保非空（必须处理）
    COALESCE(p.sku_id, -999) AS sku_id,
    COALESCE(p.product_id, -999) AS product_id,
    NVL(p.product_name, '未知商品') AS product_name,
    COALESCE(p.category_id, 0) AS category_id,
    NVL(p.category_name, '未分类') AS category_name,
    COALESCE(p.brand_id, 0) AS brand_id,
    NVL(p.brand_name, '未知品牌') AS brand_name,

    -- 允许为NULL的字段（保持原值）
    p.original_price,
    p.create_time,
    p.color_category,
    t.price_before,
    t.price_after,
    t.change_time,
    '2025-08-07' AS dts  -- 显式分区值
FROM
    jtp_commodity_warehouse.ods_product_info_full p
        LEFT JOIN
    jtp_commodity_warehouse.ods_price_trend_log t
    ON p.sku_id = t.sku_id AND p.dt = t.dt
WHERE
        p.dt = '2025-08-07'
  -- 可选：添加数据质量检查
  AND p.sku_id IS NOT NULL;  -- 确保关联主键不为空

select * FROM jtp_commodity_warehouse.dwd_product_detail WHERE dt = '2025-08-07' LIMIT 10;

DROP TABLE IF EXISTS jtp_commodity_warehouse.dwd_user_detail;
CREATE TABLE IF NOT EXISTS jtp_commodity_warehouse.dwd_user_detail (
    user_id INT NOT NULL COMMENT '用户ID',
    username STRING NOT NULL COMMENT '用户名',
    registration_time TIMESTAMP NOT NULL COMMENT '注册时间',
    gender STRING COMMENT '性别（清洗后：男/女/未知）',
    birth_date DATE COMMENT '出生日期',
    age INT COMMENT '年龄（由birth_date计算）',
    city STRING COMMENT '所在城市',
    hobby STRING COMMENT '兴趣爱好（标准化处理，如“运动,音乐”）',
    dts string COMMENT '分区字段，格式：YYYY-MM-DD'
)
    COMMENT '用户明细事实表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/dwd_user_detail'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');


INSERT OVERWRITE TABLE jtp_commodity_warehouse.dwd_user_detail PARTITION (dt='2025-08-07')
SELECT
    COALESCE(user_id, -999) AS user_id,                      -- 若 user_id 为 NULL，则写入 -999（或其他不会与正常业务冲突的默认值）
    COALESCE(username, '未知用户') AS username,              -- 若 username 为 NULL，则写入 '未知用户'
    COALESCE(registration_time, CURRENT_TIMESTAMP()) AS registration_time,  -- 若注册时间为 NULL，则使用当前时间
    -- 清洗性别字段（兼容多种输入格式）
    CASE
        WHEN gender IN ('男') THEN '男'
        WHEN gender IN ('女') THEN '女'
        ELSE '未知'
        END AS gender,
    birth_date,
    -- 正确计算年龄（使用Hive/Spark支持的方式）
    CASE
        WHEN birth_date IS NOT NULL
            THEN year(current_date()) - year(birth_date) -
                 CASE
                     WHEN month(current_date()) < month(birth_date) OR
                          (month(current_date()) = month(birth_date) AND day(current_date()) < day(birth_date))
                         THEN 1
                     ELSE 0
                     END
        ELSE NULL
        END AS age,
    -- 标准化城市信息（去除前后空格，合并中间空格）
    regexp_replace(trim(city), '\\s+', ' ') AS city,
    -- 标准化兴趣爱好（统一小写并映射常见同义词）
    CASE lower(trim(hobby))
        WHEN 'sports' THEN '运动'
        WHEN 'reading' THEN '阅读'
        WHEN 'music' THEN '音乐'
        WHEN 'food' THEN '美食'
        WHEN 'travel' THEN '旅行'
        WHEN '运动' THEN '运动'
        WHEN '體育' THEN '运动'
        WHEN '阅读' THEN '阅读'
        WHEN '讀書' THEN '阅读'
        WHEN '音乐' THEN '音乐'
        WHEN '音樂' THEN '音乐'
        WHEN '美食' THEN '美食'
        WHEN '食物' THEN '美食'
        WHEN '旅行' THEN '旅行'
        WHEN '旅游' THEN '旅行'
        ELSE lower(trim(hobby))
        END AS standardized_hobby,
    '2025-08-07' as dts
FROM
    jtp_commodity_warehouse.ods_user_full
WHERE
        dt = '2025-08-07';

select * from jtp_commodity_warehouse.dwd_user_detail where dt = '2025-08-07' limit 10;

DROP TABLE IF EXISTS jtp_commodity_warehouse.dwd_order_detail;
CREATE TABLE IF NOT EXISTS jtp_commodity_warehouse.dwd_order_detail (
    -- 订单主表信息
    order_id INT NOT NULL COMMENT '订单ID',
    user_id INT NOT NULL COMMENT '用户ID',
    total_amount DECIMAL(12, 2) NOT NULL COMMENT '订单总金额',
    status STRING NOT NULL COMMENT '订单状态（标准化：paid/unpaid/closed）',
    create_time TIMESTAMP NOT NULL COMMENT '订单创建时间',
    pay_time TIMESTAMP COMMENT '支付时间',
    -- 订单明细表信息
    sku_id INT NOT NULL COMMENT 'SKU ID',
    buy_num INT NOT NULL COMMENT '购买数量',
    item_price DECIMAL(10, 2) NOT NULL COMMENT '商品单价',
    item_total DECIMAL(12, 2) COMMENT '单品总金额（buy_num * item_price）',
    -- 分区及元数据
    dts STRING COMMENT '订单日期'
)
    COMMENT '订单及明细关联事实表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/dwd_order_detail'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE jtp_commodity_warehouse.dwd_order_detail PARTITION (dt='2025-08-07')
SELECT
    -- 订单主表信息
    COALESCE(o.order_id, -999) AS order_id,
    COALESCE(o.user_id, -999) AS user_id,
    COALESCE(o.total_amount, 0.00) AS total_amount,
    -- 标准化订单状态（统一小写）
    LOWER(COALESCE(o.status, 'unpaid')) AS status,
    COALESCE(o.create_time, CURRENT_TIMESTAMP()) AS create_time,
    COALESCE(o.pay_time, NULL) AS pay_time,
    -- 订单明细信息
    COALESCE(d.sku_id, -999) AS sku_id,
    COALESCE(d.buy_num, 0) AS buy_num,
    COALESCE(d.item_price, 0.00) AS item_price,
    -- 计算单品总金额（数量×单价）
    d.buy_num * d.item_price AS item_total,
    '2025-08-07' AS dt
FROM
    jtp_commodity_warehouse.ods_order_incr o
        INNER JOIN
    jtp_commodity_warehouse.ods_order_detail_full d
    ON o.order_id = d.order_id
        AND o.dt = d.dt  -- 按日期关联
WHERE
        o.dt = '2025-08-07';

select * from jtp_commodity_warehouse.dwd_order_detail where dt = '2025-08-07' limit 10;


DROP TABLE IF EXISTS jtp_commodity_warehouse.dwd_product_review_detail;
CREATE TABLE IF NOT EXISTS jtp_commodity_warehouse.dwd_product_review_detail (
    review_id INT COMMENT '评价ID',
    sku_id INT NOT NULL COMMENT 'SKU ID',
    user_id INT NOT NULL COMMENT '评价用户ID',
    order_id INT NOT NULL COMMENT '关联订单ID',
    score INT NOT NULL COMMENT '评分（1-5分）',
    review_content STRING COMMENT '评价内容（清洗特殊字符）',
    review_time TIMESTAMP NOT NULL COMMENT '评价时间',
    is_positive INT NOT NULL COMMENT '是否正面评价（1:是,0:否）',
    is_old_buyer INT COMMENT '是否老买家（1:是,0:否，关联用户历史订单判断）',
    has_image INT COMMENT '是否带图评价（1:是,0:否，基于评价内容判断）',
    dts STRING COMMENT '评价日期'
)
    COMMENT '商品评价明细事实表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/dwd_product_review_detail'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE jtp_commodity_warehouse.dwd_product_review_detail PARTITION (dt='2025-08-07')
SELECT
    r.id AS review_id,
    COALESCE(r.sku_id, -999) AS sku_id,
    COALESCE(r.user_id, -999) AS user_id,
    COALESCE(r.order_id, -999) AS order_id,
    COALESCE(r.score, 0) AS score,
    REGEXP_REPLACE(r.review_content, '[^a-zA-Z0-9\u4e00-\u9fa5,，.。 ]', '') AS review_content,
    COALESCE(r.review_time, CURRENT_TIMESTAMP()) AS review_time,
    COALESCE(r.is_positive, 0) AS is_positive,
    CASE
        WHEN (SELECT COUNT(DISTINCT order_id)
              FROM jtp_commodity_warehouse.ods_order_incr
              WHERE user_id = r.user_id AND dt < '2025-08-07') >= 2
            THEN 1 ELSE 0 END AS is_old_buyer,
    CASE
        WHEN r.review_content LIKE '%图%' OR r.review_content LIKE '%图片%'
            THEN 1 ELSE 0 END AS has_image,
    '2025-08-07' AS dt
FROM
    jtp_commodity_warehouse.ods_product_review_incr r
WHERE
        dt = '2025-08-07';


select * from jtp_commodity_warehouse.dwd_product_review_detail where dt = '2025-08-07' limit 10;


--- 有问题
DROP TABLE IF EXISTS jtp_commodity_warehouse.dwd_user_action_detail;
CREATE TABLE IF NOT EXISTS jtp_commodity_warehouse.dwd_user_action_detail (
    log_id BIGINT NOT NULL COMMENT '日志ID',
    session_id STRING NOT NULL COMMENT '会话ID',
    user_id INT NOT NULL COMMENT '用户ID',
    sku_id INT COMMENT '关联SKU ID（无则为NULL）',
    content_id INT COMMENT '关联内容ID（无则为NULL）',
    content_type STRING COMMENT '内容类型（标准化：live/short_video/graphic）',
    action_type STRING NOT NULL COMMENT '行为类型（如click/view/add_cart/buy）',
    source_channel STRING NOT NULL COMMENT '流量来源渠道（如app/wap/pc）',
    device_type STRING COMMENT '设备类型（无线端/有线端，基于source_channel推导）',
    log_time BIGINT COMMENT '行为时间（由log_timestamp转换）',
    stay_duration INT COMMENT '停留时长（秒，仅对view行为有效）',
    dts STRING COMMENT '行为日期'
)
    COMMENT '用户行为明细事实表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/dwd_user_action_detail'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');

REFRESH TABLE jtp_commodity_warehouse.dwd_user_action_detail;

DESCRIBE FORMATTED jtp_commodity_warehouse.dwd_user_action_detail;

ALTER TABLE jtp_commodity_warehouse.dwd_user_action_detail ADD IF NOT EXISTS PARTITION (dt='2025-08-07');
----------- 有问题

INSERT OVERWRITE TABLE jtp_commodity_warehouse.dwd_user_action_detail PARTITION (dt='2025-08-07')
SELECT
    COALESCE(log_id, -1) AS log_id,
    COALESCE(session_id, 'unknown_session') AS session_id,
    COALESCE(user_id, -1) AS user_id,
    sku_id,
    content_id,
    CASE
        WHEN content_type IN ('live', '直播') THEN 'live'
        WHEN content_type IN ('short_video', '短视频') THEN 'short_video'
        WHEN content_type IN ('graphic', '图文') THEN 'graphic'
        ELSE NULL
        END AS content_type,
    COALESCE(action_type, 'unknown') AS action_type,
    COALESCE(source_channel, 'unknown') AS source_channel,
    CASE
        WHEN source_channel IN ('app', 'wap', 'mobile') THEN '无线端'
        WHEN source_channel IN ('pc', 'desktop') THEN '有线端'
        ELSE '未知'
        END AS device_type,
    ods_user_action_log.log_timestamp as log_time,
    CASE WHEN action_type = 'view' THEN 30 ELSE NULL END AS stay_duration,
    '2025-08-07' AS dt
FROM jtp_commodity_warehouse.ods_user_action_log
WHERE dt = '2025-08-07'
  AND log_id IS NOT NULL
  AND session_id IS NOT NULL
  AND user_id IS NOT NULL
  AND action_type IS NOT NULL
  AND source_channel IS NOT NULL
;


SELECT * FROM jtp_commodity_warehouse.dwd_user_action_detail WHERE dt = '2025-08-07' LIMIT 10;


DROP TABLE IF EXISTS jtp_commodity_warehouse.dwd_aggregated_wide;
CREATE TABLE IF NOT EXISTS jtp_commodity_warehouse.dwd_aggregated_wide (
    -- 用户行为主表字段（核心）
    log_id BIGINT COMMENT '日志ID',
    session_id STRING COMMENT '会话ID',
    user_id INT COMMENT '用户ID（来自行为表）',
    sku_id INT COMMENT '关联SKU ID（来自行为表）',
    content_id INT COMMENT '关联内容ID（来自行为表）',
    content_type STRING COMMENT '内容类型（来自行为表）',
    action_type STRING COMMENT '行为类型（如click/view/add_cart/buy，来自行为表）',
    source_channel STRING COMMENT '流量来源渠道（如app/wap/pc，来自行为表）',
    device_type STRING COMMENT '设备类型（来自行为表）',
    log_time BIGINT COMMENT '行为时间（来自行为表）',
    stay_duration INT COMMENT '停留时长（秒，仅对view行为有效，来自行为表）',
    dts STRING COMMENT '行为日期（格式：YYYY-MM-DD，来自行为表）',

    -- 用户信息（来自 dwd_user_detail）
    user_id_u INT COMMENT '用户ID（来自用户明细表）',
    username STRING COMMENT '用户名（来自用户明细表）',
    registration_time TIMESTAMP COMMENT '注册时间（来自用户明细表）',
    gender STRING COMMENT '性别（男/女/未知，来自用户明细表）',
    birth_date DATE COMMENT '出生日期（来自用户明细表）',
    age INT COMMENT '年龄（计算字段，来自用户明细表）',
    city STRING COMMENT '所在城市（来自用户明细表）',
    hobby STRING COMMENT '兴趣爱好（标准化处理，来自用户明细表）',

    -- 商品信息（来自 dwd_product_detail）
    sku_id_p INT COMMENT 'SKU ID（来自商品明细表）',
    product_id INT COMMENT '商品ID（来自商品明细表）',
    product_name STRING COMMENT '商品名称（来自商品明细表）',
    category_id INT COMMENT '类目ID（来自商品明细表）',
    category_name STRING COMMENT '类目名称（来自商品明细表）',
    brand_id INT COMMENT '品牌ID（来自商品明细表）',
    brand_name STRING COMMENT '品牌名称（来自商品明细表）',
    original_price DECIMAL(10, 2) COMMENT '原始价格（来自商品明细表）',
    color_category  STRING COMMENT '颜色',

    -- 订单信息（来自 dwd_order_detail）
    order_id INT COMMENT '订单ID（来自订单明细表）',
    user_id_o INT COMMENT '用户ID（来自订单表）',
    total_amount DECIMAL(12, 2) COMMENT '订单总金额（来自订单表）',
    status STRING COMMENT '订单状态（如paid/unpaid/closed，来自订单表）',
    create_time TIMESTAMP COMMENT '订单创建时间（来自订单表）',
    pay_time TIMESTAMP COMMENT '支付时间（来自订单表）',
    sku_id_o INT COMMENT 'SKU ID（来自订单明细表）',
    buy_num INT COMMENT '购买数量（来自订单明细表）',
    item_price DECIMAL(10, 2) COMMENT '商品单价（来自订单明细表）',
    item_total DECIMAL(12, 2) COMMENT '单品总金额（buy_num * item_price，来自订单明细表）',

    -- 评价信息（来自 dwd_product_review_detail）
    review_id INT COMMENT '评价ID（来自评价表）',
    sku_id_r INT COMMENT 'SKU ID（来自评价表）',
    user_id_r INT COMMENT '评价用户ID（来自评价表）',
    order_id_r INT COMMENT '关联订单ID（来自评价表）',
    score INT COMMENT '评分（1-5分，来自评价表）',
    review_content STRING COMMENT '评价内容（清洗特殊字符，来自评价表）',
    review_time TIMESTAMP COMMENT '评价时间（来自评价表）',
    is_positive INT COMMENT '是否正面评价（1是，0否，来自评价表）',
    is_old_buyer INT COMMENT '是否老买家（1是，0否，来自评价表）',
    has_image INT COMMENT '是否带图评价（1是，0否，来自评价表）',
    -- 公共分区字段
    dtse STRING COMMENT '数据分区日期字段，格式：YYYY-MM-DD'
)
    COMMENT 'DWD层用户行为及关联信息大宽表，整合了行为、用户、商品、订单、评价数据'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_commodity_warehouse/dwd_aggregated_wide'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');


INSERT OVERWRITE TABLE jtp_commodity_warehouse.dwd_aggregated_wide PARTITION (dt='2025-08-07')
SELECT
    -- 用户行为主表字段（核心）
    a.log_id,
    a.session_id,
    a.user_id,
    a.sku_id,
    a.content_id,
    a.content_type,
    a.action_type,
    a.source_channel,
    a.device_type,
    a.log_time,
    a.stay_duration,
    a.dts,

    -- 用户信息（来自 dwd_user_detail）
    u.user_id AS user_id_u,
    u.username,
    u.registration_time,
    u.gender,
    u.birth_date,
    u.age,
    u.city,
    u.hobby,

    -- 商品信息（来自 dwd_product_detail）
    p.sku_id AS sku_id_p,
    p.product_id,
    p.product_name,
    p.category_id,
    p.category_name,
    p.brand_id,
    p.brand_name,
    p.original_price,
    p.color_category,

    -- 订单信息（来自 dwd_order_detail）
    o.order_id,
    o.user_id AS user_id_o,
    o.total_amount,
    o.status,
    o.create_time,
    o.pay_time,
    o.sku_id AS sku_id_o,
    o.buy_num,
    o.item_price,
    o.item_total,

    -- 评价信息（来自 dwd_product_review_detail）
    r.review_id,
    r.sku_id AS sku_id_r,
    r.user_id AS user_id_r,
    r.order_id AS order_id_r,
    r.score,
    r.review_content,
    r.review_time,
    r.is_positive,
    r.is_old_buyer,
    r.has_image,

    -- 公共分区字段
    a.dt as dtse

FROM
    jtp_commodity_warehouse.dwd_user_action_detail a
        LEFT JOIN jtp_commodity_warehouse.dwd_user_detail u
                  ON a.user_id = u.user_id AND a.dt = u.dt
        LEFT JOIN jtp_commodity_warehouse.dwd_product_detail p
                  ON a.sku_id = p.sku_id AND a.dt = p.dt
        LEFT JOIN jtp_commodity_warehouse.dwd_order_detail o
                  ON a.user_id = o.user_id AND a.dt = o.dt
        LEFT JOIN jtp_commodity_warehouse.dwd_product_review_detail r
                  ON a.user_id = r.user_id  AND a.dt = r.dt
WHERE
        a.dt = '2025-08-07';

select * from jtp_commodity_warehouse.dwd_aggregated_wide where dt='2025-08-07' ;








