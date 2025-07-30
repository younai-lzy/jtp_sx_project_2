-- ODS层表结构

-- 1. ods_user_action_log: 用户行为日志表
-- 记录用户在电商平台上的各种行为，如页面浏览、点击、加入购物车、购买等。
CREATE EXTERNAL TABLE IF NOT EXISTS ods_user_action_log
(
    log_id      STRING COMMENT '日志唯一ID',
    user_id     STRING COMMENT '用户ID',
    session_id  STRING COMMENT '会话ID',
    page_id     STRING COMMENT '页面ID',
    element_id  STRING COMMENT '点击元素ID (如果事件类型为点击)',
    event_type  STRING COMMENT '事件类型 (page_view, click, add_to_cart, purchase)',
    event_time  TIMESTAMP COMMENT '事件发生时间',
    product_id  STRING COMMENT '如果事件与商品相关，记录商品ID',
    ip_address  STRING COMMENT '用户IP地址',
    device_type STRING COMMENT '设备类型 (PC, Mobile)'
)
    COMMENT '用户行为日志原始表'
    PARTITIONED BY (date_id STRING COMMENT '分区日期，格式YYYYMMDD')
    STORED AS PARQUET
    LOCATION '/user/hive/warehouse/ods.db/ods_user_action_log';

-- 2. ods_order_info: 订单信息表
-- 记录用户的订单详情，用于计算引导支付金额。
CREATE EXTERNAL TABLE IF NOT EXISTS ods_order_info
(
    order_id     STRING COMMENT '订单ID',
    user_id      STRING COMMENT '用户ID',
    product_id   STRING COMMENT '商品ID',
    order_amount DOUBLE COMMENT '订单金额',
    order_time   TIMESTAMP COMMENT '下单时间',
    pay_time     TIMESTAMP COMMENT '支付时间',
    order_status STRING COMMENT '订单状态 (paid, unpaid, cancelled)'
)
    COMMENT '订单信息原始表'
    PARTITIONED BY (date_id STRING COMMENT '分区日期，格式YYYYMMDD')
    STORED AS PARQUET
    LOCATION '/user/hive/warehouse/ods.db/ods_order_info';

-- 3. ods_product_info: 商品信息表
-- 记录商品的基本信息，用于关联商品ID获取商品名称等。
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_info
(
    product_id   STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category     STRING COMMENT '商品类别'
)
    COMMENT '商品维度信息表'
    STORED AS PARQUET
    LOCATION '/user/hive/warehouse/ods.db/ods_product_info';

-- 4. ods_page_info: 页面信息表
-- 记录页面的基本信息，用于关联页面ID获取页面名称和类型。
CREATE EXTERNAL TABLE IF NOT EXISTS ods_page_info
(
    page_id   STRING COMMENT '页面ID',
    page_name STRING COMMENT '页面名称',
    page_type STRING COMMENT '页面类型 (home, category, product_detail, activity, search_result)'
)
    COMMENT '页面维度信息表'
    STORED AS PARQUET
    LOCATION '/user/hive/warehouse/ods.db/ods_page_info';


-- ADS层表结构

-- ads_page_traffic_analysis_daily: 每日页面流量分析汇总表
-- 聚合每日页面流量数据，提供分析看板所需的核心指标。
CREATE EXTERNAL TABLE IF NOT EXISTS ads_page_traffic_analysis_daily
(
    page_id               STRING COMMENT '页面ID',
    page_name             STRING COMMENT '页面名称',
    page_type             STRING COMMENT '页面类型',
    total_clicks          BIGINT COMMENT '总点击量',
    total_page_views      BIGINT COMMENT '总访问量',
    unique_visitors       BIGINT COMMENT '独立访客数',
    unique_clickers       BIGINT COMMENT '独立点击人数',
    guided_payment_amount DOUBLE COMMENT '引导支付金额',
    conversion_rate       DOUBLE COMMENT '页面点击转化率 (独立点击人数 / 独立访客数)',
    bounce_rate           DOUBLE COMMENT '跳失率 (单页面访问会话数 / 总会话数)'
)
    COMMENT '每日页面流量分析汇总表'
    PARTITIONED BY (date_id STRING COMMENT '分区日期，格式YYYYMMDD')
    STORED AS PARQUET
    LOCATION '/user/hive/warehouse/ads.db/ads_page_traffic_analysis_daily';
