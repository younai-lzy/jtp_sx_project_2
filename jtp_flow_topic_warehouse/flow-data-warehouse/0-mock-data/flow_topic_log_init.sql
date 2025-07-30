# noinspection SqlCurrentSchemaInspectionForFile

-- 创建数据库
CREATE DATABASE IF NOT EXISTS jtp_flow_topic CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 切换到新创建的数据库
USE jtp_flow_topic;

-- 1. user_action_log: 用户行为日志表
-- 记录用户在电商平台上的各种行为，如页面浏览、点击、加入购物车、购买等。
CREATE TABLE IF NOT EXISTS user_action_log
(
    log_id      VARCHAR(64) PRIMARY KEY COMMENT '日志唯一ID',
    user_id     VARCHAR(64) COMMENT '用户ID',
    session_id  VARCHAR(64) COMMENT '会话ID',
    page_id     VARCHAR(64) COMMENT '页面ID',
    element_id  VARCHAR(64) COMMENT '点击元素ID (如果事件类型为点击)',
    event_type  VARCHAR(32) COMMENT '事件类型 (page_view, click, add_to_cart, purchase)',
    event_time  DATETIME COMMENT '事件发生时间',
    product_id  VARCHAR(64) COMMENT '如果事件与商品相关，记录商品ID',
    ip_address  VARCHAR(15) COMMENT '用户IP地址',
    device_type VARCHAR(16) COMMENT '设备类型 (PC, Mobile)',
    date_id     VARCHAR(8) COMMENT '分区日期，格式YYYYMMDD (用于逻辑分区或索引)'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT ='用户行为日志原始表';

-- 2. order_info: 订单信息表
-- 记录用户的订单详情，用于计算引导支付金额。
CREATE TABLE IF NOT EXISTS order_info
(
    order_id     VARCHAR(64) PRIMARY KEY COMMENT '订单ID',
    user_id      VARCHAR(64) COMMENT '用户ID',
    product_id   VARCHAR(64) COMMENT '商品ID',
    order_amount DOUBLE COMMENT '订单金额',
    order_time   DATETIME COMMENT '下单时间',
    pay_time     DATETIME COMMENT '支付时间',
    order_status VARCHAR(32) COMMENT '订单状态 (paid, unpaid, cancelled)',
    date_id      VARCHAR(8) COMMENT '分区日期，格式YYYYMMDD (用于逻辑分区或索引)'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT ='订单信息原始表';

-- 3. product_info: 商品信息表
-- 记录商品的基本信息，用于关联商品ID获取商品名称等。
CREATE TABLE IF NOT EXISTS product_info
(
    product_id   VARCHAR(64) PRIMARY KEY COMMENT '商品ID',
    product_name VARCHAR(255) COMMENT '商品名称',
    category     VARCHAR(64) COMMENT '商品类别'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT ='商品维度信息表';

-- 4. page_info: 页面信息表
-- 记录页面的基本信息，用于关联页面ID获取页面名称和类型。
CREATE TABLE IF NOT EXISTS page_info
(
    page_id   VARCHAR(64) PRIMARY KEY COMMENT '页面ID',
    page_name VARCHAR(255) COMMENT '页面名称',
    page_type VARCHAR(64) COMMENT '页面类型 (home, category, product_detail, activity, search_result)'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci COMMENT ='页面维度信息表';


