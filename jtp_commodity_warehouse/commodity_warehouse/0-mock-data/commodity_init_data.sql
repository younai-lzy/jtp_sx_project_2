-- ====================================================================================================
-- 数据库和表结构定义
-- ====================================================================================================

-- 创建数据库，如果数据库已存在则不执行
CREATE DATABASE IF NOT EXISTS `ecommerce_dw`;
-- 切换到新创建的数据库，确保后续操作在此数据库中进行
USE `ecommerce_dw`;

-- 1. 全量商品信息表 (ods_product_info_full)
-- 作用：存储从业务系统同步过来的全量商品基础信息，作为商品维度的原始数据源。
CREATE TABLE `ods_product_info_full`
(
    `id`             INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `sku_id`         INT UNSIGNED NOT NULL COMMENT 'SKU ID，商品的最小销售单元，全局唯一标识',
    `product_id`     INT UNSIGNED NOT NULL COMMENT '商品ID，用于关联同一款商品的不同SKU',
    `product_name`   VARCHAR(255)   NOT NULL COMMENT '商品名称',
    `category_id`    INT UNSIGNED NOT NULL COMMENT '商品所属类目ID',
    `category_name`  VARCHAR(100)   NOT NULL COMMENT '商品所属类目名称',
    `brand_id`       INT UNSIGNED NOT NULL COMMENT '商品所属品牌ID',
    `brand_name`     VARCHAR(100)   NOT NULL COMMENT '商品所属品牌名称',
    `original_price` DECIMAL(10, 2) NOT NULL COMMENT '商品原始价格',
    `create_time`    DATETIME       NOT NULL COMMENT '商品在业务系统中的创建时间',
    `ts`             TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据同步时间戳',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_sku_id` (`sku_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全量商品信息表';

-- 2. 全量用户信息表 (ods_user_full)
-- 作用：存储从业务系统同步过来的用户基础信息，是进行新老用户、年龄、性别、兴趣等用户画像分析的基础。
CREATE TABLE `ods_user_full`
(
    `user_id`           INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '用户ID，唯一标识一个用户',
    `username`          VARCHAR(100) NOT NULL COMMENT '用户名',
    `registration_time` DATETIME     NOT NULL COMMENT '用户的注册时间，用于计算新老用户',
    `gender`            VARCHAR(10) COMMENT '用户性别，用于性别占比分析',
    `birth_date`        DATE COMMENT '用户出生日期，用于计算年龄和年龄占比',
    `city`              VARCHAR(50) COMMENT '用户所在城市',
    `hobby`             VARCHAR(100) COMMENT '用户兴趣爱好，用于兴趣爱好占比分析',
    PRIMARY KEY (`user_id`),
    UNIQUE KEY `uk_username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全量用户信息表';

-- 3. 用户行为日志表 (ods_user_action_log)
-- 作用：存储用户在网站或App上的所有行为日志。
-- 重新设计此表，将内容浏览行为也整合进来，实现对用户行为的全面记录。
-- 这样既能进行流量分析和转化分析，也能进行内容互动分析。
CREATE TABLE `ods_user_action_log`
(
    `log_id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '日志ID，唯一标识一条行为记录',
    `session_id`     VARCHAR(64) NOT NULL COMMENT '会话ID，用于将同一用户的连续行为串联起来',
    `user_id`        INT UNSIGNED NOT NULL COMMENT '用户ID，用于关联用户信息',
    `sku_id`         INT UNSIGNED COMMENT '用户行为相关的SKU ID，当行为与商品相关时有值，否则为NULL',
    `content_id`     INT UNSIGNED COMMENT '用户行为相关的内容ID，当行为与内容相关时有值，否则为NULL',
    `content_type`   VARCHAR(50) COMMENT '内容类型：live（直播）, short_video（短视频）, graphic（图文）。当行为与内容相关时有值，否则为NULL',
    `action_type`    VARCHAR(50) NOT NULL COMMENT '行为类型：view, click, add_cart, search, buy (商品行为); content_view, content_like, content_share (内容行为)',
    `source_channel` VARCHAR(50) NOT NULL COMMENT '流量来源渠道：自然搜索、广告、推荐、社交媒体等',
    `log_timestamp`  BIGINT UNSIGNED NOT NULL COMMENT '行为发生时的时间戳',
    PRIMARY KEY (`log_id`),
    KEY `idx_user_sku` (`user_id`, `sku_id`),
    KEY `idx_user_content` (`user_id`, `content_id`),
    KEY `idx_action_type` (`action_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户行为日志表';

-- 4. 全量订单信息表 (ods_order_incr)
-- 作用：存储订单的增量信息，如总金额、状态等，用于订单维度的分析。
CREATE TABLE `ods_order_incr`
(
    `id`           INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `order_id`     INT UNSIGNED NOT NULL COMMENT '订单ID，唯一标识一个订单',
    `user_id`      INT UNSIGNED NOT NULL COMMENT '下单用户ID',
    `total_amount` DECIMAL(12, 2) NOT NULL COMMENT '订单总金额',
    `status`       VARCHAR(50)    NOT NULL COMMENT '订单状态：paid（已支付）, unpaid（未支付）, closed（已关闭）',
    `create_time`  DATETIME       NOT NULL COMMENT '订单创建时间',
    `pay_time`     DATETIME COMMENT '订单支付时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_order_id` (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全量订单信息表';

-- 5. 全量订单明细表 (ods_order_detail_full)
-- 作用：存储每个订单中购买的商品明细信息，与订单主表通过 order_id 关联。
CREATE TABLE `ods_order_detail_full`
(
    `id`         INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `order_id`   INT UNSIGNED NOT NULL COMMENT '订单ID，与 ods_order_incr 表关联',
    `sku_id`     INT UNSIGNED NOT NULL COMMENT '购买的SKU ID，与 ods_product_info_full 表关联',
    `buy_num`    INT UNSIGNED NOT NULL COMMENT '购买数量',
    `item_price` DECIMAL(10, 2) NOT NULL COMMENT '商品单价',
    PRIMARY KEY (`id`),
    KEY `idx_order_id` (`order_id`),
    KEY `idx_sku_id` (`sku_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全量订单明细表';

-- 6. 商品价格变动日志表 (ods_price_trend_log)
-- 作用：记录商品价格的历史变动，用于价格趋势和促销效果分析。
CREATE TABLE `ods_price_trend_log`
(
    `id`           INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `sku_id`       INT UNSIGNED NOT NULL COMMENT 'SKU ID，与 ods_product_info_full 表关联',
    `price_before` DECIMAL(10, 2) NOT NULL COMMENT '价格变动前',
    `price_after`  DECIMAL(10, 2) NOT NULL COMMENT '价格变动后',
    `change_time`  DATETIME       NOT NULL COMMENT '价格变动发生的时间',
    PRIMARY KEY (`id`),
    KEY `idx_sku_id` (`sku_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品价格变动日志表';

-- 7. 全量商品评价表 (ods_product_review_incr)
-- 作用：存储用户对商品的增量评价数据，用于商品口碑和好评率分析。
CREATE TABLE `ods_product_review_incr`
(
    `id`             INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `sku_id`         INT UNSIGNED NOT NULL COMMENT '评价的SKU ID，与 ods_product_info_full 表关联',
    `user_id`        INT UNSIGNED NOT NULL COMMENT '评价用户ID，与 ods_user_full 表关联',
    `order_id`       INT UNSIGNED NOT NULL COMMENT '评价关联的订单ID',
    `score`          INT UNSIGNED NOT NULL COMMENT '评分（1-5分）',
    `review_content` TEXT COMMENT '评价内容',
    `review_time`    DATETIME   NOT NULL COMMENT '评价时间',
    `is_positive`    TINYINT(1) NOT NULL COMMENT '是否为正面评价（1:是, 0:否）',
    PRIMARY KEY (`id`),
    KEY `idx_sku_id` (`sku_id`),
    KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全量商品评价表';