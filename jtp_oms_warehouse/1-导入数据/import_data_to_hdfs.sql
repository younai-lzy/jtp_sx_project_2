create database if not exists jtp_oms_warehouse location
    'hdfs://node101:8020/warehouse/jtp_oms_warehouse';

use jtp_oms_warehouse;

-- 会员表
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
-- where dt = '2024-12-31'
-- where dt = '2025-01-01'
;


show partitions jtp_oms_warehouse.ods_ums_member;

ALTER TABLE jtp_oms_warehouse.ods_ums_member ADD IF NOT EXISTS PARTITION (dt = '2024-12-31');

ALTER TABLE jtp_oms_warehouse.ods_ums_member ADD IF NOT EXISTS PARTITION (dt = '2025-01-01');

-- 优惠表
DROP TABLE IF EXISTS jtp_oms_warehouse.ods_oms_coupon_full;
CREATE EXTERNAL TABLE IF NOT EXISTS jtp_oms_warehouse.ods_oms_coupon_full
(
    id               STRING COMMENT '主键ID',
    type             STRING COMMENT '优惠卷类型；0->全场赠券；1->会员赠券；2->购物赠券；3->注册赠券',
    name             STRING COMMENT '优惠卷名称',
    platform         STRING COMMENT '使用平台：0->全部；1->移动；2->PC',
    count            STRING COMMENT '数量',
    amount           STRING COMMENT '金额',
    per_limit        STRING COMMENT '每人限领张数',
    min_point        STRING COMMENT '使用门槛；0表示无门槛',
    start_time       STRING COMMENT '优惠卷开始时间',
    end_time         STRING COMMENT '优惠卷结束时间',
    use_type         STRING COMMENT '使用类型：0->全场通用；1->指定分类；2->指定商品',
    note             STRING COMMENT '备注',
    publish_count    STRING COMMENT '发行数量',
    use_count        STRING COMMENT '已使用数量',
    receive_count    STRING COMMENT '领取数量',
    enable_time      STRING COMMENT '可以领取的日期',
    code             STRING COMMENT '优惠码',
    member_level     STRING COMMENT '可领取的会员类型：0->无限时'
) COMMENT 'OMS系统优惠卷信息表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION 'hdfs://node101:8020/warehouse/jtp_oms_warehouse/ods_oms_coupon_full';


-- 查询数据
SELECT
    *
FROM jtp_oms_warehouse.ods_oms_coupon_full
-- WHERE dt = '2024-12-31'
WHERE dt = '2025-01-01'
;

ALTER TABLE jtp_oms_warehouse.ods_oms_coupon_full ADD IF NOT EXISTS PARTITION (dt = '2024-12-31');

ALTER TABLE jtp_oms_warehouse.ods_oms_coupon_full ADD IF NOT EXISTS PARTITION (dt = '2025-01-01');


-- 订单表
DROP TABLE IF EXISTS jtp_oms_warehouse.ods_oms_order_incr;
CREATE EXTERNAL TABLE IF NOT EXISTS jtp_oms_warehouse.ods_oms_order_incr
(
    `id`                      BIGINT COMMENT '订单id',
    `member_id`               BIGINT COMMENT '会员ID',
    `coupon_id`               BIGINT COMMENT '优惠卷ID',
    `order_sn`                STRING COMMENT '订单编号',
    `create_time`             STRING COMMENT '提交时间',
    `member_username`         STRING COMMENT '用户帐号',
    `total_amount`            DECIMAL(16, 2) COMMENT '订单总金额',
    `pay_amount`              DECIMAL(16, 2) COMMENT '应付金额（实际支付金额）',
    `freight_amount`          DECIMAL(16, 2) COMMENT '运费金额',
    `promotion_amount`        DECIMAL(16, 2) COMMENT '促销优化金额（促销价、满减、阶梯价）',
    `INTegration_amount`      DECIMAL(16, 2) COMMENT '积分抵扣金额',
    `coupon_amount`           DECIMAL(16, 2) COMMENT '优惠券抵扣金额',
    `discount_amount`         DECIMAL(16, 2) COMMENT '管理员后台调整订单使用的折扣金额',
    `pay_type`                INT COMMENT '支付方式：0->未支付；1->支付宝；2->微信',
    `source_type`             INT COMMENT '订单来源：0->PC订单；1->app订单',
    `status`                  INT COMMENT '订单状态：0->待付款；1->待发货；2->已发货；3->已完成；4->已关闭；5->无效订单',
    `order_type`              INT COMMENT '订单类型：0->正常订单；1->秒杀订单',
    `delivery_company`        STRING COMMENT '物流公司',
    `delivery_sn`             STRING COMMENT '物流单号',
    `auto_confirm_day`        INT COMMENT '自动确认时间（天）',
    `INTegration`             INT COMMENT '可以获得的积分',
    `growth`                  INT COMMENT '可以活动的成长值',
    `promotion_info`          STRING COMMENT '活动信息',
    `bill_type`               INT COMMENT '发票类型：0->不开发票；1->电子发票；2->纸质发票',
    `bill_header`             STRING COMMENT '发票抬头',
    `bill_content`            STRING COMMENT '发票内容',
    `bill_receiver_phone`     STRING COMMENT '收票人电话',
    `bill_receiver_email`     STRING COMMENT '收票人邮箱',
    `receiver_name`           STRING COMMENT '收货人姓名',
    `receiver_phone`          STRING COMMENT '收货人电话',
    `receiver_post_code`      STRING COMMENT '收货人邮编',
    `receiver_province`       STRING COMMENT '省份/直辖市',
    `receiver_city`           STRING COMMENT '城市',
    `receiver_region`         STRING COMMENT '区',
    `receiver_detail_address` STRING COMMENT '详细地址',
    `note`                    STRING COMMENT '订单备注',
    `confirm_status`          INT COMMENT '确认收货状态：0->未确认；1->已确认',
    `delete_status`           INT COMMENT '删除状态：0->未删除；1->已删除',
    `use_INTegration`         INT COMMENT '下单时使用的积分',
    `payment_time`            STRING COMMENT '支付时间',
    `delivery_time`           STRING COMMENT '发货时间',
    `receive_time`            STRING COMMENT '确认收货时间',
    `comment_time`            STRING COMMENT '评价时间',
    `modify_time`             STRING COMMENT '修改时间'
) COMMENT 'OMS系统订单表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION 'hdfs://node101:8020/warehouse/jtp_oms_warehouse/ods_oms_order_incr';


-- 查询数据
SELECT
    *
FROM jtp_oms_warehouse.ods_oms_order_incr
-- WHERE dt = '2024-12-31'
-- WHERE dt = '2025-01-01'
;
ALTER TABLE jtp_oms_warehouse.ods_oms_order_incr ADD IF NOT EXISTS PARTITION (dt = '2024-12-31');

ALTER TABLE jtp_oms_warehouse.ods_oms_order_incr ADD IF NOT EXISTS PARTITION (dt = '2025-01-01');

-- 订单商品明细表
DROP TABLE IF EXISTS jtp_oms_warehouse.ods_oms_order_item_incr;
CREATE EXTERNAL TABLE IF NOT EXISTS jtp_oms_warehouse.ods_oms_order_item_incr
(
    id                  BIGINT,
    order_id            BIGINT COMMENT '订单id',
    order_sn            STRING COMMENT '订单编号',
    product_id          BIGINT COMMENT '商品id',
    product_pic         STRING COMMENT '商品图片',
    product_name        STRING COMMENT '商品名称',
    product_brand       STRING COMMENT '商品品牌',
    product_sn          STRING COMMENT '商品SN码',
    product_price       DECIMAL(16, 2) COMMENT '销售价格',
    product_quantity    INT COMMENT '购买数量',
    product_sku_id      BIGINT COMMENT '商品sku编号',
    product_sku_code    STRING COMMENT '商品sku条码',
    product_category_id BIGINT COMMENT '商品分类id',
    sp1                 STRING COMMENT '商品的销售属性',
    sp2                 STRING COMMENT '商品的销售属性2',
    sp3                 STRING COMMENT '商品的销售属性3',
    promotion_name      STRING COMMENT '商品促销名称',
    promotion_amount    DECIMAL(16, 2) COMMENT '商品促销分解金额',
    coupon_amount       DECIMAL(16, 2) COMMENT '优惠券优惠分解金额',
    INTegration_amount  DECIMAL(16, 2) COMMENT '积分优惠分解金额',
    real_amount         DECIMAL(16, 2) COMMENT '该商品经过优惠后的分解金额',
    gift_integration    INT,
    gift_growth         INT,
    product_attr        STRING COMMENT '商品销售属性:[{"key":"颜色","value":"颜色"},{"key":"容量","value":"4G"}]',
    `create_time`       STRING COMMENT '插入时间',
    `modify_time`       STRING COMMENT '修改时间'
) COMMENT 'OMS系统订单商品明细表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION 'hdfs://node101:8020/warehouse/jtp_oms_warehouse/ods_oms_order_item_incr'
;


-- 查询数据
SELECT
    *
FROM jtp_oms_warehouse.ods_oms_order_item_incr
-- WHERE dt = '2024-12-31'
WHERE dt = '2025-01-01'
;

-- 添加分区
ALTER TABLE jtp_oms_warehouse.ods_oms_order_item_incr ADD IF NOT EXISTS PARTITION (dt = '2024-12-31');

ALTER TABLE jtp_oms_warehouse.ods_oms_order_item_incr ADD IF NOT EXISTS PARTITION (dt = '2025-01-01');

-- 优惠使用表
DROP TABLE IF EXISTS jtp_oms_warehouse.ods_oms_coupon_use_incr;
CREATE EXTERNAL TABLE IF NOT EXISTS jtp_oms_warehouse.ods_oms_coupon_use_incr
(
    id               STRING COMMENT '主键ID',
    coupon_id        STRING COMMENT '优惠卷ID',
    member_id        STRING COMMENT '会员ID',
    coupon_code      STRING COMMENT '优惠卷编码',
    member_nickname  STRING COMMENT '领取人昵称',
    get_type         STRING COMMENT '获取类型：0->后台赠送；1->主动获取',
    create_time      STRING COMMENT '订单日期时间',
    use_status       STRING COMMENT '使用状态：0->未使用；1->已使用；2->已过期',
    use_time         STRING COMMENT '使用时间',
    order_id         STRING COMMENT '订单编号',
    order_sn         STRING COMMENT '订单号码'
) COMMENT 'OMS系统优惠券使用、领取历史表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION 'hdfs://node101:8020/warehouse/jtp_oms_warehouse/ods_oms_coupon_use_incr'
;


-- 查询数据
SELECT
    *
FROM jtp_oms_warehouse.ods_oms_coupon_use_incr
-- WHERE dt = '2024-12-31'
WHERE dt = '2025-01-01'
;

-- 添加分区
ALTER TABLE jtp_oms_warehouse.ods_oms_coupon_use_incr ADD IF NOT EXISTS PARTITION (dt = '2024-12-31');

ALTER TABLE jtp_oms_warehouse.ods_oms_coupon_use_incr ADD IF NOT EXISTS PARTITION (dt = '2025-01-01');