

/*
    1. 计算每个用户购买每个商品次数、金额、数量
    2. 计算不同维度数据统计
        商品统计
        品牌统计
        品类统计
        每个品牌Top10商品
        每个品类Top10商品
*/
-- ==========================================================================
-- todo 1. 计算每天每个用户购买每个商品：次数、金额、数量
-- ==========================================================================
drop table if exists jtp_oms_dws.dws_order_day_member_item_report;
create table if not exists jtp_oms_dws.dws_order_day_member_item_report
as
select member_id, product_id, product_brand, product_category_id, date_format(create_time, 'yyyy-MM-dd') AS dt, -- 统计日期
       count(id) AS order_count, -- 购买次数
       sum(product_quantity*product_price) AS order_amount, -- 购买金额
       sum(product_quantity) AS product_quantity -- 购买数量
from jtp_oms_dwd.dwd_oms_order_item_incr
where dt <= '2024-12-31'
group by member_id, product_id, product_brand, product_category_id, date_format(create_time, 'yyyy-MM-dd')
;


-- ==========================================================================
-- todo  2. 计算不同维度数据统计
--         商品统计             -->   GROUP BY product_id
--         品牌统计             -->   GROUP BY product_brand
--         品类统计             -->   GROUP BY product_category_id
--         每个品牌Top10商品     -->  GROUP BY product_brand、product_id
--         每个品类Top10商品     -->  GROUP BY product_category_id、product_id
--      分组字段
--          product_id、product_brand、product_category_id
-- ==========================================================================

/*
    增强分组聚合函数：
        grouping sets  -> 指定分组字段   ->  group by x, y, z GROUPING SETS ( x, y, z, (x, y), (x, z))
        rolling up     -> 上卷分组字段   ->  group by x, y, z ROLLING UP
        cube           -> 立方分组字段   ->  group by x, y, z cube
*/
drop table if exists jtp_oms_dws.dws_order_cube_report;
create table if not exists jtp_oms_dws.dws_order_cube_report
as
-- s2.将增强聚合中纬度为null替换为空字符串
select
    report_dt,
    nvl(product_id, '') AS product_id,
    nvl(product_brand, '')  AS product_brand,
    nvl(product_category_id,'') AS product_category_id,
    -- 次数、金额、数量
    order_count,
    order_amount,
    product_quantity,
    -- 购买人数
    member_count
from(
        -- s1 使用grouping sets增强聚合
        select
            '2024-12-31' as report_dt,
            product_id,
            product_brand,
            product_category_id,
            -- 次数、金额、数量
            count(order_count) AS order_count,
            sum(order_amount) AS order_amount,
            sum(product_quantity) AS product_quantity
            -- 计算人数
                ,count(distinct member_id) as member_count
        from jtp_oms_dws.dws_order_day_member_item_report
        where dt = '2024-12-31'
        group by product_id, product_brand, product_category_id
            grouping sets (
                 product_id,
                 product_brand,
                 product_category_id,
            (product_id, product_brand),
            (product_id, product_category_id)
            )
    )t1
;






/*
    todo 多维分析结果查询，核心点在于：分组字段是否为空字符串（原始： 分组字段是否为NULL）
*/
-- 查询 Top10品牌
select
    product_brand
    , order_amount
    , order_count
    , product_quantity
    , member_count
from jtp_oms_dws.dws_order_cube_report
where report_dt = '2024-12-31'
    and product_brand != ''
    and product_id = ''
    and product_category_id = ''
order by order_count desc,
         order_amount desc,
         product_quantity  desc,
         member_count desc;


-- 查询 各品类Top10商品


