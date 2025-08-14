
/*
    1. 计算每个用户购买每个商品次数、金额、数量
    2. 计算每个商品复购率
    3. 可以推广到品牌复购率、品类复购率、商家复购率等
*/

-- ==========================================================================
-- todo 1. 计算每天每个用户购买每个商品次数、金额、数量
-- ==========================================================================
drop table if exists jtp_oms_dws.dws_order_member_item_incr;
create table if not exists jtp_oms_dws.dws_order_member_item_incr
as
select member_id, product_id, product_brand, product_category_id, date_format(create_time, 'yyyy-MM-dd') AS dt, -- 统计日期
       count(1) AS buy_times, -- 购买次数
       sum(product_quantity*product_price) AS buy_amount, -- 购买金额
       sum(product_quantity) AS buy_num -- 购买数量
from jtp_oms_dwd.dwd_oms_order_item_incr
where dt <= '2024-12-31'
group by member_id, product_id, product_brand, product_category_id, date_format(create_time, 'yyyy-MM-dd')
;


-- ==========================================================================
-- todo 2. 计算12月份每个商品复购率
--  需求理解：复购率，某种程度上可以理解为购买次数大于1的人数占比
--  考察知识点：IF判断函数，count计数函数
-- ==========================================================================

-- s2 按照商品分组，计算每个用户购买商品次数为2次及以上的人数
drop table if exists jtp_oms_dwd.ads_item_repurchase_rate_report;
create table if not exists jtp_oms_dwd.ads_item_repurchase_rate_report
as
select
    '2024-12' as report_month,
    1 as recent_months,
    product_id,
    -- 购买人数
    count(member_id) as user_count,
    -- 购买超过2次的人数
    count(if(buy_times >= 2, member_id, null)) as buy_twice_user_count,
    -- 复购率 = 购买大于1次的人数/购买人数
    round(
    count(if(buy_times >= 2, member_id, null)) / count(member_id), 4
    ) as buy_twice_rate
from
    (-- s1 获取12月份的每个用户购买每个商品的次数
        select
            member_id,
            product_id,
            sum(buy_times) as buy_times
        from jtp_oms_dwd.dws_order_member_item_incr
        where date_format(dt, 'yyyy-MM') = '2024-12'
        group by product_id, member_id
    )
group by product_id
;

-- ==========================================================================
-- todo 3. 计算10月\11月\12月份每个品牌复购率
-- ==========================================================================

-- s2 按照品牌分组，计算每个品牌购买次数为2次及以上的人数
select
    '2024-12' as report_month,
    3 as recent_months,
    product_brand,
    -- 购买人数
    count(member_id) as user_count,
    -- 购买超过2次的人数
    count(if(quarter_buy_times >= 2, member_id, null)) as buy_twice_user_count,
    -- 复购率 = 购买大于1次的人数/购买人数
    round(
    count(if(quarter_buy_times >= 2, member_id, null)) / count(member_id), 4
    ) as buy_twice_rate
from (
         -- s1 计算3个月，每个用户对每个品牌的购买次数
         select
             member_id,
             product_brand,
             sum(buy_times) as quarter_buy_times
         from jtp_oms_dws.dws_order_member_item_incr
         where date_format(dt, 'yyyy-MM') in ('2024-10', '2024-11', '2024-12')
         group by member_id, product_brand
     )
group by product_brand



