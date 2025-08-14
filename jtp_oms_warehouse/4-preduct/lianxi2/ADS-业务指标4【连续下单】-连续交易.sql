
/*
    1. 最近一个月下单超过5次的用户
    2. 最近7日连续下单至少3次的用户
    3. 连续半年每个月都下单用户
*/
-- ==========================================================================
-- todo 1. 最近一个月下单超过5次的用户
-- ==========================================================================
select
    member_id
    , count(id) as order_count
from jtp_oms_dwd.dwd_oms_order_info_incr
where dt >= add_months(dt, -1) and dt <= '2024-12-31'
group by member_id
having count(id) > 5
;


-- ==========================================================================
-- todo 2. 最近7日连续下单至少3次的用户
-- ==========================================================================
-- 根据连续的日期，和用户进行分组
select
    member_id
from
(
    -- 2.排序
    select
        member_id,
        row_number() over (partition by member_id order by dt) as rn,
        -- 使用date_sub()计算是不是连续
        DATE_SUB(dt, ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY dt)) AS date_diff
    from (
-- 1. 去重
             select member_id,
                    dt
             from jtp_oms_dwd.dwd_oms_order_info_incr
             where dt >= date_sub('2024-12-31', 6)
               and dt <= '2024-12-31'
             group by member_id, dt
         )
)
group by member_id, date_diff
having count(member_id) >= 3
;


-- ==========================================================================
-- todo 3. 连续半年每个月都下单用户
-- ==========================================================================
SELECT member_id
FROM
(
    -- 3.对连续月份进行分组，并计数
    SELECT
        member_id, date_diff,
        count(member_id) AS month_count
    FROM
        (
            -- 2.排序,通过日期和排序号的差值识别连续月份
            SELECT member_id,
                   -- 日期减去在用户组内的排序号
                   DATE_SUB(month_dt, row_number() over (partition by member_id order by month_dt)) AS date_diff
            FROM
                (
                    -- 1.半年下单的用户
                    SELECT
                        member_id,
                        date_format(dt, 'yyyy-MM-dd') AS month_dt
                    FROM jtp_oms_dwd.dwd_oms_order_info_incr
                    WHERE dt >= add_months('2024-12', -5)
                      AND dt <= '2024-12'
                    GROUP BY member_id, date_format(dt, 'yyyy-MM-dd')
                )T1
        )T2
    GROUP BY member_id, date_diff
    HAVING month_count = 6
) AS T3
;













