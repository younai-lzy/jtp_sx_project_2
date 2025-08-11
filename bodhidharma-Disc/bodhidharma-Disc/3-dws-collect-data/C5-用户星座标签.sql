DROP TABLE IF EXISTS bodhidharma_disc.dws_user_constellation_tag;
CREATE TABLE IF NOT EXISTS bodhidharma_disc.dws_user_constellation_tag
(
    user_id       BIGINT COMMENT '用户ID',
    constellation VARCHAR(255) COMMENT '星座'
)ENGINE = OLAP
    DUPLICATE KEY (user_id)
DISTRIBUTED BY HASH (user_id) BUCKETS 5
PROPERTIES (
    "replication_num" = "1"
);


-- 星座标签
INSERT INTO bodhidharma_disc.dws_user_constellation_tag
SELECT user_id
     , CASE
           WHEN substr(user_birth_of_date, 6, 5) BETWEEN '12-22' AND '01-19' THEN '摩羯座'
           WHEN substr(user_birth_of_date, 6, 5) BETWEEN '01-20' AND '02-18' THEN '水瓶座'
           WHEN substr(user_birth_of_date, 6, 5) BETWEEN '02-19' AND '03-20' THEN '双鱼座'
           WHEN substr(user_birth_of_date, 6, 5) BETWEEN '03-21' AND '04-19' THEN '白羊座'
           WHEN substr(user_birth_of_date, 6, 5) BETWEEN '04-20' AND '05-20' THEN '金牛座'
           WHEN substr(user_birth_of_date, 6, 5) BETWEEN '05-21' AND '06-21' THEN '双子座'
           WHEN substr(user_birth_of_date, 6, 5) BETWEEN '06-22' AND '07-22' THEN '巨蟹座'
           WHEN substr(user_birth_of_date, 6, 5) BETWEEN '07-23' AND '08-22' THEN '狮子座'
           WHEN substr(user_birth_of_date, 6, 5) BETWEEN '08-23' AND '09-22' THEN '处女座'
           WHEN substr(user_birth_of_date, 6, 5) BETWEEN '09-23' AND '10-23' THEN '天秤座'
           WHEN substr(user_birth_of_date, 6, 5) BETWEEN '10-24' AND '11-22' THEN '天蝎座'
           WHEN substr(user_birth_of_date, 6, 5) BETWEEN '11-23' AND '12-21' THEN '射手座'
           ELSE '未知'
    END AS constellation
FROM bodhidharma_disc.dwd_user_behavior_log
WHERE dt = '2025-08-10'
;