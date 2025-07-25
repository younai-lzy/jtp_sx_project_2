CREATE DATABASE IF NOT EXISTS gongdan location "hdfs://node101:8020/user/spark/warehouse/gongdan";
USE gongdan;

DROP TABLE IF EXISTS gongdan.gongdan_user;

-- 用户信息表
CREATE TABLE IF NOT EXISTS gongdan.gongdan_user
(
    user_id          STRING COMMENT "用户ID",
    username         STRING COMMENT "用户名 ",
    nickname         STRING COMMENT " 昵称 ",
    gender           STRING COMMENT "性别(M/F/U)",
    age              INT COMMENT " 年龄 ",
    birthday         STRING COMMENT " 生日 ",
    register_date    STRING COMMENT " 注册日期 ",
    register_channel STRING COMMENT " 注册渠道 ",
    vip_level        INT COMMENT " VIP 等级(O-普通用户) ",
    vip_expire_date  STRING COMMENT " VIP 到期日期 ",
    phone            STRING COMMENT "手机号 ",
    email            STRING COMMENT "邮箱 ",
    province         STRING COMMENT "省份 ",
    city             STRING COMMENT "城市 ",
    tags_string      STRING COMMENT "用户标签",
    is_musician      BOOLEAN COMMENT "是否音乐人 ",
    musician_level   INT COMMENT " 音乐人等级 ",
    update_time      TIMESTAMP COMMENT " 更新时间"
) COMMENT "用户信息表"
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/gongdan/gongdan_user';
;

-- 添加分区
ALTER TABLE gongdan.gongdan_user ADD IF NOT EXISTS PARTITION (dt = '2020-01-15');
ALTER TABLE gongdan.gongdan_user ADD IF NOT EXISTS PARTITION (dt = '2020-08-08');
ALTER TABLE gongdan.gongdan_user ADD IF NOT EXISTS PARTITION (dt = '2021-03-01');
ALTER TABLE gongdan.gongdan_user ADD IF NOT EXISTS PARTITION (dt = '2021-11-11');
ALTER TABLE gongdan.gongdan_user ADD IF NOT EXISTS PARTITION (dt = '2022-05-20');
ALTER TABLE gongdan.gongdan_user ADD IF NOT EXISTS PARTITION (dt = '2022-09-05');
ALTER TABLE gongdan.gongdan_user ADD IF NOT EXISTS PARTITION (dt = '2023-01-01');
ALTER TABLE gongdan.gongdan_user ADD IF NOT EXISTS PARTITION (dt = '2023-06-18');
ALTER TABLE gongdan.gongdan_user ADD IF NOT EXISTS PARTITION (dt = '2018-04-10');
ALTER TABLE gongdan.gongdan_user ADD IF NOT EXISTS PARTITION (dt = '2019-07-20');

SHOW PARTITIONS gongdan.gongdan_user;

SELECT *
FROM gongdan.gongdan_user
-- where dt = "2020-01-15"
-- where dt = "2020-08-08"
-- where dt = "2021-03-01"
-- where dt = "2021-11-11"
-- where dt = "2022-05-20"
-- where dt = "2023-01-01"
-- where dt = "2023-06-18"
-- where dt = "2018-04-10"
-- where dt = "2019-07-20"
;













select *
from gongdan.gongdan_user
where dt = '2020-01-15';


select user_id, username, nickname, gender, age, birthday, register_date, register_channel, vip_level, vip_expire_date, phone, email, province, city, tags, is_musician, musician_level, update_time, dt
from gongdan_user;

-- INSERT INTO gongdan_user (user_id, username, nickname, gender, age, birthday, register_date, register_channel,
--                           vip_level, vip_expire_date, phone, email, province, city, tags, is_musician, musician_level,
--                           update_time)
-- VALUES ('U001', 'zhangsan', '三爷', 'M', 28, '1997-05-10', '2020-01-15', 'AppStore', 3, '2025-12-31', '13812345678',
--         'zhangsan@example.com', '广东', '广州', ARRAY('科技爱好者', '电影迷'), FALSE, NULL, CURRENT_TIMESTAMP),
--        ('U002', 'lisi', '小李飞刀', 'F', 22, '2003-08-22', '2021-03-01', 'AndroidMarket', 0, NULL, '13987654321',
--         'lisi@example.com', '上海', '上海', ARRAY('流行音乐', '动漫'), FALSE, NULL, CURRENT_TIMESTAMP),
--        ('U003', 'wangwu', '音乐小王子', 'M', 35, '1990-11-03', '2019-07-20', 'Web', 5, '2026-06-30', '13700001111',
--         'wangwu@example.com', '北京', '北京', ARRAY('独立音乐人', '摇滚', '吉他'), TRUE, 8, CURRENT_TIMESTAMP),
--        ('U004', 'zhaoliu', '甜心', 'F', 19, '2006-02-14', '2022-09-05', 'WeChat', 1, '2024-11-15', '13622223333',
--         'zhaoliu@example.com', '四川', '成都', ARRAY('二次元', '美食'), FALSE, NULL, CURRENT_TIMESTAMP),
--        ('U005', 'qianqi', '老司机', 'M', 45, '1980-01-01', '2018-04-10', 'AppStore', 4, '2025-09-30', '13544445555',
--         'qianqi@example.com', '江苏', '南京', ARRAY('经典老歌', '汽车'), FALSE, NULL, CURRENT_TIMESTAMP),
--        ('U006', 'sunba', '神秘人', 'U', 30, '1995-04-25', '2023-01-01', 'QQ', 0, NULL, '13466667777',
--         'sunba@example.com', '湖北', '武汉', ARRAY('电子音乐'), FALSE, NULL, CURRENT_TIMESTAMP),
--        ('U007', 'zhoujiu', '嘻哈达人', 'M', 26, '1999-09-09', '2021-11-11', 'Web', 2, '2025-03-31', '13377778888',
--         'zhoujiu@example.com', '湖南', '长沙', ARRAY('说唱', '街舞'), TRUE, 5, CURRENT_TIMESTAMP),
--        ('U008', 'wuxiao', '古风仙女', 'F', 24, '2001-07-07', '2022-05-20', 'AppStore', 3, '2026-01-31', '13299990000',
--         'wuxiao@example.com', '浙江', '杭州', ARRAY('古风', '民乐'), FALSE, NULL, CURRENT_TIMESTAMP),
--        ('U009', 'zhengda', '硬核玩家', 'M', 32, '1993-03-12', '2020-08-08', 'AndroidMarket', 5, '2027-02-28',
--         '13111112222', 'zhengda@example.com', '山东', '青岛', ARRAY('游戏', '重金属'), FALSE, NULL, CURRENT_TIMESTAMP),
--        ('U010', 'fengxiao', '治愈系', 'F', 29, '1996-10-10', '2023-06-18', 'WeChat', 1, '2024-10-31', '13033334444',
--         'fengxiao@example.com', '福建', '厦门', ARRAY('轻音乐', '旅行'), FALSE, NULL, CURRENT_TIMESTAMP);

-- 歌曲信息表
create table if not exists gongdan_song
(
    song_id           STRING COMMENT "歌曲ID",
    song_name         STRING COMMENT "歌曲名称",
    song_alias        STRING COMMENT "歌曲别名",
    artist_id         STRING COMMENT "艺人ID",
    artist_name       STRING COMMENT "艺人名称",
    album_id          STRING COMMENT "专辑ID",
    album_name        STRING COMMENT "专辑名称",
    duration          INT COMMENT "时长(秒)",
    language          STRING COMMENT "语言",
    genre             STRING COMMENT "流派",
    publish_date      STRING COMMENT "发行日期",
    copyright_company STRING COMMENT "版权公司",
    is_original       BOOLEAN COMMENT "是否原创",
    is_high_quality   BOOLEAN COMMENT "是否高品质",
    is_free           BOOLEAN COMMENT "是否免费",
    play_count_total  BIGINT COMMENT "总播放量",
    lyric_count       INT COMMENT "歌词数量",
    comment_count     INT COMMENT "评论数量",
    update_time       TIMESTAMP COMMENT "更新时间"
) COMMENT "歌曲信息表";

INSERT INTO gongdan_song (song_id, song_name, song_alias, artist_id, artist_name, album_id, album_name, duration,
                          language, genre, publish_date, copyright_company, is_original, is_high_quality, is_free,
                          play_count_total, lyric_count, comment_count, update_time)
VALUES ('S001', '追梦人', 'Dream Chaser', 'A001', '李明', 'AL001', '追梦人', 240, '华语', '流行', '2023-01-01',
        '天籁唱片', TRUE, TRUE, FALSE, 15000000, 120, 5000, CURRENT_TIMESTAMP),
       ('S002', '城市之光', 'City Light', 'A001', '李明', 'AL001', '追梦人', 280, '华语', '流行', '2023-01-01',
        '天籁唱片', TRUE, TRUE, TRUE, 10000000, 150, 3000, CURRENT_TIMESTAMP),
       ('S003', '摇滚之心', 'Rock Heart', 'A002', '张华', 'AL002', '城市边缘', 300, '华语', '摇滚', '2022-05-10',
        '独立音乐', TRUE, TRUE, FALSE, 8000000, 180, 2500, CURRENT_TIMESTAMP),
       ('S004', '甜蜜蜜', 'Sweet Honey', 'A003', '王丽', 'AL003', '甜蜜约定', 200, '华语', '流行', '2023-03-20',
        '星光娱乐', FALSE, TRUE, TRUE, 12000000, 100, 4000, CURRENT_TIMESTAMP),
       ('S005', 'Midnight Drive', '午夜驾驶', 'A004', 'John Doe', 'AL004', 'Urban Groove', 260, '英语', 'R&B',
        '2024-01-15', '环球音乐', TRUE, TRUE, FALSE, 7000000, 130, 1500, CURRENT_TIMESTAMP),
       ('S006', 'Corcovado', '科尔科瓦多', 'A005', '小野丽莎', 'AL005', 'Quiet Moments', 220, '葡萄牙语', '爵士',
        '2021-11-01', '独立制作', FALSE, TRUE, TRUE, 5000000, 80, 800, CURRENT_TIMESTAMP),
       ('S007', '故乡的云', 'Cloud of Hometown', 'A006', '陈曦', 'AL006', '山河故人', 290, '华语', '民谣', '2023-08-01',
        '独立音乐', TRUE, TRUE, FALSE, 3000000, 160, 1000, CURRENT_TIMESTAMP),
       ('S008', 'Beat Drop', '节拍掉落', 'A007', 'DJ Electric', 'AL007', 'Future Beats', 180, '英语', 'EDM',
        '2024-03-01', '电音厂牌', TRUE, TRUE, TRUE, 2000000, 50, 300, CURRENT_TIMESTAMP),
       ('S009', '星辰大海', 'Stars and Sea', 'A008', '林语', 'AL008', '青春序曲', 250, '华语', '流行', '2024-05-20',
        '新星经纪', TRUE, TRUE, FALSE, 4000000, 110, 700, CURRENT_TIMESTAMP),
       ('S010', '自由飞翔', 'Free Flight', 'A009', 'Black Cat Band', 'AL009', '摇滚精神', 320, '华语', '摇滚',
        '2020-01-01', '独立音乐', TRUE, TRUE, FALSE, 3500000, 200, 600, CURRENT_TIMESTAMP),
       ('S011', '月光奏鸣曲', 'Moonlight Sonata', 'A010', 'Mikael', 'AL010', '琴声悠扬', 360, '纯音乐', '古典',
        '2023-09-01', '古典唱片', FALSE, TRUE, TRUE, 800000, 0, 100, CURRENT_TIMESTAMP),
       ('S012', '风的低语', 'Whispers of Wind', 'A006', '陈曦', 'AL006', '山河故人', 270, '华语', '民谣', '2023-08-01',
        '独立音乐', TRUE, TRUE, TRUE, 2500000, 140, 500, CURRENT_TIMESTAMP),
       ('S013', '未来之声', 'Voice of Future', 'A007', 'DJ Electric', 'AL007', 'Future Beats', 200, '英语', 'EDM',
        '2024-03-01', '电音厂牌', TRUE, TRUE, FALSE, 1800000, 60, 250, CURRENT_TIMESTAMP),
       ('S014', '爱在心间', 'Love in Heart', 'A001', '李明', 'AL001', '追梦人', 230, '华语', '流行', '2023-01-01',
        '天籁唱片', TRUE, TRUE, FALSE, 11000000, 115, 3500, CURRENT_TIMESTAMP),
       ('S015', '无尽的旅程', 'Endless Journey', 'A002', '张华', 'AL002', '城市边缘', 310, '华语', '摇滚', '2022-05-10',
        '独立音乐', TRUE, TRUE, TRUE, 6500000, 190, 2000, CURRENT_TIMESTAMP);


create table if not exists gongdan_artist
(
    artist_id    STRING COMMENT "艺人ID",
    artist_name  STRING COMMENT "艺人名称",
    artist_alias STRING COMMENT "艺人类别",
    gender       STRING COMMENT "性别",
    birth_date   STRING COMMENT "出生日期",
    country      STRING COMMENT "国家",
    province     STRING COMMENT "省份",
    city         STRING COMMENT "城市",
    category     STRING COMMENT "艺人分类",
    debut_date   STRING COMMENT "出道日期",
    company      STRING COMMENT "经纪公司",
    fans_count   BIGINT COMMENT "粉丝数量",
    song_count   INT COMMENT "歌曲数量",
    album_count  INT COMMENT "专辑数量",
    mv_count     INT COMMENT "MV数量",
    is_indie     BOOLEAN COMMENT "是否独立音乐人",
    tags         ARRAY<STRING>COMMENT "艺人标签",
    update_time  TIMESTAMP COMMENT "更新时间"
) COMMENT "艺人信息表"
;

INSERT INTO gongdan_artist (artist_id, artist_name, artist_alias, gender, birth_date, country, province, city, category,
                            debut_date, company, fans_count, song_count, album_count, mv_count, is_indie, tags,
                            update_time)
VALUES ('A001', '李明', '小明', 'M', '1985-03-15', '中国', '北京', '北京', '流行歌手', '2005-08-01', '天籁唱片',
        12000000, 150, 10, 20, FALSE, ARRAY('实力派', '情歌王子'), CURRENT_TIMESTAMP),
       ('A002', '张华', '摇滚老炮', 'M', '1978-07-22', '中国', '四川', '成都', '摇滚乐队', '1998-05-01', '独立音乐',
        3500000, 80, 5, 10, TRUE, ARRAY('摇滚', '现场感'), CURRENT_TIMESTAMP),
       ('A003', '王丽', '甜歌皇后', 'F', '1990-01-01', '中国', '上海', '上海', '流行歌手', '2010-01-01', '星光娱乐',
        8000000, 120, 8, 15, FALSE, ARRAY('甜美', '治愈'), CURRENT_TIMESTAMP),
       ('A004', 'John Doe', 'JD', 'M', '1992-04-05', '美国', NULL, '洛杉矶', 'R&B歌手', '2015-03-01', '环球音乐',
        5000000, 60, 4, 8, FALSE, ARRAY('R&B', '国际'), CURRENT_TIMESTAMP),
       ('A005', '小野丽莎', 'Bossa Nova女王', 'F', '1962-07-29', '巴西', NULL, NULL, '爵士歌手', '1989-01-01',
        '独立制作', 2000000, 200, 15, 5, TRUE, ARRAY('爵士', 'Bossa Nova'), CURRENT_TIMESTAMP),
       ('A006', '陈曦', '民谣诗人', 'M', '1988-09-10', '中国', '浙江', '杭州', '民谣歌手', '2012-10-01', '独立音乐',
        1500000, 40, 3, 2, TRUE, ARRAY('民谣', '原创'), CURRENT_TIMESTAMP),
       ('A007', 'DJ Electric', '电音之王', 'U', '1995-06-20', '英国', NULL, '伦敦', 'DJ', '2018-01-01', '电音厂牌',
        700000, 30, 2, 10, FALSE, ARRAY('EDM', '电子'), CURRENT_TIMESTAMP),
       ('A008', '林语', '新生代偶像', 'F', '2000-02-28', '中国', '湖南', '长沙', '偶像歌手', '2020-08-01', '新星经纪',
        4000000, 25, 2, 5, FALSE, ARRAY('偶像', '唱跳'), CURRENT_TIMESTAMP),
       ('A009', 'Black Cat Band', '黑猫乐队', 'U', '1990-11-11', '中国', '北京', '北京', '摇滚乐队', '1990-11-11',
        '独立音乐', 1000000, 50, 4, 6, TRUE, ARRAY('老牌乐队', '硬核'), CURRENT_TIMESTAMP),
       ('A010', 'Mikael', '古典新秀', 'M', '1998-05-05', '德国', NULL, '柏林', '古典音乐家', '2021-01-01', '古典唱片',
        100000, 10, 1, 1, FALSE, ARRAY('古典', '钢琴'), CURRENT_TIMESTAMP);

create table if not exists gongdan_album
(
    album_id         STRING COMMENT "专辑ID",
    album_name       STRING COMMENT "专辑名称",
    artist_id        STRING COMMENT "艺人ID",
    artist_name      STRING COMMENT "艺人名称",
    publish_date     STRING COMMENT "发行日期",
    company          STRING COMMENT "发行公司",
    language         STRING COMMENT "语言",
    genre            STRING COMMENT "流派",
    song_count       INT COMMENT "歌曲数量",
    play_count_total BIGINT COMMENT "总播放量",
    description      STRING COMMENT "专辑描述",
    cover_url        STRING COMMENT "封面URL",
    is_digital       BOOLEAN COMMENT "是否数字专辑",
    price            DECIMAL(10, 2) COMMENT "价格",
    update_time      TIMESTAMP COMMENT "更新时间"
) COMMENT "专辑信息表";

INSERT INTO gongdan_album (album_id, album_name, artist_id, artist_name, publish_date, company, language, genre,
                           song_count, play_count_total, description, cover_url, is_digital, price, update_time)
VALUES ('AL001', '追梦人', 'A001', '李明', '2023-01-01', '天籁唱片', '华语', '流行', 10, 50000000,
        '李明最新专辑，收录多首热门情歌。', 'http://example.com/covers/al001.jpg', FALSE, 58.00, CURRENT_TIMESTAMP),
       ('AL002', '城市边缘', 'A002', '张华', '2022-05-10', '独立音乐', '华语', '摇滚', 8, 15000000,
        '张华乐队的经典摇滚专辑。', 'http://example.com/covers/al002.jpg', TRUE, 30.00, CURRENT_TIMESTAMP),
       ('AL003', '甜蜜约定', 'A003', '王丽', '2023-03-20', '星光娱乐', '华语', '流行', 12, 30000000,
        '王丽的治愈系情歌合集。', 'http://example.com/covers/al003.jpg', FALSE, 68.00, CURRENT_TIMESTAMP),
       ('AL004', 'Urban Groove', 'A004', 'John Doe', '2024-01-15', '环球音乐', '英语', 'R&B', 9, 20000000,
        'John Doe的都市R&B专辑。', 'http://example.com/covers/al004.jpg', TRUE, 12.99, CURRENT_TIMESTAMP),
       ('AL005', 'Quiet Moments', 'A005', '小野丽莎', '2021-11-01', '独立制作', '葡萄牙语', '爵士', 14, 10000000,
        '小野丽莎的Bossa Nova精选。', 'http://example.com/covers/al005.jpg', TRUE, 9.99, CURRENT_TIMESTAMP),
       ('AL006', '山河故人', 'A006', '陈曦', '2023-08-01', '独立音乐', '华语', '民谣', 7, 8000000, '陈曦的民谣诗篇。',
        'http://example.com/covers/al006.jpg', TRUE, 25.00, CURRENT_TIMESTAMP),
       ('AL007', 'Future Beats', 'A007', 'DJ Electric', '2024-03-01', '电音厂牌', '英语', 'EDM', 6, 5000000,
        'DJ Electric的最新电音力作。', 'http://example.com/covers/al007.jpg', TRUE, 15.00, CURRENT_TIMESTAMP),
       ('AL008', '青春序曲', 'A008', '林语', '2024-05-20', '新星经纪', '华语', '流行', 5, 10000000,
        '林语的首张个人专辑。', 'http://example.com/covers/al008.jpg', FALSE, 49.00, CURRENT_TIMESTAMP),
       ('AL009', '摇滚精神', 'A009', 'Black Cat Band', '2020-01-01', '独立音乐', '华语', '摇滚', 10, 7000000,
        '黑猫乐队的经典现场专辑。', 'http://example.com/covers/al009.jpg', TRUE, 35.00, CURRENT_TIMESTAMP),
       ('AL010', '琴声悠扬', 'A010', 'Mikael', '2023-09-01', '古典唱片', '纯音乐', '古典', 8, 1000000,
        'Mikael的钢琴独奏专辑。', 'http://example.com/covers/al010.jpg', TRUE, 19.99, CURRENT_TIMESTAMP);

create table if not exists gongdan_device
(
    device_id     STRING COMMENT "设备ID",
    device_type   STRING COMMENT "设备类型(phone/pad/pc/tv)",
    brand         STRING COMMENT "品牌",
    model         STRING COMMENT "型号",
    os_type       STRING COMMENT "操作系统(ios/android/windows/mac)",
    os_version    STRING COMMENT "系统版本",
    screen_width  INT COMMENT "屏幕宽度",
    screen_height INT COMMENT "屏幕高度",
    network_type  STRING COMMENT "网络类型",
    resolution    STRING COMMENT "分辨率",
    cpu_type      STRING COMMENT "CPU类型",
    ram_size      INT COMMENT "内存大小(GB)",
    rom_size      INT COMMENT "存储大小(GB)",
    update_time   TIMESTAMP COMMENT "更新时间"

) COMMENT "设备信息表";

INSERT INTO gongdan_device (device_id, device_type, brand, model, os_type, os_version, screen_width, screen_height,
                            network_type, resolution, cpu_type, ram_size, rom_size, update_time)
VALUES ('D001', 'phone', 'Apple', 'iPhone 13 Pro', 'iOS', '16.5', 1170, 2532, '5G', '2532x1170', 'A15 Bionic', 6, 256,
        CURRENT_TIMESTAMP),
       ('D002', 'phone', 'Samsung', 'Galaxy S23 Ultra', 'Android', '13', 1440, 3088, '5G', '3088x1440',
        'Snapdragon 8 Gen 2', 12, 512, CURRENT_TIMESTAMP),
       ('D003', 'pad', 'Apple', 'iPad Air 5', 'iOS', '16.4', 1640, 2360, 'Wi-Fi', '2360x1640', 'M1', 8, 128,
        CURRENT_TIMESTAMP),
       ('D004', 'pc', 'Lenovo', 'ThinkPad X1 Carbon', 'Windows', '11 Pro', 1920, 1080, 'Wi-Fi', '1920x1080',
        'Intel i7-1260P', 16, 1024, CURRENT_TIMESTAMP),
       ('D005', 'pc', 'Apple', 'MacBook Pro 14', 'macOS', 'Ventura', 3024, 1964, 'Wi-Fi', '3024x1964', 'M2 Pro', 16,
        512, CURRENT_TIMESTAMP),
       ('D006', 'tv', 'Sony', 'Bravia XR A95K', 'Android', '10', 3840, 2160, 'Ethernet', '3840x2160',
        'Cognitive Processor XR', 4, 32, CURRENT_TIMESTAMP),
       ('D007', 'phone', 'Huawei', 'Mate 50 Pro', 'Android', 'HarmonyOS 3.0', 1212, 2700, '4G', '2700x1212',
        'Kirin 9000S', 8, 256, CURRENT_TIMESTAMP),
       ('D008', 'phone', 'Xiaomi', 'Xiaomi 13 Ultra', 'Android', '14', 1440, 3200, '5G', '3200x1440',
        'Snapdragon 8 Gen 2', 12, 512, CURRENT_TIMESTAMP),
       ('D009', 'pad', 'Samsung', 'Galaxy Tab S8', 'Android', '13', 1600, 2560, 'Wi-Fi', '2560x1600',
        'Snapdragon 8 Gen 1', 8, 256, CURRENT_TIMESTAMP),
       ('D010', 'pc', 'Dell', 'XPS 15', 'Windows', '10 Home', 1920, 1200, 'Wi-Fi', '1920x1200', 'Intel i9-13900H', 32,
        2048, CURRENT_TIMESTAMP),
       ('D011', 'phone', 'OnePlus', 'OnePlus 11', 'Android', '13', 1440, 3216, '5G', '3216x1440', 'Snapdragon 8 Gen 2',
        16, 512, CURRENT_TIMESTAMP),
       ('D012', 'tv', 'LG', 'OLED C2', 'WebOS', '7.0', 3840, 2160, 'Wi-Fi', '3840x2160', 'α9 Gen5 AI Processor 4K', 3,
        16, CURRENT_TIMESTAMP);


