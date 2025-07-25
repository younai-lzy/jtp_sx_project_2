#!/bin/bash

# 定义数据日期变量
if [ -n "$1" ] ; then
  data_date=$1
else
  data_date=`date -d "-1 days" +%F`
fi

echo "开始执行 Sqoop 数据导入到 HDFS，日期: ${data_date}"

# 定义远程执行 Sqoop 的 SSH 用户和主机
# 请将 'bwie' 替换为你在 node101 上的实际用户名
SSH_USER="bwie"
REMOTE_HOST="node101"
SQOOP_BIN="/opt/module/sqoop/bin/sqoop"

# 导入 songgou_user_info
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--connect jdbc:mysql://node103:3306/jtp_sgcx \
--username root \
--password 123456 \
--query \"SELECT user_id, register_time, register_city_id, gender, age_range, is_certified, last_active_date, user_level FROM songgou_user_info WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songgou_user_info/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string \"\\\\N\" \
--null-non-string \"\\\\N\" \
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 songgou_user_info 失败！"; exit 1; fi
echo "Sqoop导入 songgou_user_info 成功。"

# 导入 songguo_bike_info
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--connect jdbc:mysql://node103:3306/jtp_sgcx \
--username root \
--password 123456 \
--query \"SELECT bike_id, bike_type, manufacture_date, battery_type, initial_city_id, is_shared FROM songguo_bike_info WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_bike_info/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string \"\\\\N\" \
--null-non-string \"\\\\N\" \
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 songguo_bike_info 失败！"; exit 1; fi
echo "Sqoop导入 songguo_bike_info 成功。"

# 导入 songguo_city_info
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--connect jdbc:mysql://node103:3306/jtp_sgcx \
--username root \
--password 123456 \
--query \"SELECT city_id, city_name, province_id, province_name, region_id, is_hot_city FROM songguo_city_info WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_city_info/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string \"\\\\N\" \
--null-non-string \"\\\\N\" \
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 songguo_city_info 失败！"; exit 1; fi
echo "Sqoop导入 songguo_city_info 成功。"

# 导入 songguo_bike_status_type (已修正FROM子句)
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--connect jdbc:mysql://node103:3306/jtp_sgcx \
--username root \
--password 123456 \
--query \"SELECT status_id, status_name, is_operational, priority FROM songguo_bike_status_type WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_bike_status_type/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string \"\\\\N\" \
--null-non-string \"\\\\N\" \
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 songguo_bike_status_type 失败！"; exit 1; fi
echo "Sqoop导入 songguo_bike_status_type 成功。"

# 导入 songguo_campaign
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--connect jdbc:mysql://node103:3306/jtp_sgcx \
--username root \
--password 123456 \
--query \"SELECT campaign_id, campaign_name, start_time, end_time, campaign_type, target_city_ids FROM songguo_campaign WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_campaign/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string \"\\\\N\" \
--null-non-string \"\\\\N\" \
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 songguo_campaign 失败！"; exit 1; fi
echo "Sqoop导入 songguo_campaign 成功。"

# 导入 songgou_order_info
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--connect jdbc:mysql://node103:3306/jtp_sgcx \
--username root \
--password 123456 \
--query \"SELECT order_id, user_id, bike_id, start_time, end_time, start_lng, start_lat, end_lng, end_lat, distance, duration, base_fee, extra_fee, total_fee, coupon_amount, actual_pay, pay_type, pay_status, city_id, region_id, is_night_ride, is_first_ride FROM songgou_order_info WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songgou_order_info/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string \"\\\\N\" \
--null-non-string \"\\\\N\" \
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 songgou_order_info 失败！"; exit 1; fi
echo "Sqoop导入 songgou_order_info 成功。"

# 导入 songguo_bike_status
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--connect jdbc:mysql://node103:3306/jtp_sgcx \
--username root \
--password 123456 \
--query \"SELECT bike_id, city_id, district_id, battery_level, is_rented, is_maintenance, is_damaged, gps_lng, gps_lat, status_time FROM songguo_bike_status WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_bike_status/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string \"\\\\N\" \
--null-non-string \"\\\\N\" \
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 songguo_bike_status 失败！"; exit 1; fi
echo "Sqoop导入 songguo_bike_status 成功。"

# 导入 songguo_bike_operation
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--connect jdbc:mysql://node103:3306/jtp_sgcx \
--username root \
--password 123456 \
--query \"SELECT operation_id, bike_id, operator_id, operation_type, operation_time, before_battery, after_battery, before_status, after_status, city_id, district_id FROM songguo_bike_operation WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_bike_operation/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string \"\\\\N\" \
--null-non-string \"\\\\N\" \
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 songguo_bike_operation 失败！"; exit 1; fi
echo "Sqoop导入 songguo_bike_operation 成功。"

# 导入 songguo_transaction_record
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--connect jdbc:mysql://node103:3306/jtp_sgcx \
--username root \
--password 123456 \
--query \"SELECT transaction_id, order_id, user_id, transaction_type, transaction_amount, transaction_time, payment_channel, payment_status, merchant_id, city_id FROM songguo_transaction_record WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_transaction_record/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string \"\\\\N\" \
--null-non-string \"\\\\N\" \
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 songguo_transaction_record 失败！"; exit 1; fi
echo "Sqoop导入 songguo_transaction_record 成功。"

# 导入 songguo_user_behavior (修正表名)
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--connect jdbc:mysql://node103:3306/jtp_sgcx \
--username root \
--password 123456 \
--query \"SELECT user_id, behavior_type, behavior_time, device_id, app_version, os_type, ip, city_id FROM songguo_user_behavior WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/songguo_warehouse/songguo_user_behavior/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string \"\\\\N\" \
--null-non-string \"\\\\N\" \
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 songguo_user_behavior 失败！"; exit 1; fi
echo "Sqoop导入 songguo_user_behavior 成功。"

echo "所有 Sqoop 导入任务完成。"
