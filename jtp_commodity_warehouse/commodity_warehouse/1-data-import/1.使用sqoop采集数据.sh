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

# 导入 ods_order_info
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--verbose \
--driver com.mysql.cj.jdbc.Driver \
--connect jdbc:mysql://node103:3306/jtp_flow_topic \
--username root \
--password 123456 \
--query \"SELECT order_id, user_id, product_id, order_amount, order_time, pay_time, order_status, date_id FROM order_info WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/ods_order_info/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string 'NULL' \\
--null-non-string 'NULL' \\
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 ods_order_info 失败！"; exit 1; fi
echo "Sqoop导入 ods_order_info 成功。"

# 导入 ods_product_info
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--verbose \
--driver com.mysql.cj.jdbc.Driver \
--connect jdbc:mysql://node103:3306/jtp_flow_topic \
--username root \
--password 123456 \
--query \"SELECT product_id, product_name, category FROM product_info WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/ods_product_info/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string 'NULL' \\
--null-non-string 'NULL' \\
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 ods_product_info 失败！"; exit 1; fi
echo "Sqoop导入 ods_product_info 成功。"

# 导入 ods_page_info
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--verbose \
--driver com.mysql.cj.jdbc.Driver \
--connect jdbc:mysql://node103:3306/jtp_flow_topic \
--username root \
--password 123456 \
--query \"SELECT page_id, page_name, page_type FROM page_info WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/ods_page_info/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string 'NULL' \\
--null-non-string 'NULL' \\
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 ods_page_info 失败！"; exit 1; fi
echo "Sqoop导入 ods_page_info 成功。"

# 导入 ods_user_action_log (已修正FROM子句)
ssh "${SSH_USER}@${REMOTE_HOST}" "${SQOOP_BIN} import \
--verbose \
--driver com.mysql.cj.jdbc.Driver \
--connect jdbc:mysql://node103:3306/jtp_flow_topic \
--username root \
--password 123456 \
--query \"SELECT log_id, user_id, session_id, page_id, element_id, event_type, event_time, product_id, ip_address, device_type, date_id FROM user_action_log WHERE \\\$CONDITIONS AND 1 = 1;\" \
--delete-target-dir \
--target-dir \"hdfs://node101:8020/user/spark/warehouse/jtp_flow_topic_warehouse/ods_user_action_log/dt=${data_date}\" \
--as-textfile \
--fields-terminated-by \",\" \
--null-string 'NULL' \\
--null-non-string 'NULL' \\
--num-mappers 1"

if [ $? -ne 0 ]; then echo "Sqoop导入 ods_user_action_log 失败！"; exit 1; fi
echo "Sqoop导入 ods_user_action_log 成功。"

echo "所有 Sqoop 导入任务完成。"