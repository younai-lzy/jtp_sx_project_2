#!/bin/bash

# --- 0. 从 DolphinScheduler 环境中获取并导出 biz_date ---
# 这行确保 DolphinScheduler 的 ${biz_date} 变量值能够被本 Shell 脚本正确捕获和使用。
# 配合 DolphinScheduler 任务配置中的 "自定义参数" (Custom Parameters) 使用。
export biz_date=${biz_date}

# 如果上述 export 失败（例如 DolphinScheduler版本过老或配置问题），
# 并且您希望在脚本内部有兜底逻辑，可以使用以下方式获取昨日日期（不推荐用于精确调度日期）
# if [ -z "${biz_date}" ]; then
#     echo "警告：DolphinScheduler变量 biz_date 未能成功获取，将使用昨日日期作为兜底。"
#     # 获取昨日日期，格式 YYYYMMDD
#     biz_date=$(date -d "yesterday" +%Y%m%d)
#     echo "兜底日期设置为: ${biz_date}"
# fi


# --- 1. 设置核心环境变量 ---
# 请确保这些路径与您的集群实际部署路径一致
export JAVA_HOME="/opt/module/jdk"
export HADOOP_HOME="/opt/module/hadoop"
export SQOOP_HOME="/opt/module/sqoop"
export HIVE_HOME="/opt/module/hive"
export HBASE_HOME="/opt/module/hbase" # 确保 HBase Home 也被设置

# Hadoop 配置目录 (必须的，因为 Sqoop 依赖 Hadoop)
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"

# --- 2. 构建完整的 HADOOP_CLASSPATH ---
HADOOP_CLASSPATH=""

# 1. 获取 Hadoop 自身的 classpath
HADOOP_CLASSPATH_FROM_HADOOP_CMD="$(${HADOOP_HOME}/bin/hadoop classpath)"
if [ -z "${HADOOP_CLASSPATH_FROM_HADOOP_CMD}" ]; then
    echo "错误：无法通过 'hadoop classpath' 获取 Hadoop JAR 包路径。请检查 HADOOP_HOME 设置和 'bwie' 用户权限。"
    exit 1
fi
HADOOP_CLASSPATH="${HADOOP_CLASSPATH_FROM_HADOOP_CMD}"

# 2. 添加 Hive HCatalog JARs
if [ -d "${HIVE_HOME}/hcatalog/share/hcatalog" ]; then
  for jar in "${HIVE_HOME}"/hcatalog/share/hcatalog/*.jar; do
    HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${jar}"
  done
else
    echo "警告：未找到 Hive HCatalog 目录 ${HIVE_HOME}/hcatalog/share/hcatalog，如果使用 HCatalog，可能会出现问题。"
fi

# 3. 添加 Hive Lib JARs
if [ -d "${HIVE_HOME}/lib" ]; then
    for jar in "${HIVE_HOME}"/lib/*.jar; do
        HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${jar}"
    done
else
    echo "警告：未找到 Hive lib 目录 ${HIVE_HOME}/lib。"
fi

# 4. 添加 HBase Lib JARs (这是解决 'GetJavaProperty' 错误的关键部分)
# Sqoop 可能会在内部探测 HBase，所以确保 HBase 的客户端 JAR 包在 CLASSPATH 中。
if [ -d "${HBASE_HOME}/lib" ]; then
  for jar in "${HBASE_HOME}"/lib/*.jar; do
    HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${jar}"
  C  done
else
    echo "警告：未找到 HBase lib 目录 ${HBASE_HOME}/lib。这可能会导致 Sqoop 在某些情况下失败。"
fi

# 导出最终的 HADOOP_CLASSPATH
export HADOOP_CLASSPATH

# --- 3. 打印环境信息 (便于检查) --
echo "--- 诊断环境信息 ---"
echo "JAVA_HOME: ${JAVA_HOME}"
echo "HADOOP_HOME: ${HADOOP_HOME}"
echo "HIVE_HOME: ${HIVE_HOME}"
echo "HBASE_HOME: ${HBASE_HOME}"
echo "SQOOP_HOME: ${SQOOP_HOME}"
echo "HADOOP_CONF_DIR: ${HADOOP_CONF_DIR}"
echo "HADOOP_CLASSPATH (部分显示，完整路径很长): ${HADOOP_CLASSPATH:0:500}..." # 只显示前500字符
echo "--------------------"

# --- 4. 定义 Sqoop 任务参数 ---
# 您需要根据实际情况修改这些参数
DORIS_TABLE="ads_page_analysis_daily"
HCATALOG_TABLE="ads_page_analysis_daily"
HIVE_DATABASE="jtp_flow_topic_warehouse"
# ${biz_date} 是 DolphinScheduler 的内置变量，运行时会被替换为业务日期
EXPORT_DATE="${biz_date}" # 使用上面导出的 biz_date 变量

DORIS_FE_HOST="node102"
DORIS_FE_QUERY_PORT="9030"
DORIS_USERNAME="root"
# 密码文件路径，请确保 DolphinScheduler Worker 节点上的 bwie 用户有权限读取此文件
DORIS_PASSWORD_FILE="/opt/module/dolphinscheduler/script/doris_password.txt"

# 映射列类型，根据您的表结构调整
MAP_COLUMN_JAVA_ARGS="dt=String,page_id=String,page_name=String,page_type=String,total_page_views=Long,unique_visitors=Long,total_clicks=Long,unique_clickers=Long,add_to_cart_count=Long,purchase_count=Long,conversion_rate_click=BigDecimal,conversion_rate_add_to_cart=BigDecimal,conversion_rate_purchase=BigDecimal"
NUM_MAPPERS="1" # MapReduce 并行度

# --- 5. 构建 Sqoop 命令 ---
# 注意：使用双引号包裹整个命令，并对其中的变量进行引用，以正确处理空格和特殊字符
SQOOP_COMMAND="${SQOOP_HOME}/bin/sqoop export \
  --connect jdbc:mysql://${DORIS_FE_HOST}:${DORIS_FE_QUERY_PORT}/${HIVE_DATABASE} \
  --username ${DORIS_USERNAME} \
  --password-file file://${DORIS_PASSWORD_FILE} \
  --table ${DORIS_TABLE} \
  --export-dir /user/hive/warehouse/${HIVE_DATABASE}.db/${HCATALOG_TABLE}/dt=${EXPORT_DATE} \
  --hcatalog-table ${HCATALOG_TABLE} \
  --hcatalog-database ${HIVE_DATABASE} \
  --num-mappers ${NUM_MAPPERS} \
  --hcatalog-home ${HIVE_HOME}/hcatalog \
  --map-column-java ${MAP_COLUMN_JAVA_ARGS} \
  --update-mode allowinsert \
  --input-null-string '\\N' \
  --input-null-non-string '\\N'"

# --- 6. 打印并执行 Sqoop 命令 ---
echo "--- 将要执行的 Sqoop 命令 ---"
echo "${SQOOP_COMMAND}"
echo "-----------------------------"

# 执行 Sqoop 命令
# 使用 'eval' 可以确保所有变量和路径被正确解析
eval "${SQOOP_COMMAND}"

# 检查 Sqoop 命令的退出状态码
if [ $? -ne 0 ]; then
  echo "错误：Sqoop 导出到 Doris 表 ${DORIS_TABLE} 失败！请检查上述 Sqoop 命令和日志中的详细错误信息。"
  exit 1
else
  echo "Sqoop 导出到 Doris 表 ${DORIS_TABLE} 成功完成！"
  exit 0
fi