import os
import subprocess
import pandas as pd
from pyspark.sql import SparkSession

# 设置环境变量和配置
os.environ['PYSPARK_PYTHON'] = "/opt/venv/bin/python"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars hdfs://node01:9000/user/JDBC/mysql-connector-java-8.0.29.jar pyspark-shell'

# 定义函数执行Shell命令
def execute_shell_command(command):
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"执行命令时出错: {e.stderr}")

# 创建Hive数据库和表的命令
create_database_command = """
hive -e "create database if not exists bjweather;"
"""

create_ods_table_command = """
hive -e "
CREATE TABLE if not exists bjweather.bj_ods (
  district string,
  year_mon_day string,
  week string,
  max_t string,
  min_t string,
  weather string,
  wind string,
  aqi string,
  aqi_type string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^WEA';"
"""

# 使用Sqoop从MySQL导入数据到Hive表的命令
sqoop_import_command = """
/opt/sqoop-1.4.7/bin/sqoop import \
 --connect jdbc:mysql://192.168.137.1:3306/weather \
 --username root \
 --password 123456 \
 --delete-target-dir \
 --target-dir /sqoop_to_hive \
 --hive-import \
 --hive-database bjweather \
 --hive-table bj_ods \
 --fields-terminated-by '^WEA' \
 --hive-drop-import-delims \
 --as-textfile --hive-overwrite \
 --null-string '\' \
 --null-non-string '\' \
 --query 'select * from 北京 where $CONDITIONS' \
 -m 1
"""

# 执行Hive和Sqoop命令
commands = [
    create_database_command,
    create_ods_table_command,
    sqoop_import_command
]

for command in commands:
    execute_shell_command(command)
print("执行成功")
# 创建SparkSession对象，启用Hive支持
spark = SparkSession.builder.\
    appName("Loadclean to Hive").\
    config("spark.sql.warehouse.dir", "hdfs://192.168.137.128:8020/user/hive/warehouse").\
    config("hive.metastore.uris", "thrift://192.168.137.128:9083").\
    enableHiveSupport().\
    getOrCreate()

print("创建成功")
# 从bj_ods表读取数据到Spark DataFrame
df_spark = spark.table("bjweather.bj_ods")
print("读取成功")
# 将Spark DataFrame转换为Pandas DataFrame
df_pandas = df_spark.toPandas()
print("转换成功")
# 数据处理：使用Pandas进行处理
df_pandas['max_t'] = df_pandas['max_t'].str.replace('°', '').astype(int)
df_pandas['min_t'] = df_pandas['min_t'].str.replace('°', '').astype(int)
df_pandas['aqi'] = pd.to_numeric(df_pandas['aqi'], errors='coerce').astype('float')
df_pandas['aqi'] = df_pandas.groupby(['district', df_pandas['year_mon_day'].str[5:10]])['aqi'].transform(lambda x: x.fillna(x.mean()).round())
print("填充成功")
# 定义函数映射aqi_type
def map_aqi_type(aqi_value):
    if pd.isnull(aqi_value):
        return None
    elif aqi_value <= 50:
        return '优'
    elif aqi_value <= 100:
        return '良'
    elif aqi_value <= 150:
        return '轻度污染'
    elif aqi_value <= 200:
        return '中度污染'
    elif aqi_value <= 300:
        return '重度污染'
    else:
        return '严重污染'

# 填充 'aqi_type' 列的缺失值
df_pandas['aqi_type'] = df_pandas['aqi'].apply(map_aqi_type)

# 排序数据
df_pandas.sort_values(by=['district', 'year_mon_day'], inplace=True)
print("排序成功")
# 将处理后的Pandas DataFrame转换为Spark DataFrame
df_spark_processed = spark.createDataFrame(df_pandas)
print("处理成功")
#df_spark_processed = df_spark_processed.coalesce(1)  # 将数据合并到一个分区

# 将处理后的数据写入Hive
df_spark_processed.write \
    .mode('overwrite') \
    .saveAsTable("bjweather.bj_dwd")

print("写入成功")
# 停止SparkSession
spark.stop()
