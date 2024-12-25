import pymysql

# 数据库连接信息
db_host = 'localhost'
db_user = 'root'
db_password = '123456'
db_name = 'www'  # 源数据库名
target_db_name = 'weather'  # 目标数据库名

# 连接到源数据库
connection = pymysql.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)

# 连接到目标数据库
target_connection = pymysql.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=target_db_name
)

try:
    with connection.cursor() as cursor, target_connection.cursor() as target_cursor:
        # 获取所有表名
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]

        # 创建新的表 北京
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS 北京 (
            地区 VARCHAR(50),
            日期 VARCHAR(50),
            星期 VARCHAR(50),
            最高温 VARCHAR(50),
            最低温 VARCHAR(50),
            天气 VARCHAR(50),
            风向 VARCHAR(50),
            空气质量指数 VARCHAR(50),
            空气质量 VARCHAR(50)
        )
        """
        target_cursor.execute(create_table_sql)
        
        # 将所有表的数据插入到新的表中
        for table in tables:
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            if rows:
                columns = ['地区', '日期', '星期', '最高温', '最低温', '天气', '风向', '空气质量指数', '空气质量']
                placeholders = ', '.join(['%s'] * len(columns))
                insert_sql = f"INSERT INTO 北京 ({', '.join(columns)}) VALUES ({placeholders})"
                for row in rows:
                    row_values = (table,) + row  # 在前面添加地区字段的值
                    target_cursor.execute(insert_sql, row_values)

        # 提交更改到目标数据库
        target_connection.commit()

finally:
    connection.close()
    target_connection.close()
