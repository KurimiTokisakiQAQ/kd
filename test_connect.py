import pymysql

# TiDB/MySQL 连接配置
HOST = "da-dw-tidb-10900.chj.cloud"     # 集群域名
PORT = 3306                             # 注意：这里你写的是 3306，如果 TiDB MySQL 端口是 4000，需要改成 4000
USER = "da_algo_craw_wr"                # 用户名
PASSWORD = "99FBD18120C777560A9451FB65A8E74F60CFBBD3"  # 密码
DATABASE = "da_crawler_dw"              # 数据库名

try:
    print(f"尝试连接 TiDB 数据库: {HOST}:{PORT} ...")
    conn = pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor  # 查询结果返回字典
    )
    print("✅ 数据库连接成功！")

    cursor = conn.cursor()

    # 测试执行简单查询
    cursor.execute("SELECT VERSION() AS tidb_version;")
    version_result = cursor.fetchall()
    print("TiDB 版本信息:", version_result)

    cursor.execute("SELECT NOW() AS current_time;")
    time_result = cursor.fetchall()
    print("当前时间:", time_result)

    # 关闭连接
    cursor.close()
    conn.close()
    print("✅ 数据库连接已关闭")

except pymysql.MySQLError as e:
    print("❌ 数据库连接失败:", e)
except Exception as ex:
    print("❌ 发生未知错误:", ex)