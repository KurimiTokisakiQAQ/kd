import pymysql
import time

# ===================
# 示例连接信息
# ===================

TABLE_NAME = "dwd_idc_life_ent_soc_public_sentiment_battery_work_mix_rt"             # 表名

# 创建数据库连接
conn = pymysql.connect(
    host="da-dw-tidb-10900.chj.cloud",
    port=3306,
    user="da_algo_craw_wr",
    password="99FBD18120C777560A9451FB65A8E74F60CFBBD3",
    database="da_crawler_dw",
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor  # 返回字典结构
)

cursor = conn.cursor()

# 假设表有一个自增主键 id 来判断新记录
last_id = 0

print(f"开始监测 TiDB 表 {TABLE_NAME}，Ctrl+C 可退出...")
try:
    while True:
        # 查询比 last_id 大的记录
        sql = f"SELECT * FROM {TABLE_NAME} WHERE id > %s ORDER BY id ASC"
        cursor.execute(sql, (last_id,))
        rows = cursor.fetchall()

        if rows:
            for row in rows:
                print("检测到新数据:", row)
                # 更新 last_id 为最新行的 id
                if row['id'] > last_id:
                    last_id = row['id']

        # 休眠几秒再查，防止查询过于频繁
        time.sleep(3)

except KeyboardInterrupt:
    print("监测程序已退出")
finally:
    cursor.close()
    conn.close()