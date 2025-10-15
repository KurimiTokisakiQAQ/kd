# monitor_tidb.py
import pymysql
import time
import json
import subprocess

TABLE_NAME = "dwd_idc_life_ent_soc_public_sentiment_battery_work_mix_rt"

# 数据库连接参数
conn = pymysql.connect(
    host="da-dw-tidb-10900.chj.cloud",
    port=3306,
    user="da_algo_craw_wr",
    password="99FBD18120C777560A9451FB65A8E74F60CFBBD3",
    database="da_crawler_dw",
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor
)

cursor = conn.cursor()

last_id = 0

print(f"开始监测 TiDB 表 {TABLE_NAME}，Ctrl+C 可退出...")
try:
    while True:
        sql = f"SELECT * FROM {TABLE_NAME} WHERE id > %s ORDER BY id ASC"
        cursor.execute(sql, (last_id,))
        rows = cursor.fetchall()

        if rows:
            for row in rows:
                print("检测到新数据:", row)

                # 直接调用 feishu_notify.py 脚本，并传入 JSON 字符串
                try:
                    row_json_str = json.dumps(row, ensure_ascii=False)
                    subprocess.run(["python3", "feishu_notify.py", row_json_str])
                except Exception as e:
                    print(f"❌ 调用 feishu 通知脚本失败: {e}")

                # 更新 last_id
                if row['id'] > last_id:
                    last_id = row['id']

        time.sleep(3)

except KeyboardInterrupt:
    print("监测程序已退出")
finally:
    cursor.close()
    conn.close()