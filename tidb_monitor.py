# monitor_tidb.py
import pymysql
import time
import json
import subprocess

TABLE_NAME = "dwd_idc_life_ent_soc_public_sentiment_battery_work_mix_rt"

# 数据库连接信息
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
last_id = 700

print(f"开始监测 TiDB 表 {TABLE_NAME}，Ctrl+C 可退出...")

try:
    while True:
        sql = f"SELECT * FROM {TABLE_NAME} WHERE id > %s ORDER BY id ASC"
        cursor.execute(sql, (last_id,))
        rows = cursor.fetchall()

        if rows:
            for row in rows:
                # 只打印 work_id 到控制台
                print(f"🔍 检测到新数据，work_id: {row.get('work_id', '')}")

                # 转换成 JSON 字符串（所有类型都转成可序列化）
                row_json_str = json.dumps(row, ensure_ascii=False, default=str)

                # 调用 notify_llm.py：完成相关性判定、摘要与烈度生成、飞书推送，并落库到通知表
                try:
                    subprocess.run(["python3", "notify_llm.py", row_json_str])
                except Exception as e:
                    print(f"❌ 调用飞书通知脚本失败: {e}")

                # 更新 last_id
                if row.get('id', 0) > last_id:
                    last_id = row['id']

        time.sleep(60)

except KeyboardInterrupt:
    print("监测程序已退出")
finally:
    cursor.close()
    conn.close()