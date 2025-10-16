# -*- coding: utf-8 -*-
# monitor_and_notify.py
import requests
import json
import datetime
import base64
import sys
import time
import pymysql

# ================== 飞书推送配置 ==================
WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/c74b9141-1759-40e2-ae3a-50cc6389e1bc"
FEISHU_OPEN_ID = "ou_20b2bd16a8405b93019b7291ec5202c3"

FIELD_MAP = {
    "work_id":       "主贴ID",
    "work_url":      "主贴链接",
    "work_title":    "主贴标题",
    "work_content":  "正文内容",
    "publish_time":  "发布时间",
    "crawled_time":  "抓取时间",
    "account_name":  "账号名称",
    "source":        "来源平台",
    "like_cnt":      "点赞数",
    "reply_cnt":     "评论数",
    "forward_cnt":   "转发数",
    "content_senti": "内容情感",
    "ocr_content":   "OCR识别内容"
}

def double_base64_decode(s: str) -> str:
    try:
        return base64.b64decode(base64.b64decode(s)).decode("utf-8")
    except Exception:
        return f"[解码失败]{s}"

def truncate_text(text: str, limit=200) -> str:
    if text is None:
        return ""
    text = str(text)
    return text[:limit] + "..." if len(text) > limit else text

def map_senti(val):
    try:
        v = int(val)
    except Exception:
        return str(val)
    return {-1: "负面", 0: "中性", 1: "正面"}.get(v, str(v))

def send_to_feishu(data: dict):
    post_content = []

    for k, v in data.items():
        if k == "id":
            continue

        if isinstance(v, datetime.datetime):
            v = v.strftime("%Y-%m-%d %H:%M:%S")
        if v is None:
            v = ""

        if k in ("work_title", "work_content", "ocr_content"):
            v = truncate_text(v, limit=200)
        if k == "account_name":
            v = double_base64_decode(v)
        if k == "content_senti":
            v = map_senti(v)

        label = FIELD_MAP.get(k, k)

        post_content.append([
            {"tag": "text", "text": f"【{label}】: {v}"}
        ])

    post_content.append([
        {"tag": "at", "user_id": FEISHU_OPEN_ID}
    ])

    payload = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": "📢 新增记录告警",
                    "content": post_content
                }
            }
        }
    }

    try:
        resp = requests.post(
            WEBHOOK_URL,
            headers={"Content-Type": "application/json; charset=utf-8"},
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8")
        )
        ok = False
        if resp.status_code == 200:
            try:
                j = resp.json()
                if j.get("StatusCode") == 0 or j.get("code") == 0:
                    ok = True
            except Exception:
                ok = True
        if ok:
            print("✅ 飞书消息已发送并@指定人")
        else:
            print(f"❌ 飞书消息发送失败: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"❌ 调用飞书接口异常: {e}")

# ================== TiDB 监控配置与逻辑 ==================
TABLE_NAME = "dwd_idc_life_ent_soc_public_sentiment_battery_work_mix_rt"

DB_CONFIG = {
    "host": "da-dw-tidb-10900.chj.cloud",
    "port": 3306,
    "user": "da_algo_craw_wr",
    "password": "99FBD18120C777560A9451FB65A8E74F60CFBBD3",
    "database": "da_crawler_dw",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

def monitor_tidb():
    conn = None
    cursor = None
    last_id = 0
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print(f"开始监测 TiDB 表 {TABLE_NAME}，Ctrl+C 可退出...")

        while True:
            sql = f"SELECT * FROM {TABLE_NAME} WHERE id > %s ORDER BY id ASC"
            cursor.execute(sql, (last_id,))
            rows = cursor.fetchall()

            if rows:
                for row in rows:
                    # 只打印 work_id 到控制台
                    print(f"🔍 检测到新数据，work_id: {row.get('work_id', '')}")

                    # 推送到飞书
                    send_to_feishu(row)

                    # 更新 last_id
                    rid = row.get('id', 0)
                    if rid > last_id:
                        last_id = rid

            time.sleep(3)

    except KeyboardInterrupt:
        print("监测程序已退出")
    except Exception as e:
        print(f"❌ 监测过程中发生错误: {e}")
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception:
            pass

# ================== 主入口 ==================
if __name__ == "__main__":
    # 使用方式：
    # 1) 监控 TiDB 并推送：python3 monitor_and_notify.py monitor
    # 2) 直接推送命令行传入的 JSON：python3 monitor_and_notify.py '<JSON字符串>'
    # 3) 无参数时，使用示例数据进行测试推送
    if len(sys.argv) >= 2:
        if sys.argv[1].lower() == "monitor":
            monitor_tidb()
        else:
            try:
                row_json_str = sys.argv[1]
                data = json.loads(row_json_str)
                send_to_feishu(data)
            except Exception as e:
                print(f"❌ 解析输入 JSON 失败: {e}")
                sys.exit(1)
    else:
        # 示例测试数据（仅无参数时使用）
        test_data = {
            "id": 186,
            "work_id": "315bd20e7e7690e27f2859689ac4ba04",
            "work_url": "www.baidu.com",
            "work_title": "提醒各位北方的电车小伙伴要注意冬季电池规划新能源冬天真是消耗大..." * 5,
            "work_content": "提醒各位北方的电车小伙伴要注意冬季电池规划新能源..." * 5,
            "publish_time": datetime.datetime.now(),
            "crawled_time": datetime.datetime.now(),
            "account_name": base64.b64encode(
                base64.b64encode("测试账号".encode("utf-8"))
            ).decode("utf-8"),
            "source": "微博",
            "like_cnt": 99,
            "reply_cnt": 12,
            "forward_cnt": 5,
            "content_senti": 0,
            "ocr_content": "OCR识别的长文本数据..." * 10
        }
        print("未检测到输入参数，使用示例数据进行测试推送...")
        send_to_feishu(test_data)