# feishu_notify.py
import requests
import json
import sys
import datetime

# 飞书 webhook 地址
WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/c74b9141-1759-40e2-ae3a-50cc6389e1bc"

def send_to_feishu(data: dict):
    """
    把数据字典格式化成 key: value 换行形式，并发送到飞书机器人
    """
    message_lines = []
    for k, v in data.items():
        # datetime 转成字符串
        if isinstance(v, datetime.datetime):
            v = v.strftime("%Y-%m-%d %H:%M:%S")
        # None 转成空字符串
        if v is None:
            v = ""
        message_lines.append(f"{k}: {v}")

    message_text = "\n".join(message_lines)

    payload = {
        "msg_type": "text",
        "content": {
            "text": message_text
        }
    }

    try:
        resp = requests.post(WEBHOOK_URL, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
        if resp.status_code == 200:
            print(f"✅ 飞书消息已发送:\n{message_text}")
        else:
            print(f"❌ 飞书消息发送失败: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"❌ 调用飞书接口异常: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            json_str = sys.argv[1]
            row_data = json.loads(json_str)
            send_to_feishu(row_data)
        except Exception as e:
            print(f"❌ 解析 JSON 失败: {e}")
    else:
        # 测试发送
        test_data = {
            "work_id": "w123456",
            "work_title": "电池测试消息",
            "source": "抖音",
            "content_senti": -1,
            "like_cnt": 99,
            "forward_cnt": 20,
            "crawled_time": datetime.datetime.now()
        }
        send_to_feishu(test_data)