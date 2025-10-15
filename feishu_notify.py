# feishu_notify.py
import requests
import json
import sys

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/c74b9141-1759-40e2-ae3a-50cc6389e1bc"

def send_to_feishu(data: dict):
    """
    把数据字典格式化成 key: value 换行的形式，然后发送到飞书机器人
    """
    message_lines = []
    for k, v in data.items():
        # 确保 value 是字符串
        if v is None:
            v = ""
        message_lines.append(f"{k}: {v}")
    message_text = "\n".join(message_lines)

    # 飞书机器人消息格式（文本类型）
    payload = {
        "msg_type": "text",
        "content": {
            "text": message_text
        }
    }

    try:
        resp = requests.post(WEBHOOK_URL, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
        if resp.status_code == 200:
            print(f"✅ 飞书消息已发送: {message_text}")
        else:
            print(f"❌ 飞书消息发送失败: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"❌ 调用飞书接口异常: {e}")

if __name__ == "__main__":
    # 如果支持命令行参数直接传 JSON（为了兼容监控程序调用）
    if len(sys.argv) > 1:
        try:
            json_str = sys.argv[1]
            row_data = json.loads(json_str)
            send_to_feishu(row_data)
        except Exception as e:
            print(f"❌ feishu_notify 脚本解析 JSON 失败: {e}")