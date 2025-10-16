# -*- coding: utf-8 -*-
# feishu_notify.py
import requests
import json
import datetime
import base64
import sys

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/c74b9141-1759-40e2-ae3a-50cc6389e1bc"
FEISHU_OPEN_ID = "ou_20b2bd16a8405b93019b7291ec5202c3"

# 字段映射
FIELD_MAP = {
    "work_id":       "主贴ID",
    "work_url":      "主贴链接",
    "work_title":    "主贴标题",
    "work_content":  "正文内容",
    "publish_time":  "发布时间",
    "crawled_time":  "抓取时间",
    "account_name":  "作者名称",
    "source":        "来源平台",
    "like_cnt":      "点赞数",
    "reply_cnt":     "评论数",
    "forward_cnt":   "转发数",
    "content_senti": "内容情感",
    "ocr_content":   "OCR识别内容"
}

# 仅按该顺序展示字段
ORDERED_FIELDS = [
    "source", "work_url", "publish_time", "account_name",
    "work_title", "work_content", "ocr_content",
    "like_cnt", "reply_cnt", "forward_cnt"
]

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

    for k in ORDERED_FIELDS:
        if k not in data:
            continue
        v = data.get(k)

        # 时间格式化
        if isinstance(v, datetime.datetime):
            v = v.strftime("%Y-%m-%d %H:%M:%S")
        if v is None:
            v = ""

        # 特定字段处理
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

    # 末尾 @ 指定人
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

if __name__ == "__main__":
    # 运行逻辑：
    # - 如果通过命令行传入 JSON 字符串（tidb 监控脚本调用），则推送新记录
    # - 如果未传入参数，作为独立脚本运行，使用示例数据进行测试
    if len(sys.argv) >= 2:
        try:
            row_json_str = sys.argv[1]
            data = json.loads(row_json_str)
            send_to_feishu(data)
        except Exception as e:
            print(f"❌ 解析输入 JSON 失败: {e}")
            sys.exit(1)
    else:
        # 示例测试数据（仅独立运行时使用，可自行删除）
        test_data = {
            "id": 186,
            "work_id": "315bd20e7e7690e27f2859689ac4ba04",
            "work_url": "www.baidu.com",
            "work_title": "提醒各位北方的电车小伙伴要注意冬季电池规划新能源冬天真是消耗大..." * 2,
            "work_content": "提醒各位北方的电车小伙伴要注意冬季电池规划新能源..." * 2,
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
            "ocr_content": "OCR识别的长文本数据..." * 2
        }
        print("未检测到输入参数，使用示例数据进行测试推送...")
        send_to_feishu(test_data)