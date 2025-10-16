# -*- coding: utf-8 -*-
import requests
import json
import datetime
import base64

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

    # 输出字典中的字段（保持传入数据的顺序）
    for k, v in data.items():
        if k == "id":
            continue

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

        # 字段名映射
        label = FIELD_MAP.get(k, k)

        # 每个字段单独成一行
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
    # 测试数据
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
    send_to_feishu(test_data)