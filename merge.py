# -*- coding: utf-8 -*-
# monitor_notify.py
import requests
import json
import datetime
import base64
import re
import pymysql
import time

# ================= 基础配置 =================
TABLE_NAME = "dwd_idc_life_ent_soc_public_sentiment_battery_work_mix_rt"

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/c74b9141-1759-40e2-ae3a-50cc6389e1bc"
FEISHU_OPEN_ID = "ou_20b2bd16a8405b93019b7291ec5202c3"

# 大模型调用配置（已鉴权）
API_URL = "https://llm-cmt-api.dev.fc.chj.cloud/agentops/chat/completions"
HEADERS = {"Content-Type": "application/json"}  # 如需鉴权，补充 Authorization

# TiDB 连接信息（与监控表同库）
DB_CONFIG = {
    "host": "da-dw-tidb-10900.chj.cloud",
    "port": 3306,
    "user": "da_algo_craw_wr",
    "password": "99FBD18120C777560A9451FB65A8E74F60CFBBD3",
    "database": "da_crawler_dw",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

# 通知落库的目标表
NOTIFY_TABLE = "dwd_idc_life_ent_soc_public_sentiment_battery_work_notify_mix_rt"

# 字段映射与展示顺序
FIELD_MAP = {
    "summary":      "文章摘要",
    "work_id":      "主贴ID",
    "work_url":     "主贴链接",
    "work_title":   "主贴标题",
    "work_content": "正文内容",
    "publish_time": "发布时间",
    "crawled_time": "抓取时间",
    "account_name": "作者名称",
    "source":       "来源平台",
    "like_cnt":     "点赞数",
    "reply_cnt":    "评论数",
    "forward_cnt":  "转发数",
    "content_senti":"内容情感",
    "ocr_content":  "OCR识别内容"
}
ORDERED_FIELDS = [
    "source", "work_url", "publish_time", "account_name",
    "summary",
    # 如需展示更多字段，打开注释
    # "work_title", "work_content",
    # "like_cnt", "reply_cnt", "forward_cnt"
]
ADVICE_BY_SEVERITY = {"低": "请相关人员了解", "中": "请相关人员关注", "高": "请相关人员重点关注"}


# ================= 公共工具 =================
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

def call_chat_completion_stream(prompt: str, model: str = "azure-gpt-4o") -> str:
    payload = {"model": model, "messages": [{"role": "user", "content": prompt}], "stream": True}
    result_chunks = []
    with requests.post(API_URL, headers=HEADERS, data=json.dumps(payload), stream=True, timeout=300) as resp:
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
        for raw_line in resp.iter_lines(chunk_size=1024, decode_unicode=False):
            if not raw_line:
                continue
            line = raw_line.strip()
            if not line.startswith(b"data:"):
                continue
            data_bytes = line[len(b"data:"):].strip()
            if data_bytes == b"[DONE]":
                break
            try:
                obj = json.loads(data_bytes.decode("utf-8"))
            except Exception:
                result_chunks.append(data_bytes.decode("utf-8", errors="ignore"))
                continue
            choices = obj.get("choices") or []
            for ch in choices:
                delta = ch.get("delta") or {}
                chunk = delta.get("content") or ch.get("content") or (ch.get("message") or {}).get("content")
                if chunk:
                    result_chunks.append(chunk)
    return "".join(result_chunks).strip()

def _extract_json_from_text(text: str) -> str:
    s = text.strip()
    fence = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", s, flags=re.IGNORECASE)
    if fence:
        return fence.group(1).strip()
    start = s.find("{")
    if start != -1:
        depth = 0
        for i in range(start, len(s)):
            c = s[i]
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return s[start:i+1].strip()
    return s


# ================= 统一评估（严格版） =================
def build_evaluation_prompt(title: str, content: str, ocr: str) -> str:
    title = title or ""
    content = content or ""
    ocr = ocr or ""
    return (
        "请严格评估以下帖子，并只返回纯JSON："
        '{"focus":"是|否","problem":"是|否","summary":"约50字中文摘要","severity":"低|中|高"}。'
        "判定规则："
        "focus=是：帖子的主体必须严格围绕理想汽车（Li Auto/理想ONE/L6/L7/L8/L9/i6/i8/Mega）的电池或增程器的问题。"
        "problem=是：明确指出理想电池或增程器存在不足/缺陷/风险/故障/事故/投诉/维权/召回等问题；"
        "若为品牌对比/评测/体验分享/一般建议/科普等，且未明确指出理想电池或增程器有问题，则problem=否。"
        f"\n标题：{title}\n正文：{content}\nOCR：{ocr}\n"
        "只返回上述JSON。"
    )

def parse_evaluation_json(text: str):
    raw = text.strip()
    json_str = _extract_json_from_text(raw)
    focus = "否"
    problem = "否"
    summary = None
    severity = "中"
    try:
        obj = json.loads(json_str)
        if isinstance(obj, dict):
            fv = obj.get("focus"); pv = obj.get("problem")
            sv = obj.get("summary"); sev = obj.get("severity")
            if isinstance(fv, str):
                fvs = fv.strip()
                if fvs in ("是", "否"):
                    focus = fvs
            if isinstance(pv, str):
                pvs = pv.strip()
                if pvs in ("是", "否"):
                    problem = pvs
            if isinstance(sv, str):
                summary = sv.strip() or None
            if isinstance(sev, str) and sev.strip() in ("低", "中", "高"):
                severity = sev.strip()
    except Exception:
        pass
    if summary is None:
        summary = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.IGNORECASE).strip()
    return focus, problem, summary, severity

def evaluate_post(data: dict):
    title = data.get("work_title") or ""
    content = data.get("work_content") or ""
    ocr = data.get("ocr_content") or ""
    t = title if isinstance(title, str) else json.dumps(title, ensure_ascii=False)
    c = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
    o = ocr if isinstance(ocr, str) else json.dumps(ocr, ensure_ascii=False)
    prompt = build_evaluation_prompt(t, c, o)
    try:
        llm_text = call_chat_completion_stream(prompt, model="azure-gpt-4o")
    except Exception as e:
        # 模型不可用时，为避免误推，直接判定不推送
        return "否", "否", f"[评估失败] {e}", "中"
    return parse_evaluation_json(llm_text)


# ================= 通知落库 =================
def _safe_int(v, default=None):
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default

def save_notify_record_to_tidb(data: dict, summary_text: str, severity: str):
    try:
        conn = pymysql.connect(**DB_CONFIG)
        conn.autocommit(True)
        with conn.cursor() as cursor:
            sql = f"""
            INSERT INTO {NOTIFY_TABLE} (
                work_id, work_url, work_title, work_content,
                publish_time, crawled_time, account_name, source,
                like_cnt, reply_cnt, forward_cnt, content_senti,
                ocr_content, summary, event_level
            ) VALUES (
                %(work_id)s, %(work_url)s, %(work_title)s, %(work_content)s,
                %(publish_time)s, %(crawled_time)s, %(account_name)s, %(source)s,
                %(like_cnt)s, %(reply_cnt)s, %(forward_cnt)s, %(content_senti)s,
                %(ocr_content)s, %(summary)s, %(event_level)s
            )
            ON DUPLICATE KEY UPDATE
                work_url=VALUES(work_url),
                work_title=VALUES(work_title),
                work_content=VALUES(work_content),
                publish_time=VALUES(publish_time),
                crawled_time=VALUES(crawled_time),
                account_name=VALUES(account_name),
                source=VALUES(source),
                like_cnt=VALUES(like_cnt),
                reply_cnt=VALUES(reply_cnt),
                forward_cnt=VALUES(forward_cnt),
                content_senti=VALUES(content_senti),
                ocr_content=VALUES(ocr_content),
                summary=VALUES(summary),
                event_level=VALUES(event_level)
            """
            params = {
                "work_id": data.get("work_id"),
                "work_url": data.get("work_url"),
                "work_title": data.get("work_title"),
                "work_content": data.get("work_content"),
                "publish_time": data.get("publish_time"),
                "crawled_time": data.get("crawled_time"),
                "account_name": data.get("account_name"),
                "source": data.get("source"),
                "like_cnt": _safe_int(data.get("like_cnt"), default=-99),
                "reply_cnt": _safe_int(data.get("reply_cnt"), default=-99),
                "forward_cnt": _safe_int(data.get("forward_cnt"), default=-99),
                "content_senti": _safe_int(data.get("content_senti")),
                "ocr_content": data.get("ocr_content"),
                "summary": summary_text,
                "event_level": severity
            }
            cursor.execute(sql, params)
            print("✅ 通知数据已落库到 TiDB 通知表")
    except Exception as e:
        print(f"❌ 通知数据落库失败: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ================= 飞书推送流程 =================
def send_to_feishu(data: dict):
    # 完全依赖大模型评估：主体严格围绕理想电池/增程器问题 + 明确指出问题
    focus, problem, summary_text, severity = evaluate_post(data)
    if focus != "是":
        print("跳过推送与落库：主体未严格聚焦理想汽车的电池或增程器问题。")
        return False
    if problem != "是":
        print("跳过推送与落库：未明确指出理想电池或增程器存在不足/缺陷/风险/故障等问题（对比/评测未明确指出问题不推送）。")
        return False

    advice = ADVICE_BY_SEVERITY.get(severity, ADVICE_BY_SEVERITY["中"])

    post_content = []
    for k in ORDERED_FIELDS:
        v = summary_text if k == "summary" else data.get(k)
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
        post_content.append([{"tag": "text", "text": f"【{label}】: {v}"}])

    post_content.append([
        {"tag": "at", "user_id": FEISHU_OPEN_ID},
        {"tag": "text", "text": f" {advice}（烈度：{severity}）"}
    ])

    payload = {
        "msg_type": "post",
        "content": {
            "post": {"zh_cn": {"title": "📢 负面舆情告警（理想电池/增程器）", "content": post_content}}
        }
    }

    ok = False
    try:
        resp = requests.post(
            WEBHOOK_URL,
            headers={"Content-Type": "application/json; charset=utf-8"},
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8")
        )
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

    if ok:
        try:
            save_notify_record_to_tidb(data, summary_text, severity)
        except Exception as e:
            print(f"❌ 推送后落库异常: {e}")

    return ok


# ================= 持续监控 TiDB 并处理 =================
def monitor_tidb_and_notify(poll_interval_sec=60):
    conn = None
    cursor = None
    last_id = 0
    print(f"开始监测 TiDB 表 {TABLE_NAME}，Ctrl+C 可退出...")

    try:
        while True:
            try:
                if conn is None:
                    conn = pymysql.connect(**DB_CONFIG)
                    cursor = conn.cursor()

                sql = f"SELECT * FROM {TABLE_NAME} WHERE id > %s ORDER BY id ASC"
                cursor.execute(sql, (last_id,))
                rows = cursor.fetchall()

                if rows:
                    for row in rows:
                        print(f"🔍 检测到新数据，work_id: {row.get('work_id', '')}")
                        try:
                            send_to_feishu(row)
                        except Exception as e:
                            print(f"❌ 处理并通知失败: {e}")

                        rid = row.get('id', 0)
                        if rid > last_id:
                            last_id = rid

                time.sleep(poll_interval_sec)

            except pymysql.err.OperationalError as e:
                print(f"❌ 数据库连接异常，尝试重连: {e}")
                try:
                    if cursor: cursor.close()
                    if conn: conn.close()
                except Exception:
                    pass
                conn, cursor = None, None
                time.sleep(5)

    except KeyboardInterrupt:
        print("监测程序已退出")
    finally:
        try:
            if cursor: cursor.close()
            if conn: conn.close()
        except Exception:
            pass


# ================= 运行入口 =================
if __name__ == "__main__":
    monitor_tidb_and_notify(poll_interval_sec=60)