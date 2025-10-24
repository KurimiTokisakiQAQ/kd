# -*- coding: utf-8 -*-
# notify_llm.py
import requests
import json
import datetime
import base64
import sys
import re
import pymysql
import difflib
import hashlib

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/c74b9141-1759-40e2-ae3a-50cc6389e1bc"
FEISHU_OPEN_ID = "ou_20b2bd16a8405b93019b7291ec5202c3"

API_URL = "https://llm-cmt-api.dev.fc.chj.cloud/agentops/chat/completions"
HEADERS = {"Content-Type": "application/json"}

DB_CONFIG = {
    "host": "da-dw-tidb-10900.chj.cloud",
    "port": 3306,
    "user": "da_algo_craw_wr",
    "password": "99FBD18120C777560A9451FB65A8E74F60CFBBD3",
    "database": "da_crawler_dw",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

NOTIFY_TABLE = "dwd_idc_life_ent_soc_public_sentiment_battery_work_notify_mix_rt"

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
ORDERED_FIELDS = ["source", "work_url", "publish_time", "account_name", "summary"]
ADVICE_BY_SEVERITY = {"低": "请相关人员了解", "中": "请相关人员关注", "高": "请相关人员重点关注"}

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

def to_datetime(v):
    if isinstance(v, datetime.datetime):
        return v
    if isinstance(v, (int, float)):
        try:
            return datetime.datetime.fromtimestamp(v)
        except Exception:
            pass
    if isinstance(v, str):
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.datetime.strptime(v.strip(), fmt)
            except Exception:
                continue
        try:
            return datetime.datetime.fromisoformat(v.strip())
        except Exception:
            pass
    return datetime.datetime.now()

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
        f"\n标题：{title}\n正文：{content}\nOCR：{ocr}\n只返回上述JSON。"
    )

def parse_evaluation_json(text: str):
    raw = text.strip()
    json_str = _extract_json_from_text(raw)
    focus = "否"; problem = "否"; summary = None; severity = "中"
    try:
        obj = json.loads(json_str)
        if isinstance(obj, dict):
            fv = obj.get("focus"); pv = obj.get("problem"); sv = obj.get("summary"); sev = obj.get("severity")
            if isinstance(fv, str) and fv.strip() in ("是","否"): focus = fv.strip()
            if isinstance(pv, str) and pv.strip() in ("是","否"): problem = pv.strip()
            if isinstance(sv, str): summary = sv.strip() or None
            if isinstance(sev, str) and sev.strip() in ("低","中","高"): severity = sev.strip()
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
        return "否", "否", f"[评估失败] {e}", "中"
    return parse_evaluation_json(llm_text)

# ============== 相似聚类（优先主贴标题+正文，否则 OCR） ==============
def clean_text(s: str) -> str:
    if not s:
        return ""
    s = str(s).lower()
    s = re.sub(r"http[s]?://\S+", " ", s)
    s = re.sub(r"www\.\S+", " ", s)
    s = re.sub(r"@\S+", " ", s)
    s = re.sub(r"#\S+#", " ", s)
    s = re.sub(r"[^\w\u4e00-\u9fff]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

REPOST_HINTS = ["转发","转载","转帖","repost","分享","转一下","via","原文见","链接","link"]

def is_primary_informative(title: str, content: str) -> bool:
    t = clean_text(title); c = clean_text(content); combo = f"{t} {c}".strip()
    if len(combo) < 20: return False
    if any(h in (title or "").lower() or h in (content or "").lower() for h in REPOST_HINTS):
        if len(combo) < 40: return False
    if not re.search(r"[\u4e00-\u9fff\w]{10,}", combo): return False
    return True

def choose_text_for_similarity(title: str, content: str, ocr: str) -> tuple:
    if is_primary_informative(title, content):
        return clean_text(title) + " " + clean_text(content), "primary"
    ocr_clean = clean_text(ocr)
    if len(ocr_clean) >= 10:
        return ocr_clean, "ocr"
    return "", "none"

def text_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()

def _stable_hash_id(title: str, content: str, ocr: str) -> str:
    base = (clean_text(title) + "|" + clean_text(content) + "|" + clean_text(ocr)).strip()
    if not base:
        base = str(datetime.datetime.now().timestamp())
    return hashlib.md5(base.encode("utf-8")).hexdigest()[:16]

def find_similar_id(conn, data: dict, lookback_days: int = 30, threshold: float = 0.72) -> str:
    new_text, _ = choose_text_for_similarity(data.get("work_title"), data.get("work_content"), data.get("ocr_content"))
    pub_dt = to_datetime(data.get("publish_time"))
    start_dt = pub_dt - datetime.timedelta(days=lookback_days)

    best_sim = 0.0
    best_similar_id = None

    try:
        with conn.cursor() as cursor:
            sql = f"""
                SELECT id, work_id, similar_id, work_title, work_content, ocr_content
                FROM {NOTIFY_TABLE}
                WHERE publish_time >= %s
            """
            cursor.execute(sql, (start_dt,))
            rows = cursor.fetchall() or []
            for r in rows:
                cand_text, _ = choose_text_for_similarity(r.get("work_title"), r.get("work_content"), r.get("ocr_content"))
                sim = text_similarity(new_text, cand_text)
                if sim > best_sim:
                    best_sim = sim
                    # 候选 similar_id 为空时，回退用候选的 work_id 或 id
                    cand_sim_id = r.get("similar_id") or r.get("work_id") or r.get("id")
                    best_similar_id = str(cand_sim_id) if cand_sim_id is not None else None
    except Exception as e:
        print(f"⚠️ 查找相似类簇异常：{e}")

    if best_sim >= threshold and best_similar_id:
        return best_similar_id

    # 强兜底：优先当前 work_id -> 当前 id -> 内容哈希
    wid = data.get("work_id"); rid = data.get("id")
    if wid: return str(wid)
    if rid: return str(rid)
    return _stable_hash_id(data.get("work_title"), data.get("work_content"), data.get("ocr_content"))

# ============== 统计相似数量（七日/单日） ==============
def compute_similar_counts(conn, similar_id: str, publish_time: datetime.datetime):
    pub_dt = to_datetime(publish_time)
    day_str = pub_dt.strftime("%Y-%m-%d")
    start_7_date = (pub_dt.date() - datetime.timedelta(days=6)).strftime("%Y-%m-%d")
    end_7_date = pub_dt.strftime("%Y-%m-%d")
    day_cnt = 0; seven_cnt = 0
    try:
        with conn.cursor() as cursor:
            sql_day = f"SELECT COUNT(*) AS cnt FROM {NOTIFY_TABLE} WHERE similar_id = %s AND DATE(publish_time) = %s"
            cursor.execute(sql_day, (similar_id, day_str))
            day_cnt = (cursor.fetchone() or {}).get("cnt", 0) or 0
            sql_7 = f"SELECT COUNT(*) AS cnt FROM {NOTIFY_TABLE} WHERE similar_id = %s AND DATE(publish_time) BETWEEN %s AND %s"
            cursor.execute(sql_7, (similar_id, start_7_date, end_7_date))
            seven_cnt = (cursor.fetchone() or {}).get("cnt", 0) or 0
    except Exception as e:
        print(f"⚠️ 统计相似主贴数量异常: {e}")
    return int(seven_cnt), int(day_cnt)

# ============== 落库（含 id 与 similar_id） ==============
def _safe_int(v, default=None):
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default

def upsert_notify_and_counts(data: dict, summary_text: str, severity: str):
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        conn.autocommit(True)

        similar_id = find_similar_id(conn, data)

        with conn.cursor() as cursor:
            sql = f"""
            INSERT INTO {NOTIFY_TABLE} (
                id,                 -- 源表自增ID，直接沿用
                work_id, work_url, work_title, work_content,
                publish_time, crawled_time, account_name, source,
                like_cnt, reply_cnt, forward_cnt, content_senti,
                ocr_content, summary, event_level, similar_id
            ) VALUES (
                %(id)s,
                %(work_id)s, %(work_url)s, %(work_title)s, %(work_content)s,
                %(publish_time)s, %(crawled_time)s, %(account_name)s, %(source)s,
                %(like_cnt)s, %(reply_cnt)s, %(forward_cnt)s, %(content_senti)s,
                %(ocr_content)s, %(summary)s, %(event_level)s, %(similar_id)s
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
                event_level=VALUES(event_level),
                similar_id=VALUES(similar_id)
            """
            params = {
                "id": data.get("id"),  # 关键：沿用源表 id
                "work_id": data.get("work_id"),
                "work_url": data.get("work_url"),
                "work_title": data.get("work_title"),
                "work_content": data.get("work_content"),
                "publish_time": to_datetime(data.get("publish_time")),
                "crawled_time": to_datetime(data.get("crawled_time")),
                "account_name": data.get("account_name"),
                "source": data.get("source"),
                "like_cnt": _safe_int(data.get("like_cnt"), default=-99),
                "reply_cnt": _safe_int(data.get("reply_cnt"), default=-99),
                "forward_cnt": _safe_int(data.get("forward_cnt"), default=-99),
                "content_senti": _safe_int(data.get("content_senti")),
                "ocr_content": data.get("ocr_content"),
                "summary": summary_text,
                "event_level": severity,
                "similar_id": similar_id  # 保证非空
            }
            cursor.execute(sql, params)
            print("✅ 通知数据已落库到 TiDB 通知表（含 id 与 similar_id）")

        seven_cnt, day_cnt = compute_similar_counts(conn, similar_id, params["publish_time"])
        return True, similar_id, seven_cnt, day_cnt
    except Exception as e:
        print(f"❌ 通知数据落库或统计失败: {e}")
        return False, None, 0, 0
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

# ============== 推送 ==============
def send_to_feishu(data: dict):
    focus, problem, summary_text, severity = evaluate_post(data)
    if focus != "是":
        print("跳过：主体未严格聚焦理想汽车的电池或增程器问题。")
        return False
    if problem != "是":
        print("跳过：未明确指出理想电池或增程器存在问题（对比/评测未明确指出问题不推送）。")
        return False

    ok_db, similar_id, seven_cnt, day_cnt = upsert_notify_and_counts(data, summary_text, severity)
    if not ok_db:
        print("❌ 由于落库/统计失败，本次不进行飞书推送。")
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

    post_content.append([{"tag": "text", "text": f"【七日内相似主贴数量】: {seven_cnt}"}])
    post_content.append([{"tag": "text", "text": f"【单日内相似主贴数量】: {day_cnt}"}])
    post_content.append([
        {"tag": "at", "user_id": FEISHU_OPEN_ID},
        {"tag": "text", "text": f" {advice}（烈度：{severity}）"}
    ])

    payload = {"msg_type": "post", "content": {"post": {"zh_cn": {"title": "📢 负面舆情告警（理想电池/增程器）", "content": post_content}}}}
    ok = False
    try:
        resp = requests.post(WEBHOOK_URL, headers={"Content-Type": "application/json; charset=utf-8"}, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"))
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
    return ok

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        try:
            row_json_str = sys.argv[1]
            data = json.loads(row_json_str)
            send_to_feishu(data)
        except Exception as e:
            print(f"❌ 解析输入 JSON 失败: {e}")
            sys.exit(1)
    else:
        test_data = {
            "id": 186,  # 源表自增ID
            "work_id": "315bd20e7e7690e27f2859689ac4ba04",
            "work_url": "www.baidu.com",
            "work_title": "理想L9电池低温充电失败并多次报错，用户投诉",
            "work_content": "车主称理想L9在寒潮下无法充电且频繁BMS报错，续航大幅下降，存在安全隐患，已向厂家投诉。",
            "publish_time": datetime.datetime.now(),
            "crawled_time": datetime.datetime.now(),
            "account_name": base64.b64encode(base64.b64encode("测试账号".encode("utf-8"))).decode("utf-8"),
            "source": "微博",
            "like_cnt": 99,
            "reply_cnt": 12,
            "forward_cnt": 5,
            "content_senti": -1,
            "ocr_content": "理想汽车L9低温无法充电频繁报错，疑似BMS故障，用户维权"
        }
        print("未检测到输入参数，使用示例数据进行测试推送并落库...")
        send_to_feishu(test_data)