# -*- coding: utf-8 -*-
# notify_llm.py
import requests
import json
import datetime
import base64
import sys
import re
import pymysql
import hashlib

# 飞书机器人 Webhook 与 @人列表
WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/c74b9141-1759-40e2-ae3a-50cc6389e1bc"
FEISHU_AT_OPEN_IDS = [
    "ou_20b2bd16a8405b93019b7291ec5202c3"
]

# 大模型接口
API_URL = "https://llm-cmt-api.dev.fc.chj.cloud/agentops/chat/completions"
HEADERS = {"Content-Type": "application/json"}  # 如需鉴权，在此补充 Authorization

# TiDB 连接信息
DB_CONFIG = {
    "host": "da-dw-tidb-10900.chj.cloud",
    "port": 3306,
    "user": "da_algo_craw_wr",
    "password": "99FBD18120C777560A9451FB65A8E74F60CFBBD3",
    "database": "da_crawler_dw",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

# 通知表
NOTIFY_TABLE = "dwd_idc_life_ent_soc_public_sentiment_battery_work_notify_mix_rt"

# 展示字段
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

# 候选摘要数量上限，避免提示词过长
MAX_SUMMARY_CANDIDATES = 200

# ===================== 公共工具 =====================
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

def _safe_int(v, default=None):
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default

def safe_bigint(v):
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            return int(v)
        if isinstance(v, str):
            s = v.strip()
            return int(s) if re.fullmatch(r"[+-]?\d+", s) else None
    except Exception:
        return None
    return None

# ===================== 大模型通用调用与解析 =====================
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

# ===================== 推送准入：统一评估 =====================
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

# ===================== 基于摘要的 LLM 聚合 =====================
def get_summary_candidates(conn, max_candidates: int = MAX_SUMMARY_CANDIDATES):
    """
    从通知表取已有记录的摘要作为候选（按时间倒序），限制数量避免提示词过长。
    返回列表元素：{"similar_id": str, "summary": str}
    """
    rows = []
    try:
        with conn.cursor() as cursor:
            sql = f"""
                SELECT similar_id, summary, publish_time
                FROM {NOTIFY_TABLE}
                WHERE summary IS NOT NULL AND summary <> ''
                ORDER BY publish_time DESC
                LIMIT %s
            """
            cursor.execute(sql, (max_candidates,))
            fetched = cursor.fetchall() or []
            for r in fetched:
                sid = r.get("similar_id")
                summ = r.get("summary")
                if not sid or not summ:
                    continue
                rows.append({"similar_id": str(sid), "summary": str(summ)})
    except Exception as e:
        print(f"⚠️ 提取摘要候选异常: {e}")
    return rows

def _norm_summary(s: str) -> str:
    if not s:
        return ""
    s = str(s).strip()
    # 统一空白与去表情/特殊符号的简单清洗
    s = re.sub(r"[\uFE0F\u200B\u200C\u200D]", "", s)  # 去掉变体选择器/零宽字符
    s = re.sub(r"\s+", " ", s)
    return s[:800]  # 控制单条摘要长度

def build_summary_similarity_prompt(new_summary: str, candidates: list) -> str:
    """
    基于“事件级”粒度的摘要相似判定提示词（含数值指标优先规则）：
    - 优先按具体事件聚类，避免仅因“宽泛主题”（如“电池衰退反馈”）而误聚类
    - 若出现相同且具辨识度的关键数值指标（如“电池衰退率88.3%”“检测指数741”“召回编号XXXX”），
      在上下文一致且无明显时间/主体冲突时，可直接判为同一事件，即便不足三项高度一致
    """
    cand_lines = []
    for i, c in enumerate(candidates, 1):
        cand_lines.append(f"[{i}] similar_id={c['similar_id']}\n摘要：{_norm_summary(c['summary'])}")
    cand_block = "\n\n".join(cand_lines) if cand_lines else "无候选摘要"

    return (
        "你是一个中文舆情“事件级”聚类判定器。目标：仅在“同一具体事件”时聚为一类；"
        "若只是“话题/主题相似”（如泛化的“电池衰退反馈”），则不要聚为一类。\n\n"
        "事件级一致的判定标准（满足越多越可信）：\n"
        "1) 时间要素：明确日期（至少到月日）一致，或可推断为同一时间窗口（容差不超过±3天）；\n"
        "2) 车型/年款/版本：具体车型（如 理想L6/L7/L8/L9/Mega 等）一致；\n"
        "3) 部件/故障点/现象：如“电池包鼓包”“BMS报错”“增程器异常”“下摆臂异响”等一致；\n"
        "4) 机构/主体/地点：检测机构/门店/维权组织/政府部门/媒体名/具体城市或门店一致；\n"
        "5) 证据或来源：如“官方通告/检测报告/召回编号/工单号/媒体报道链接”等一致；\n"
        "6) 数量/指标：同一报告或同一事故下的关键数值（受影响车辆数、指数值、比率等）一致。\n"
        "7) 数值指标优先规则（可单独触发聚类）：如果两个摘要出现相同且具辨识度的关键数值指标，"
        "且指标所指向的上下文一致（例如同属电池质量监测/同一检测项目），且无明显时间或主体冲突，"
        "则可以直接判定为同一事件，即使未达到“至少三项高度一致”。\n"
        "  - 具辨识度的指标示例：电池衰退率88.3%、检测指数741、召回编号/工单号、伤亡/受影响车辆精确数量等；\n"
        "  - 不具辨识度或易误判的数字不应单独触发：年份（如2025年）、“Top10”这类榜单序号、常见整百整数（如100%）等；\n"
        "  - 若出现相同数字但上下文指代不同（如不同车型/不同项目），不可据此直接聚类。\n\n"
        "决策准则：\n"
        "- 若满足上述要素中“至少三项高度一致”，选择相应候选；\n"
        "- 否则，若命中“数值指标优先规则”，且上下文一致且无冲突，也可选择该候选；\n"
        "- 若多条候选均满足，选择要素重合度最高的那一条；\n"
        "- 否则返回 NEW。\n\n"
        "输出严格为纯JSON：{\"choose\":\"<similar_id|NEW|索引号>\",\"reason\":\"简述\"}，不要输出其它内容。\n\n"
        f"【新摘要】\n{_norm_summary(new_summary)}\n\n"
        f"【候选摘要列表】\n{cand_block}\n"
    )

def choose_cluster_by_summary_llm(conn, new_summary: str):
    """
    用 LLM 基于摘要在候选中选择同类簇的 similar_id；若返回 NEW 或失败则返回 None。
    """
    candidates = get_summary_candidates(conn, max_candidates=MAX_SUMMARY_CANDIDATES)
    if not candidates or not new_summary:
        return None

    prompt = build_summary_similarity_prompt(new_summary, candidates)
    try:
        llm_text = call_chat_completion_stream(prompt, model="azure-gpt-4o")
        js = _extract_json_from_text(llm_text)
        obj = json.loads(js)
        choose = str(obj.get("choose") or "").strip()
        reason = obj.get("reason")
        if choose.upper() == "NEW":
            print(f"[LLM聚类] 未命中候选（NEW）。理由：{reason}")
            return None
        # 如果返回的是索引号，映射到 similar_id
        if re.fullmatch(r"\d+", choose):
            idx = int(choose)
            candidates_len = len(candidates)
            if 1 <= idx <= candidates_len:
                sid = candidates[idx - 1]["similar_id"]
                print(f"[LLM聚类] 命中候选索引 {idx}/{candidates_len} -> similar_id={sid}。理由：{reason}")
                return sid
            return None
        # 否则认为直接返回了 similar_id
        for c in candidates:
            if choose == c["similar_id"]:
                print(f"[LLM聚类] 直接命中 similar_id={choose}。理由：{reason}")
                return choose
        # 返回了未知 ID，视为未命中
        print(f"[LLM聚类] 返回的 choose={choose} 未匹配任何候选 similar_id。理由：{reason}")
        return None
    except Exception as e:
        print(f"⚠️ LLM摘要聚类评估失败：{e}")
        return None

def _stable_hash_id(title: str, content: str, ocr: str) -> str:
    base = (str(title or "") + "|" + str(content or "") + "|" + str(ocr or "")).strip()
    if not base:
        base = str(datetime.datetime.now().timestamp())
    return hashlib.md5(base.encode("utf-8")).hexdigest()[:16]

def ensure_similar_id(data, computed_similar_id: str) -> str:
    """
    保证类似簇ID非空：
    - 优先 LLM 选中的 existing similar_id（对应首次出现的 work_id，不变）
    - 其次本条 work_id
    - 再次源表 id
    - 最后内容哈希兜底
    """
    if computed_similar_id:
        return str(computed_similar_id)
    wid = data.get("work_id")
    if wid:
        return str(wid)
    rid = safe_bigint(data.get("id"))
    if rid is not None:
        return str(rid)
    return _stable_hash_id(data.get("work_title"), data.get("work_content"), data.get("ocr_content"))

def find_similar_id(conn, data: dict, summary_text: str) -> str:
    """
    基于摘要的 LLM 聚合：用新记录的摘要与 notify 表内摘要比较，选择同类簇的 similar_id；
    未命中则按既定逻辑兜底。
    """
    chosen = choose_cluster_by_summary_llm(conn, summary_text)
    return ensure_similar_id(data, chosen)

# ===================== 统计相似数量（七日/单日，不含本条） =====================
def compute_similar_counts(conn, similar_id: str, publish_time: datetime.datetime, exclude_id=None, exclude_work_id=None):
    pub_dt = to_datetime(publish_time)
    day_str = pub_dt.strftime("%Y-%m-%d")
    start_7_date = (pub_dt.date() - datetime.timedelta(days=6)).strftime("%Y-%m-%d")
    end_7_date = pub_dt.strftime("%Y-%m-%d")

    base_conds = ["similar_id = %s"]
    base_params = [similar_id]
    if exclude_id is not None:
        base_conds.append("id <> %s")
        base_params.append(exclude_id)
    if exclude_work_id:
        base_conds.append("work_id <> %s")
        base_params.append(exclude_work_id)

    day_cnt = 0
    seven_cnt = 0
    try:
        with conn.cursor() as cursor:
            # 单日
            conds_day = base_conds + ["DATE(publish_time) = %s"]
            params_day = base_params + [day_str]
            sql_day = f"SELECT COUNT(*) AS cnt FROM {NOTIFY_TABLE} WHERE " + " AND ".join(conds_day)
            cursor.execute(sql_day, params_day)
            day_cnt = (cursor.fetchone() or {}).get("cnt", 0) or 0

            # 七日
            conds_7 = base_conds + ["DATE(publish_time) BETWEEN %s AND %s"]
            params_7 = base_params + [start_7_date, end_7_date]
            sql_7 = f"SELECT COUNT(*) AS cnt FROM {NOTIFY_TABLE} WHERE " + " AND ".join(conds_7)
            cursor.execute(sql_7, params_7)
            seven_cnt = (cursor.fetchone() or {}).get("cnt", 0) or 0
    except Exception as e:
        print(f"⚠️ 统计相似主贴数量异常: {e}")
    return int(seven_cnt), int(day_cnt)

# ===================== 落库（含 id 与 similar_id） =====================
def upsert_notify_and_counts(data: dict, summary_text: str, severity: str):
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        conn.autocommit(True)

        similar_id = find_similar_id(conn, data, summary_text)
        row_id = safe_bigint(data.get("id"))
        work_id = data.get("work_id")

        print(f"[DEBUG] parsed id={row_id} (type={type(row_id).__name__}), work_id={work_id}, similar_id={similar_id}")

        with conn.cursor() as cursor:
            sql = f"""
            INSERT INTO {NOTIFY_TABLE} (
                id,
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
                "id": row_id,
                "work_id": work_id,
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
                "similar_id": similar_id
            }
            cursor.execute(sql, params)
            print("✅ 通知数据已落库到 TiDB 通知表（含 id 与 similar_id）")

        # 相似数量统计（不计本条）
        seven_cnt, day_cnt = compute_similar_counts(
            conn,
            similar_id,
            params["publish_time"],
            exclude_id=row_id,
            exclude_work_id=work_id
        )
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

# ===================== 推送 =====================
def send_to_feishu(data: dict):
    # 先做统一评估（准入）
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

    # 相似主贴数量（不计入本条）
    post_content.append([{"tag": "text", "text": f"【相似主贴数量】{seven_cnt}条（7日）、{day_cnt}条（单日）"}])

    # 在同一行 @ 多人 + 建议（不展示“烈度：x”）
    mention_line = [{"tag": "at", "user_id": uid} for uid in FEISHU_AT_OPEN_IDS]
    mention_line.append({"tag": "text", "text": f" {advice}"})
    post_content.append(mention_line)

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

# ===================== 入口 =====================
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
            "work_url": "http://weibo.com/1633157160/Q9QMCwm18",
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