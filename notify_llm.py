# -*- coding: utf-8 -*-
# notify_llm.py
import requests
import json
import datetime
import base64
import sys
import re
import pymysql

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/c74b9141-1759-40e2-ae3a-50cc6389e1bc"
FEISHU_OPEN_ID = "ou_20b2bd16a8405b93019b7291ec5202c3"

# 大模型调用配置（已鉴权）
API_URL = "https://llm-cmt-api.dev.fc.chj.cloud/agentops/chat/completions"
HEADERS = {
    "Content-Type": "application/json",
    # "Authorization": "Bearer <your-token>",
}

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

# 通知落库的目标表（新增：summary、event_level）
NOTIFY_TABLE = "dwd_idc_life_ent_soc_public_sentiment_battery_work_notify_mix_rt"

# 字段映射
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

# 展示顺序（在主贴标题前新增“文章摘要”）
ORDERED_FIELDS = [
    "source", "work_url", "publish_time", "account_name",
    "summary",
    "work_title", "work_content",
    "like_cnt", "reply_cnt", "forward_cnt"
]

# 根据烈度输出处理意见
ADVICE_BY_SEVERITY = {
    "低": "请相关人员了解",
    "中": "请相关人员关注",
    "高": "请相关人员重点关注",
}

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
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": True
    }

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
                chunk = delta.get("content")
                if chunk is None:
                    chunk = ch.get("content")
                if chunk is None:
                    msg = ch.get("message") or {}
                    chunk = msg.get("content")
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
                    candidate = s[start:i+1]
                    return candidate.strip()
    return s

# ================= 品牌与主题词检测 =================
def contains_li_brand(text: str) -> bool:
    """
    严格匹配理想汽车品牌或车型：
    理想汽车/Li Auto；理想ONE；理想L6/L7/L8/L9；理想i6/i8；理想Mega/MEGA
    """
    if not text:
        return False
    s = str(text)
    patterns = [
        r"理想汽车",
        r"\bli\s*auto\b",            # Li Auto
        r"理想\s*one", r"理想one",
        r"理想\s*l6", r"理想\s*l7", r"理想\s*l8", r"理想\s*l9",
        r"理想l6", r"理想l7", r"理想l8", r"理想l9",
        r"理想\s*i6", r"理想\s*i8",
        r"理想i6", r"理想i8",
        r"理想\s*mega", r"理想\s*MEGA",
        r"理想mega", r"理想MEGA",
    ]
    return any(re.search(pat, s, flags=re.IGNORECASE) for pat in patterns)

def contains_competitor_brand(text: str) -> bool:
    """
    友商品牌列表（可扩充）。只要出现任一即视为有友商品牌信号。
    """
    if not text:
        return False
    s = str(text).lower()
    competitors = [
        # 新势力/国内
        "特斯拉", "tesla",
        "比亚迪", "byd",
        "蔚来", "nio",
        "小鹏", "xpeng",
        "极氪", "zeekr",
        "问界", "aito", "华为", "huawei", "赛力斯", "seres",
        "智己", "im",
        "岚图", "voyah",
        "腾势", "denza",
        "深蓝", "changan",
        "哪吒", "hozon",
        "零跑", "leapmotor",
        "广汽埃安", "埃安", "aion", "gac",
        "吉利", "geely",
        # 传统国际品牌
        "宝马", "bmw",
        "奔驰", "mercedes", "benz",
        "奥迪", "audi",
        "大众", "vw", "volkswagen",
        "丰田", "toyota",
        "本田", "honda",
        "日产", "nissan",
        # 其它
        "极越", "jiue",
        "极狐", "arcfox",
        "长城", "great wall",
        "魏牌", "wey",
        "坦克", "tank",
        "小米汽车", "xiaomi", "su7",
    ]
    return any(k in s for k in competitors)

def contains_battery_topic(text: str) -> bool:
    if not text:
        return False
    s = str(text).lower()
    batt_keywords = [
        "电池", "续航", "充电", "慢充", "快充", "换电",
        "起火", "爆炸", "漏液", "鼓包", "内阻", "衰减",
        "低温", "高压", "低压", "bms", "电量", "soc", "soh",
        "容量", "能量回收", "充电桩", "充电口", "充电枪", "高压包", "三电"
    ]
    return any(k in s for k in batt_keywords)

def contains_range_extender_topic(text: str) -> bool:
    """
    增程器相关主题词：增程器/增程系统/增程/增程发动机/范围扩展器/RE（Range Extender）等
    """
    if not text:
        return False
    s = str(text).lower()
    re_keywords = [
        "增程器", "增程系统", "增程", "增程发动机", "范围扩展器",
        "range extender", "range-extender", "增程模式", "增程机",
        "发电机", "发动机增程", "erev", "增程版"
    ]
    return any(k in s for k in re_keywords)

# ================= 相关性判定（推送门禁） =================
def build_related_gate_prompt(title: str, content: str, ocr: str) -> str:
    """
    与理想汽车电池/增程器相关性的门禁判定提示词（更严格版）。
    仅返回纯 JSON：{"related":"是"} 或 {"related":"否"}。

    严格判定规则（必须同时满足）：
    1) 文本中明确出现理想汽车品牌或车型之一：
       理想汽车 / Li Auto / 理想ONE / 理想L6 / 理想L7 / 理想L8 / 理想L9 / 理想i6 / 理想i8 / 理想Mega/MEGA。
       注意：出现“理想”一词但非品牌语境（如“理想生活”“理想状态”）不算品牌指向。
    2) 话题聚焦电池或增程器相关议题（电池、续航、充电、慢充、快充、换电、安全、起火、爆炸、故障、低温、高压、BMS、SOC、SOH、容量、能量回收、增程器/增程系统/增程发动机等）。
    3) 若只出现友商品牌（如特斯拉、比亚迪、蔚来、小鹏、极氪、问界、智己、岚图、腾势、深蓝、哪吒、零跑、埃安、吉利、宝马、奔驰、奥迪、大众等）
       而没有出现理想品牌或车型，则判定为“不相关”。

    只返回纯 JSON，不要代码块或其他文字：
    {"related": "是"} 或 {"related": "否"}
    """
    title = title or ""
    content = content or ""
    ocr = ocr or ""
    return (
        "请严格判断以下文本是否与“理想汽车”的电池或增程器相关（避免把友商话题误判为理想）。"
        "必须满足：出现理想汽车品牌/车型（理想汽车/Li Auto/理想ONE/理想L6/L7/L8/L9/理想i6/i8/理想Mega），且话题集中在电池或增程器相关议题。"
        "若仅出现友商品牌（如特斯拉、比亚迪、蔚来、小鹏、极氪、问界、智己、岚图、腾势、深蓝、哪吒、零跑、埃安、吉利、宝马、奔驰、奥迪、大众等）而未出现理想品牌或车型，则判定为“不相关”。"
        "只返回纯 JSON，不要任何额外文字："
        '{"related": "是"} 或 {"related": "否"}'
        f"\n标题：{title}\n正文：{content}\nOCR：{ocr}\n"
        "只返回上述 JSON。"
    )

def parse_related_json(text: str) -> bool:
    raw = text.strip()
    json_str = _extract_json_from_text(raw)
    related = None
    try:
        obj = json.loads(json_str)
        if isinstance(obj, dict):
            val = str(obj.get("related", "")).strip()
            if val in ("是", "否"):
                related = (val == "是")
    except Exception:
        pass

    if related is None:
        # 回退规则：品牌 + （电池或增程器）主题同时出现
        has_li = contains_li_brand(raw)
        has_batt = contains_battery_topic(raw) or contains_range_extender_topic(raw)
        related = bool(has_li and has_batt)

    return related

def check_related(data: dict) -> bool:
    """
    使用大模型进行门禁判定：是否与理想汽车电池或增程器相关。
    更严格兜底：若文本出现友商品牌且未出现理想品牌/车型，则强制判定为不相关。
    同时要求必然出现电池或增程器相关话题。
    """
    title = data.get("work_title") or ""
    content = data.get("work_content") or ""
    ocr = data.get("ocr_content") or ""
    raw_text = f"{title}\n{content}\n{ocr}"

    # 快速兜底：出现友商且没有理想品牌/车型 => 不相关
    if contains_competitor_brand(raw_text) and not contains_li_brand(raw_text):
        return False

    # 避免非字符串类型导致提示词异常
    t = title if isinstance(title, str) else json.dumps(title, ensure_ascii=False)
    c = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
    o = ocr if isinstance(ocr, str) else json.dumps(ocr, ensure_ascii=False)

    prompt = build_related_gate_prompt(t, c, o)
    try:
        llm_text = call_chat_completion_stream(prompt, model="azure-gpt-4o")
        related_by_llm = parse_related_json(llm_text)
    except Exception:
        related_by_llm = None

    # 必须条件：理想品牌/车型 + （电池或增程器）话题
    has_li_brand = contains_li_brand(raw_text)
    has_topic = contains_battery_topic(raw_text) or contains_range_extender_topic(raw_text)
    has_competitor = contains_competitor_brand(raw_text)

    # 如果模型说“是”，但出现友商且没有理想品牌/车型，则纠正为否
    if related_by_llm is True and has_competitor and not has_li_brand:
        return False

    # 模型不可用时，用规则判定
    if related_by_llm is None:
        return bool(has_li_brand and has_topic)

    # 模型结果为 True 也要满足硬性条件
    return bool(related_by_llm and has_li_brand and has_topic)

# ================= 摘要与烈度（含主体聚焦判定） =================
def build_summary_prompt(title: str, content: str, ocr: str) -> str:
    """
    生成摘要、烈度，并额外判断“摘要的主体是否聚焦理想汽车的电池或增程器”。
    返回纯 JSON：
    {"summary": "...", "severity": "低|中|高", "focus": "是|否"}
    """
    title = title or ""
    content = content or ""
    ocr = ocr or ""
    return (
        "你是企业舆情分析助手。请阅读主贴标题、正文、OCR识别内容。"
        "请完成："
        "1) 用中文输出约50字的一段事件摘要（不包含判断语气）；"
        "2) 单独给出事件烈度，仅可为：低/中/高；"
        "3) 判断该摘要的主体是否聚焦“理想汽车（理想汽车/Li Auto/理想ONE/L6/L7/L8/L9/i6/i8/Mega）”的电池或增程器（增程系统/增程发动机），是或否。"
        "严格返回纯 JSON 文本，不要任何额外文字、不要代码块或反引号："
        '{"summary": "<事件摘要>", "severity": "<低|中|高>", "focus": "<是|否>"}'
        f"\n标题：{title}\n正文：{content}\nOCR：{ocr}\n"
        "只返回上述 JSON。"
    )

def parse_summary_json(text: str):
    """
    解析 {"summary": "...", "severity": "低|中|高", "focus": "是|否"}
    若 JSON 不规范，降级：从全文提取摘要；烈度默认中；focus 通过摘要和原文关键词规则估计。
    """
    raw = text.strip()
    json_str = _extract_json_from_text(raw)
    summary = None
    severity = None
    focus = None
    try:
        obj = json.loads(json_str)
        if isinstance(obj, dict):
            summary = str(obj.get("summary", "")).strip() or None
            severity = str(obj.get("severity", "")).strip() or None
            # 同时兼容不同键名
            focus_val = obj.get("focus") or obj.get("focus_li_battery_or_range_extender")
            if focus_val is not None:
                focus = str(focus_val).strip()
    except Exception:
        pass

    if summary is None:
        summary = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.IGNORECASE).strip()
    if severity not in ("低", "中", "高"):
        m = re.search(r"(低|中|高)", raw)
        severity = m.group(1) if m else "中"
    if focus not in ("是", "否"):
        # 通过摘要文本估计主体是否聚焦：理想品牌 + （电池或增程器）同时出现
        has_li = contains_li_brand(summary)
        has_topic = contains_battery_topic(summary) or contains_range_extender_topic(summary)
        focus = "是" if (has_li and has_topic) else "否"

    return summary, severity, focus

def generate_summary_and_severity(data: dict):
    """
    基于主贴标题、正文内容和 OCR 内容生成摘要与烈度，并返回主体聚焦判定（focus）。
    """
    title = data.get("work_title") or ""
    content = data.get("work_content") or ""
    ocr = data.get("ocr_content") or ""

    t = title if isinstance(title, str) else json.dumps(title, ensure_ascii=False)
    c = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
    o = ocr if isinstance(ocr, str) else json.dumps(ocr, ensure_ascii=False)

    prompt = build_summary_prompt(t, c, o)
    try:
        llm_text = call_chat_completion_stream(prompt, model="azure-gpt-4o")
    except Exception as e:
        # 降级：摘要失败时给出错误提示，烈度默认中，focus 默认否（防误推）
        return f"[摘要生成失败] {e}", "中", "否"

    summary, severity, focus = parse_summary_json(llm_text)
    return summary, severity, focus

# ================= 推送数据落库 =================
def _safe_int(v, default=None):
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default

def save_notify_record_to_tidb(data: dict, summary_text: str, severity: str):
    """
    将即将/已经推送到飞书的数据落库到通知表（同库）。
    使用 work_id 作为唯一键，若已存在则更新。
    """
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

# ================= 推送流程 =================
def send_to_feishu(data: dict):
    # 门禁：先判断是否与理想汽车电池/增程器相关，不相关则跳过推送和落库
    try:
        if not check_related(data):
            print("跳过推送与落库：模型判定与理想汽车电池/增程器不相关（记录已新增到数据库）。")
            return False
    except Exception as e:
        print(f"相关性判定异常，默认跳过推送与落库：{e}")
        return False

    post_content = []

    # 生成摘要与烈度，并获取主体聚焦判定
    summary_text, severity, focus = generate_summary_and_severity(data)

    # 新增主体聚焦门禁：若摘要主体不是理想汽车的电池或增程器，则不推送、不落库
    if focus != "是":
        print("跳过推送与落库：摘要主体不聚焦理想汽车的电池或增程器。")
        return False

    advice = ADVICE_BY_SEVERITY.get(severity, ADVICE_BY_SEVERITY["中"])

    for k in ORDERED_FIELDS:
        if k == "summary":
            v = summary_text
        else:
            if k not in data:
                continue
            v = data.get(k)

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
        {"tag": "at", "user_id": FEISHU_OPEN_ID},
        {"tag": "text", "text": f" {advice}（烈度：{severity}）"}
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

    # 推送成功后落库（如需相关即落库，可改为无论 ok 与否都执行 save_notify_record_to_tidb）
    if ok:
        try:
            save_notify_record_to_tidb(data, summary_text, severity)
        except Exception as e:
            print(f"❌ 推送后落库异常: {e}")

    return ok

# ================= 运行入口 =================
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
        # 示例测试数据（仅独立运行时使用）
        test_data = {
            "id": 186,
            "work_id": "315bd20e7e7690e27f2859689ac4ba04",
            "work_url": "www.baidu.com",
            "work_title": "理想L9在寒潮中续航衰减明显，用户反馈充电慢",
            "work_content": "有车主反映理想L9在低温环境下电池表现不佳，续航下降并且充电速度慢，需要优化BMS策略。",
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
            "ocr_content": "理想汽车L9低温续航下降充电慢的舆情曝光"
        }
        print("未检测到输入参数，使用示例数据进行测试推送并落库...")
        send_to_feishu(test_data)