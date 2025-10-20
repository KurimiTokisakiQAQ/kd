# -*- coding: utf-8 -*-
# feishu_notify.py
import requests
import json
import datetime
import base64
import sys
import re

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/c74b9141-1759-40e2-ae3a-50cc6389e1bc"
FEISHU_OPEN_ID = "ou_20b2bd16a8405b93019b7291ec5202c3"

# 大模型调用配置（已鉴权）
API_URL = "https://llm-cmt-api.dev.fc.chj.cloud/agentops/chat/completions"
HEADERS = {
    "Content-Type": "application/json",
    # "Authorization": "Bearer <your-token>",  # 已鉴权环境可保留或移除
}

# 字段映射
FIELD_MAP = {
    "summary":      "文章摘要",          # 新增：摘要字段
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
    "summary",  # 新增摘要字段位置
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
    """
    调用大模型接口（流式），返回完整文本内容（字符串）。
    兼容 OpenAI 风格的 SSE：以 'data:' 行发送 JSON chunk。
    """
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

            # 解析 chunk 的 JSON 外壳，提取 content 文本
            try:
                obj = json.loads(data_bytes.decode("utf-8"))
            except Exception:
                # 若不是 JSON，直接作为文本收集
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
    """
    从模型返回文本中提取 JSON 字符串：
    - 支持三引号代码块 ```json ... ```
    - 支持返回中混杂普通文本，尽量定位第一个完整的 JSON 对象
    """
    s = text.strip()

    # 先尝试匹配代码块 ```json ... ```
    fence = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", s, flags=re.IGNORECASE)
    if fence:
        return fence.group(1).strip()

    # 若没有代码块，尝试定位第一个完整 JSON 对象（简单的括号匹配）
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

    # 最后直接返回原文（交由上层容错）
    return s

# ================= 相关性判定（推送门禁） =================
def build_related_gate_prompt(title: str, content: str, ocr: str) -> str:
    """
    与理想汽车电池相关性的门禁判定提示词。
    仅返回纯 JSON：{"related":"是"} 或 {"related":"否"}
    判定标准：明确出现理想汽车/理想（Li Auto/Li）、且主题聚焦电池相关议题（电池/续航/充电/安全/起火/爆炸/故障/低温/BMS）。
    """
    title = title or ""
    content = content or ""
    ocr = ocr or ""
    return (
        "请判断以下文本是否与“理想汽车”的电池相关。"
        "标准：出现理想汽车/理想（Li Auto/Li）的品牌指向，且话题聚焦电池相关议题（电池、续航、充电、安全、起火、爆炸、故障、低温、BMS等）。"
        "只返回纯 JSON，不要代码块或其他文字："
        '{"related": "是"} 或 {"related": "否"}'
        f"\n标题：{title}\n正文：{content}\nOCR：{ocr}\n"
        "只返回上述 JSON。"
    )

def parse_related_json(text: str) -> bool:
    """
    解析 {"related":"是|否"}；解析失败时进行关键词降级判定。
    """
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
        # 关键词降级：同时出现“理想”或“Li Auto/Li”与电池相关词，判定为相关
        all_text = raw
        li_keywords = ["理想", "理想汽车", "Li Auto", "LI Auto", "li auto", "Li", "理想L", "理想ONE", "理想L7", "理想L8", "理想L9", "理想i8", "理想i6"]
        batt_keywords = ["电池", "续航", "充电", "起火", "爆炸", "漏液", "鼓包", "内阻", "衰减", "低温", "BMS", "电量", "SOC", "容量" , "SOH"]
        has_li = any(k.lower() in all_text.lower() for k in li_keywords)
        has_batt = any(k.lower() in all_text.lower() for k in batt_keywords)
        related = bool(has_li and has_batt)

    return related

def check_related(data: dict) -> bool:
    """
    使用大模型进行门禁判定：是否与理想汽车电池相关。
    失败时采用关键词降级策略。
    """
    title = data.get("work_title") or ""
    content = data.get("work_content") or ""
    ocr = data.get("ocr_content") or ""

    # 避免非字符串类型导致提示词异常
    if isinstance(title, (dict, list)):
        title = json.dumps(title, ensure_ascii=False)
    if isinstance(content, (dict, list)):
        content = json.dumps(content, ensure_ascii=False)
    if isinstance(ocr, (dict, list)):
        ocr = json.dumps(ocr, ensure_ascii=False)

    prompt = build_related_gate_prompt(title, content, ocr)
    try:
        llm_text = call_chat_completion_stream(prompt, model="azure-gpt-4o")
        return parse_related_json(llm_text)
    except Exception:
        # 请求失败时采用关键词降级
        all_text = f"{title}\n{content}\n{ocr}"
        li_keywords = ["理想", "理想汽车", "Li Auto", "LI Auto", "li auto", "Li", "理想L", "理想ONE", "理想L7", "理想L8", "理想L9", "理想i8", "理想i6"]
        batt_keywords = ["电池", "续航", "充电", "起火", "爆炸", "漏液", "鼓包", "内阻", "衰减", "低温", "BMS", "电量", "SOC", "容量" , "SOH"]
        has_li = any(k.lower() in all_text.lower() for k in li_keywords)
        has_batt = any(k.lower() in all_text.lower() for k in batt_keywords)
        return bool(has_li and has_batt)

# ================= 摘要与烈度 =================
def build_summary_prompt(title: str, content: str, ocr: str) -> str:
    """
    摘要约50字，只输出事件摘要（不包含是否与理想汽车电池相关的判断）。
    严格单独给出事件烈度（低/中/高）。
    返回纯 JSON 文本，不要使用代码块或反引号：
    {"summary": "...", "severity": "低|中|高"}
    """
    title = title or ""
    content = content or ""
    ocr = ocr or ""
    return (
        "你是企业舆情分析助手。请阅读主贴标题、正文、OCR识别内容。"
        "请完成："
        "1) 用中文输出约50字的一段事件摘要（不包含是否与理想汽车电池相关的判断）；"
        "2) 单独给出事件烈度，仅可为：低/中/高；"
        "严格返回纯 JSON 文本，不要任何额外文字，不要使用代码块或反引号："
        '{"summary": "<事件摘要>", "severity": "<低|中|高>"}'
        f"\n标题：{title}\n正文：{content}\nOCR：{ocr}\n"
        "只返回上述 JSON。"
    )

def parse_summary_json(text: str):
    """
    解析模型返回的 JSON：{"summary": "...", "severity": "低|中|高"}
    失败时做降级处理：从文本中粗略提取烈度（低/中/高），摘要为整段文本（剥离代码块标记）。
    """
    raw = text.strip()

    # 尝试提取 JSON 字符串
    json_str = _extract_json_from_text(raw)

    summary = None
    severity = None

    # 优先 JSON 解析
    try:
        obj = json.loads(json_str)
        if isinstance(obj, dict):
            summary = str(obj.get("summary", "")).strip() or None
            severity = str(obj.get("severity", "")).strip() or None
    except Exception:
        pass

    # 若 JSON 解析失败或缺项，做降级处理
    if summary is None:
        # 去掉可能的代码块包裹
        summary = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.IGNORECASE).strip()

    if severity not in ("低", "中", "高"):
        m = re.search(r"(低|中|高)", raw)
        if m:
            severity = m.group(1)
        else:
            severity = "中"

    return summary, severity

def generate_summary_and_severity(data: dict):
    """
    基于主贴标题、正文内容和 OCR 内容生成摘要与烈度。
    推送消息不对摘要做字数限制。
    """
    title = data.get("work_title") or ""
    content = data.get("work_content") or ""
    ocr = data.get("ocr_content") or ""

    # 避免非字符串类型导致提示词异常
    if isinstance(title, (dict, list)):
        title = json.dumps(title, ensure_ascii=False)
    if isinstance(content, (dict, list)):
        content = json.dumps(content, ensure_ascii=False)
    if isinstance(ocr, (dict, list)):
        ocr = json.dumps(ocr, ensure_ascii=False)

    prompt = build_summary_prompt(title, content, ocr)
    try:
        llm_text = call_chat_completion_stream(prompt, model="azure-gpt-4o")
    except Exception as e:
        # 降级：摘要失败时给出错误提示，烈度默认中
        return f"[摘要生成失败] {e}", "中"

    summary, severity = parse_summary_json(llm_text)
    return summary, severity

# ================= 推送流程 =================
def send_to_feishu(data: dict):
    # 门禁：先判断是否与理想汽车电池相关，不相关则跳过推送
    try:
        if not check_related(data):
            print("跳过推送：模型判定与理想汽车电池不相关（记录已新增到数据库）。")
            return
    except Exception as e:
        print(f"相关性判定异常，默认跳过推送：{e}")
        return

    post_content = []

    # 生成摘要与烈度（摘要不截断）
    summary_text, severity = generate_summary_and_severity(data)
    advice = ADVICE_BY_SEVERITY.get(severity, ADVICE_BY_SEVERITY["中"])

    for k in ORDERED_FIELDS:
        # “summary”为虚拟字段，从LLM生成，不在原数据中
        if k == "summary":
            v = summary_text
        else:
            if k not in data:
                continue
            v = data.get(k)

            # 时间格式化
            if isinstance(v, datetime.datetime):
                v = v.strftime("%Y-%m-%d %H:%M:%S")
            if v is None:
                v = ""

            # 特定字段处理（摘要不参与截断）
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

    # 末尾：@ 指定人 + 同一行追加处理意见（期望格式：@人 请相关人员XX（烈度：X））
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

# ================= 运行入口 =================
if __name__ == "__main__":
    # 运行逻辑：
    # - 如果通过命令行传入 JSON 字符串（tidb 监控脚本调用），则判定并推送
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
        # 示例测试数据（仅独立运行时使用，可自行替换）
        test_data = {
            "id": 186,
            "work_id": "315bd20e7e7690e27f2859689ac4ba04",
            "work_url": "www.baidu.com",
            "work_title": "电池爆炸，死伤10余人（理想L9疑似事故）",
            "work_content": "据现场消息，疑似理想L9发生电池爆炸导致人员伤亡，具体原因调查中",
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
            "ocr_content": "报道称理想汽车某车型电池故障引发起火爆炸，现场有伤亡"
        }
        print("未检测到输入参数，使用示例数据进行测试推送...")
        send_to_feishu(test_data)