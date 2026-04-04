#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import os
import re
import errno
import sys
import time
import traceback
import socket
import gzip
import concurrent.futures
import signal
import secrets
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import requests
import random
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

STAT_TZ = os.environ.get("STAT_TZ", "Asia/Shanghai")
TZ = ZoneInfo(STAT_TZ)  # 统计时区

KOMARI_BASE_URL = os.environ.get("KOMARI_BASE_URL", "").rstrip("/")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DATA_DIR = os.environ.get("DATA_DIR", "/var/lib/komari-traffic")

HISTORY_HOT_DAYS = int(os.environ.get("HISTORY_HOT_DAYS", "60"))
HISTORY_RETENTION_DAYS = int(os.environ.get("HISTORY_RETENTION_DAYS", "400"))

KOMARI_API_TOKEN = os.environ.get("KOMARI_API_TOKEN", "")
KOMARI_API_TOKEN_HEADER = os.environ.get("KOMARI_API_TOKEN_HEADER", "Authorization")
KOMARI_API_TOKEN_PREFIX = os.environ.get("KOMARI_API_TOKEN_PREFIX", "Bearer")
KOMARI_FETCH_WORKERS = int(os.environ.get("KOMARI_FETCH_WORKERS", "6"))

TOP_N = int(os.environ.get("TOP_N", "3"))  # 默认 Top3（日报/周报/月报/Top命令都用它）

AI_API_BASE = os.environ.get("AI_API_BASE", "").rstrip("/")
AI_API_KEY = os.environ.get("AI_API_KEY", "").strip()
AI_MODEL = os.environ.get("AI_MODEL", "").strip()

# /top Nh 依赖采样快照：bot 运行时自动采样
SAMPLE_INTERVAL_SECONDS = int(os.environ.get("SAMPLE_INTERVAL_SECONDS", "300"))  # 默认 5 分钟
SAMPLE_RETENTION_HOURS = int(os.environ.get("SAMPLE_RETENTION_HOURS", "2"))    # 默认保留 2 小时采样

BASELINES_PATH = os.path.join(DATA_DIR, "baselines.json")
HISTORY_PATH = os.path.join(DATA_DIR, "history.json")
SAMPLES_PATH = os.path.join(DATA_DIR, "samples.json")
TG_OFFSET_PATH = os.path.join(DATA_DIR, "tg_offset.txt")
TG_CONFIRM_PATH = os.path.join(DATA_DIR, "tg_confirm.json")
AI_PACK_CACHE_PATH = os.path.join(DATA_DIR, "ai_pack_cache.json")

TIMEOUT = int(os.environ.get("KOMARI_TIMEOUT_SECONDS", "15"))  # Komari API timeout（秒）

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.environ.get("LOG_FILE", "").strip()

BOT_INSTANCE_NAME = os.environ.get("BOT_INSTANCE_NAME", "").strip()
BOT_START_NOTIFY = os.environ.get("BOT_START_NOTIFY", "1").strip().lower() not in ("0", "false", "no", "off")
AI_PACK_CACHE_TTL_SECONDS = max(0, int(os.environ.get("AI_PACK_CACHE_TTL_SECONDS", "3600")))

SHUTTING_DOWN = False
SAMPLE_THREAD: threading.Thread | None = None
SAMPLE_STOP_EVENT = threading.Event()


def ai_enabled() -> bool:
    return bool(AI_API_BASE and AI_API_KEY and AI_MODEL)


def build_http_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


HTTP_SESSION = build_http_session()


def setup_logging():
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if LOG_FILE:
        handlers.append(logging.FileHandler(LOG_FILE, encoding="utf-8"))
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
    )


def build_komari_headers() -> dict:
    headers = {"Accept": "application/json"}
    if KOMARI_API_TOKEN:
        prefix = KOMARI_API_TOKEN_PREFIX.strip()
        value = f"{prefix} {KOMARI_API_TOKEN}".strip()
        headers[KOMARI_API_TOKEN_HEADER] = value
    return headers


def _require_positive_int(name: str, value: int):
    if value <= 0:
        raise RuntimeError(f"{name} must be > 0")


def validate_config_or_raise():
    required = {
        "KOMARI_BASE_URL": KOMARI_BASE_URL,
        "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
        "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
    }
    missing = [k for k, v in required.items() if not str(v).strip()]
    if missing:
        raise RuntimeError("Missing required env: " + ", ".join(missing))

    _require_positive_int("KOMARI_TIMEOUT_SECONDS", TIMEOUT)
    _require_positive_int("KOMARI_FETCH_WORKERS", KOMARI_FETCH_WORKERS)
    _require_positive_int("TOP_N", TOP_N)
    _require_positive_int("SAMPLE_INTERVAL_SECONDS", SAMPLE_INTERVAL_SECONDS)
    _require_positive_int("SAMPLE_RETENTION_HOURS", SAMPLE_RETENTION_HOURS)


def run_healthcheck_or_raise():
    ensure_dirs()

    test_path = os.path.join(DATA_DIR, ".health_write_test")
    try:
        with open(test_path, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(test_path)
    except Exception as e:
        raise RuntimeError(f"DATA_DIR not writable: {DATA_DIR}: {e}")

    for p in [BASELINES_PATH, HISTORY_PATH, SAMPLES_PATH, TG_OFFSET_PATH, TG_CONFIRM_PATH]:
        if os.path.exists(p):
            try:
                if p == TG_OFFSET_PATH:
                    _ = load_offset()
                else:
                    load_json_strict(p)
            except Exception as e:
                raise RuntimeError(f"Corrupted file: {p}: {e}")

    try:
        HTTP_SESSION.get(KOMARI_BASE_URL, timeout=TIMEOUT, headers=build_komari_headers())
    except Exception as e:
        raise RuntimeError(f"Komari unreachable: {e}")


def _handle_sigterm(signum, _frame):
    global SHUTTING_DOWN
    SHUTTING_DOWN = True
    logging.warning("received signal %s, shutting down gracefully...", signum)


# -------------------- 基础工具 --------------------

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    try:
        os.chmod(DATA_DIR, 0o700)
    except Exception:
        pass


def human_bytes(n: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    x = float(max(int(n), 0))
    for idx, u in enumerate(units):
        if x < 1024 or idx == len(units) - 1:
            return f"{x:.2f} {u}" if u != "B" else f"{int(x)} B"
        x /= 1024


def now_dt() -> datetime:
    return datetime.now(TZ)


def today_date() -> date:
    return now_dt().date()


def start_of_day(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=TZ)


def start_of_week(d: date) -> date:
    return d - timedelta(days=d.weekday())  # 周一为起点


def start_of_month(d: date) -> date:
    return date(d.year, d.month, 1)


def yyyymm(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception:
        return default


def load_json_strict(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_date_yyyy_mm_dd(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def save_json_atomic(path: str, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    try:
        os.replace(tmp, path)
    except OSError as e:
        if e.errno != errno.EXDEV:
            raise
        with open(path, "w", encoding="utf-8") as dst, open(tmp, "r", encoding="utf-8") as src:
            dst.write(src.read())
        os.unlink(tmp)


def get_json(url: str):
    r = HTTP_SESSION.get(url, timeout=TIMEOUT, headers=build_komari_headers())
    r.raise_for_status()
    return r.json()


def post_json(url: str, payload: dict):
    r = requests.post(url, json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def telegram_send(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 未设置")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    return post_json(url, payload)


def safe_telegram_send(text: str):
    try:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            telegram_send(text)
    except Exception:
        pass


def ai_chat(messages: list[dict]) -> str:
    """
    通用 OpenAI 兼容 chat.completions 调用。
    """
    if not ai_enabled():
        return "⚠️ AI 未启用：请先配置 AI_API_BASE / AI_API_KEY / AI_MODEL 环境变量。"

    url = f"{AI_API_BASE}/chat/completions"
    headers = {
        "Authorization": f"Bearer {AI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": AI_MODEL,
        "messages": messages,
        "temperature": 0.3,
        "max_tokens": 1000,
    }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        choices = data.get("choices") or []
        if not choices:
            return "⚠️ AI 没有返回内容，请稍后重试。"
        content = (choices[0].get("message") or {}).get("content") or ""
        return content.strip() or "⚠️ AI 返回了空结果，请稍后重试。"
    except Exception as e:
        logging.exception("ai_chat error")
        return f"⚠️ 调用 AI 失败：{type(e).__name__}: {e}"

def ask_ai_with_data(question: str, data_pack: dict) -> str:
    """
    给 AI：用户问题 + 经过 Python 计算好的数据包。
    所有数值都由 Python 从 Komari / JSON 中算好，AI 只负责解读。
    """
    focused_pack = build_focused_ai_data_pack(question, data_pack)
    error_keys = [
        k for k, v in (data_pack or {}).items()
        if isinstance(v, dict) and v.get("error") == "failed"
    ]
    if len(error_keys) >= 3:
        logging.warning("ask_ai_with_data aborted: too many failed data sources: %s", ",".join(error_keys))
        return "⚠️ 数据获取异常，稍后再试。"

    try:
        data_text = json.dumps(focused_pack, ensure_ascii=False)
    except Exception:
        logging.exception("serialize data_pack error")
        data_text = str(focused_pack)

    system_prompt = (
        "你是一个 Komari 流量机器人助手，负责帮用户解读流量统计数据。\n"
        "你会收到一个 JSON 数据包 data_pack，里面所有数值都已经由程序计算完成。\n"
        "规则：\n"
        "1. 所有具体数值（例如流量大小、排名）必须直接来自 data_pack，不要自己发明新数字。\n"
        "2. 如果 data_pack 中没有足够信息回答某个问题，请明确说明“无法从当前数据中判断”，不要瞎猜。\n"
        "3. 回答使用简洁中文，优先使用 *_human 字段展示人类可读流量单位（如 GiB/TiB），避免输出超长原始整数。\n"
        "4. 涉及近 24 小时/7 天/30 天节点流量对比时，优先使用 last_24h、last_7d、last_30d 的 top_nodes。\n"
        "5. 所有回答都尽量按固定模板组织：优先输出“结论”，必要时补“依据 / 趋势 / 建议 / 风险提示”。\n"
        "6. 输出必须适合 Telegram 阅读：每个标题单独成行，每段 2~4 条短句，尽量让用户一眼扫完。\n"
        "6.1 只允许输出 Telegram HTML 支持标签（如 <b>/<i>/<code>），不要输出 Markdown（如 #、*、```）。\n"
        "7. 列表统一使用简洁项目符号，不要使用冗长的 1) 2) 3) 序号堆砌。\n"
        "8. 若用户问“刚刚这一小时/最近1小时某节点用了多少”，优先使用 last_1h_by_node.nodes。\n"
        "9. 若用户问“今天按小时某节点趋势/峰谷”，优先使用 today_hourly_by_node.nodes[*].hours / peak_hour / valley_hour。\n"
        "10. 若用户问“昨天按小时某节点趋势/峰谷”，优先使用 yesterday_hourly_by_node.nodes[*].hours / peak_hour / valley_hour。\n"
        "11. 若用户问全局小时级峰谷，优先使用 last_24h_hourly.hours / peak_hour / valley_hour。\n"
        "12. 不要向用户暴露内部字段名或实现细节，例如 data_pack、last_1h_by_node、today_hourly_by_node、up_human、down_human、total_human。\n"
        "13. 要把内部统计字段翻译成自然语言，直接说“最近1小时”“上行”“下行”“合计”等用户能看懂的话。\n"
        "14. 不需要原样打印整个 JSON，只引用对结论有用的关键信息。\n"
        "15. 不要写成开发日志或调试输出，不要出现‘字段/结构/键名/pack’等工程化表述。"
        "16. 字段说明：last_24h/last_7d/last_30d 分别表示最近 24h/168h/720h 的节点汇总；其中 cpu/ram/disk 为使用率百分比统计（avg/max/min）。"
    )
    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": (
                f"这是当前的 data_pack（JSON 格式）：\n"
                f"{data_text}\n\n"
                f"用户问题：{question}\n\n"
                "请严格根据 data_pack 里的数据进行分析和回答。"
            ),
        },
    ]
    return ai_chat(messages)


def question_requires_fresh_ai_pack(question: str) -> bool:
    q = (question or "").lower().strip()
    hot_keywords = (
        "刚刚", "最近1小时", "最近 1 小时", "一小时", "1小时", "1 h", "1h",
        "今天按小时", "今日按小时", "小时趋势", "小时级",
    )
    return any(k in q for k in hot_keywords)


def build_focused_ai_data_pack(question: str, data_pack: dict) -> dict:
    """
    按问题类型缩小喂给 AI 的数据范围，减少模型被无关字段干扰。
    """
    q = (question or "").lower().strip()
    focused = {
        "now": data_pack.get("now"),
        "stat_tz": data_pack.get("stat_tz"),
    }

    if any(k in q for k in ("刚刚", "最近1小时", "最近 1 小时", "一小时", "1小时", "1 h", "1h")):
        focused["last_1h_by_node"] = data_pack.get("last_1h_by_node")
        return focused

    if ("今天" in q or "今日" in q) and ("小时" in q or "峰谷" in q or "走势" in q):
        focused["today_hourly_by_node"] = data_pack.get("today_hourly_by_node")
        focused["today"] = data_pack.get("today")
        return focused

    if ("昨天" in q or "昨日" in q) and ("小时" in q or "峰谷" in q or "走势" in q):
        focused["yesterday_hourly_by_node"] = data_pack.get("yesterday_hourly_by_node")
        return focused

    return data_pack


def normalize_ai_answer_for_telegram(text: str) -> str:
    """
    将常见 Markdown 回答转换成更适合 Telegram(HTML parse_mode) 的纯文本样式。
    避免出现大量 * / # 等原样符号影响可读性。
    """
    out = (text or "").replace("\r\n", "\n").strip()
    if not out:
        return "⚠️ AI 返回为空，请稍后重试。"

    # 保持最小清洗，主体格式约束交给 system prompt，减少脆弱正则链。
    out = out.replace("```html", "").replace("```", "")
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()

def should_alert(throttle_key: str, min_interval_seconds: int = 300) -> bool:
    ensure_dirs()
    state_path = os.path.join(DATA_DIR, f"alert_{throttle_key}.json")
    now_ts = int(time.time())
    state = load_json(state_path, {"last": 0})
    last = int(state.get("last", 0))
    if now_ts - last < min_interval_seconds:
        return False
    save_json_atomic(state_path, {"last": now_ts})
    return True


def alert_exception(where: str, cmd: str, exc: Exception):
    host = socket.gethostname()
    ts = now_dt().strftime("%Y-%m-%d %H:%M:%S %Z")
    err = f"{type(exc).__name__}: {exc}"
    tb = traceback.format_exc()
    tb_tail = (tb[-1500:]).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    msg = (
        f"❌ <b>Komari 流量任务失败</b>\n"
        f"🕒 {ts}\n"
        f"🖥 {host}\n"
        f"📍 {where}\n"
        f"🧩 cmd: <code>{cmd}</code>\n"
        f"🧨 error: <code>{err}</code>\n\n"
        f"<b>traceback (tail)</b>\n<pre>{tb_tail}</pre>"
    )
    safe_telegram_send(msg)


# -------------------- Komari 数据获取（稳态：单节点超时/异常跳过） --------------------

@dataclass
class NodeTotal:
    uuid: str
    name: str
    up: int
    down: int


def fetch_nodes_and_totals():
    """
    返回：
      - out: list[NodeTotal]
      - skipped: list[str]  # 被跳过的节点原因（timeout/empty/bad_resp/HTTPError等）
    """
    if not KOMARI_BASE_URL:
        raise RuntimeError("KOMARI_BASE_URL 未设置（例如 https://komari.example）")

    nodes_resp = get_json(f"{KOMARI_BASE_URL}/api/nodes")
    if not (isinstance(nodes_resp, dict) and nodes_resp.get("status") == "success"):
        raise RuntimeError(f"/api/nodes 返回异常：{nodes_resp}")

    nodes = nodes_resp.get("data", [])
    out: list[NodeTotal] = []
    skipped: list[str] = []

    def fetch_one(node: dict):
        uuid = node.get("uuid")
        name = node.get("name") or uuid
        if not uuid:
            return None, None
        try:
            recent_resp = get_json(f"{KOMARI_BASE_URL}/api/recent/{uuid}")
        except requests.exceptions.ReadTimeout:
            return None, f"{name}(timeout)"
        except requests.exceptions.RequestException as e:
            return None, f"{name}({type(e).__name__})"
        except Exception as e:
            return None, f"{name}({type(e).__name__})"

        if not (isinstance(recent_resp, dict) and recent_resp.get("status") == "success"):
            return None, f"{name}(bad_resp)"

        points = recent_resp.get("data", [])
        if not points:
            return None, f"{name}(empty)"

        last = points[-1]
        net = last.get("network", {}) if isinstance(last, dict) else {}
        up = int(net.get("totalUp", 0))
        down = int(net.get("totalDown", 0))
        return NodeTotal(uuid=uuid, name=name, up=up, down=down), None

    max_workers = max(1, min(len(nodes), KOMARI_FETCH_WORKERS))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(fetch_one, n): n for n in nodes}
        for future in concurrent.futures.as_completed(future_map):
            result, skip = future.result()
            if skip:
                skipped.append(skip)
                continue
            if result:
                out.append(result)

    return out, skipped


def fetch_node_records(uuid: str, hours: int) -> list[dict]:
    if not uuid:
        raise ValueError("uuid is required")
    if hours <= 0:
        raise ValueError("hours must be > 0")
    url = f"{KOMARI_BASE_URL}/api/records/load"
    r = HTTP_SESSION.get(
        url,
        params={"uuid": uuid, "hours": int(hours)},
        timeout=TIMEOUT,
        headers=build_komari_headers(),
    )
    r.raise_for_status()
    payload = r.json()
    if not (isinstance(payload, dict) and payload.get("status") == "success"):
        raise RuntimeError(f"/api/records/load bad response: {payload}")
    data = payload.get("data", {})
    if not isinstance(data, dict):
        raise RuntimeError(f"/api/records/load data is invalid: {payload}")
    records = data.get("records", [])
    if not isinstance(records, list):
        raise RuntimeError(f"/api/records/load records is invalid: {payload}")
    return records


def _to_float_safe(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _metric_stats(values: list[float]) -> dict:
    if not values:
        return {"avg": None, "max": None, "min": None}
    return {
        "avg": round(sum(values) / len(values), 2),
        "max": round(max(values), 2),
        "min": round(min(values), 2),
    }


def _record_time_label(record: dict) -> str | None:
    if not isinstance(record, dict):
        return None
    for key in ("time", "timestamp", "created_at", "createdAt", "ts"):
        v = record.get(key)
        if v is not None:
            return str(v)
    return None


def compute_traffic_from_records(records: list[dict]) -> dict:
    if not records:
        up = down = 0
        first = last = None
    else:
        first = records[0]
        last = records[-1]
        up = max(0, int(last.get("net_total_up", 0)) - int(first.get("net_total_up", 0)))
        down = max(0, int(last.get("net_total_down", 0)) - int(first.get("net_total_down", 0)))

    total = up + down
    cpu_values = []
    ram_values = []
    disk_values = []
    for rec in records:
        cpu = _to_float_safe(rec.get("cpu"))
        ram = _to_float_safe(rec.get("ram"))
        disk = _to_float_safe(rec.get("disk"))
        if cpu is not None:
            cpu_values.append(cpu)
        if ram is not None:
            ram_values.append(ram)
        if disk is not None:
            disk_values.append(disk)

    return {
        "up": up,
        "down": down,
        "total": total,
        "up_human": human_bytes(up),
        "down_human": human_bytes(down),
        "total_human": human_bytes(total),
        "cpu": _metric_stats(cpu_values),
        "ram": _metric_stats(ram_values),
        "disk": _metric_stats(disk_values),
        "record_count": len(records),
        "from": _record_time_label(first) if first else None,
        "to": _record_time_label(last) if last else None,
    }


def build_records_summary(hours: int) -> dict:
    if hours <= 0:
        raise ValueError("hours must be > 0")
    nodes_resp = get_json(f"{KOMARI_BASE_URL}/api/nodes")
    if not (isinstance(nodes_resp, dict) and nodes_resp.get("status") == "success"):
        raise RuntimeError(f"/api/nodes 返回异常：{nodes_resp}")
    nodes = nodes_resp.get("data", [])
    if not isinstance(nodes, list):
        raise RuntimeError(f"/api/nodes data 非列表：{nodes_resp}")

    out_nodes: list[dict] = []
    skipped: list[str] = []

    def fetch_one(node: dict):
        uuid = node.get("uuid")
        name = node.get("name") or uuid or "unknown"
        if not uuid:
            return None, f"{name}(missing_uuid)"
        try:
            records = fetch_node_records(str(uuid), hours)
            summary = compute_traffic_from_records(records)
            summary["uuid"] = str(uuid)
            summary["name"] = str(name)
            return summary, None
        except requests.exceptions.ReadTimeout:
            return None, f"{name}(timeout)"
        except Exception as e:
            return None, f"{name}({type(e).__name__})"

    max_workers = max(1, min(len(nodes), KOMARI_FETCH_WORKERS))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(fetch_one, n): n for n in nodes}
        for future in concurrent.futures.as_completed(future_map):
            item, err = future.result()
            if err:
                skipped.append(err)
            elif item:
                out_nodes.append(item)

    out_nodes.sort(key=lambda x: (x["total"], x["down"], x["up"], x["name"].lower()), reverse=True)
    return {
        "hours": int(hours),
        "nodes": out_nodes,
        "top_nodes": out_nodes[: max(0, int(TOP_N))],
        "skipped": sorted(skipped),
    }


def build_nodes_map_from_current(current: list[NodeTotal]) -> dict:
    return {n.uuid: {"name": n.name, "up": n.up, "down": n.down} for n in current}


def compute_delta_from_nodes(current: list[NodeTotal], baseline_nodes: dict) -> tuple[dict, dict, list[str]]:
    """
    baseline_nodes: {uuid:{name,up,down}}
    returns: (deltas, new_baseline_nodes, reset_warnings)
    """
    deltas = {}
    new_baseline = {}
    reset_warnings = []

    for n in current:
        prev = baseline_nodes.get(n.uuid, {})
        prev_up = int(prev.get("up", 0))
        prev_down = int(prev.get("down", 0))

        up_delta = n.up - prev_up
        down_delta = n.down - prev_down

        reset = False
        if up_delta < 0:
            up_delta = n.up
            reset = True
        if down_delta < 0:
            down_delta = n.down
            reset = True
        if reset:
            reset_warnings.append(n.name)

        deltas[n.uuid] = {"name": n.name, "up": up_delta, "down": down_delta}
        new_baseline[n.uuid] = {"name": n.name, "up": n.up, "down": n.down}

    return deltas, new_baseline, reset_warnings


def compute_delta_from_maps(current_nodes_map: dict, baseline_nodes_map: dict) -> tuple[dict, list[str]]:
    """
    current_nodes_map / baseline_nodes_map:
      {uuid:{name,up,down}}
    returns: (deltas, reset_warnings)
    """
    deltas = {}
    reset_warnings = []
    for uuid, cur in current_nodes_map.items():
        prev = baseline_nodes_map.get(uuid, {})
        name = cur.get("name", uuid)

        cur_up = int(cur.get("up", 0))
        cur_down = int(cur.get("down", 0))
        prev_up = int(prev.get("up", 0))
        prev_down = int(prev.get("down", 0))

        up_delta = cur_up - prev_up
        down_delta = cur_down - prev_down

        reset = False
        if up_delta < 0:
            up_delta = cur_up
            reset = True
        if down_delta < 0:
            down_delta = cur_down
            reset = True
        if reset:
            reset_warnings.append(name)

        deltas[uuid] = {"name": name, "up": up_delta, "down": down_delta}

    return deltas, reset_warnings


def compute_strict_sample_delta_from_maps(current_nodes_map: dict, previous_nodes_map: dict) -> tuple[dict, list[str]]:
    """
    用于 samples.json 邻近样本之间的严格差分。

    规则：
    - 仅对“当前样本和前一样本都存在”的节点计算差分；
    - 若计数器回退/重置（负差），该段差分按 0 处理，不回退到当前累计值；
    - 这样可避免把累计计数器绝对值误判成某一小时/某一采样区间的流量。
    returns: (deltas, warnings)
    """
    deltas = {}
    warnings = []

    for uuid, cur in current_nodes_map.items():
        prev = previous_nodes_map.get(uuid)
        name = cur.get("name", uuid)

        if not prev:
            warnings.append(f"{name}(missing_prev)")
            continue

        cur_up = int(cur.get("up", 0))
        cur_down = int(cur.get("down", 0))
        prev_up = int(prev.get("up", 0))
        prev_down = int(prev.get("down", 0))

        up_delta = cur_up - prev_up
        down_delta = cur_down - prev_down

        if up_delta < 0 or down_delta < 0:
            warnings.append(f"{name}(counter_reset)")
            up_delta = max(up_delta, 0)
            down_delta = max(down_delta, 0)

        deltas[uuid] = {"name": name, "up": up_delta, "down": down_delta}

    return deltas, warnings


# -------------------- Top 榜展示 --------------------

def top_lines(deltas: dict, n: int) -> list[str]:
    items = []
    for v in deltas.values():
        name = v.get("name", "")
        up = int(v.get("up", 0))
        down = int(v.get("down", 0))
        total = up + down
        items.append((total, down, up, name))

    items.sort(reverse=True, key=lambda x: (x[0], x[1], x[2], x[3].lower()))
    top = items[: max(0, int(n))]

    if not top:
        return ["（暂无数据）"]

    rows = []
    for i, (total, down, up, name) in enumerate(top, start=1):
        rows.append(
            f"{i}️⃣ <b>{name}</b>：{human_bytes(total)}"
            f"（⬇️ {human_bytes(down)} / ⬆️ {human_bytes(up)}）"
        )
    return rows


def build_today_delta_struct() -> dict | None:
    """
    返回今天各节点增量统计的结构化数据。
    """
    td = today_date()
    tag = td.strftime("%Y-%m-%d")
    baseline_nodes = get_baseline_nodes(tag)
    now = now_dt()
    result = {
        "date": tag,
        "now": now.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "nodes": [],
        "skipped": [],
        "reset_warnings": [],
    }

    current, skipped = fetch_nodes_and_totals()
    result["skipped"] = skipped

    if baseline_nodes is None:
        for n in current:
            up = int(n.up)
            down = int(n.down)
            total = up + down
            result["nodes"].append(
                {
                    "name": n.name,
                    "up": up,
                    "down": down,
                    "total": total,
                    "up_human": human_bytes(up),
                    "down_human": human_bytes(down),
                    "total_human": human_bytes(total),
                }
            )
        result["nodes"].sort(key=lambda x: (x["total"], x["down"], x["up"], x["name"].lower()), reverse=True)
        result["note"] = "baseline_missing"
        return result

    deltas, _new_baseline, reset_warnings = compute_delta_from_nodes(current, baseline_nodes)
    result["reset_warnings"] = reset_warnings

    for v in deltas.values():
        up = int(v.get("up", 0))
        down = int(v.get("down", 0))
        total = up + down
        result["nodes"].append(
            {
                "name": v.get("name", ""),
                "up": up,
                "down": down,
                "total": total,
                "up_human": human_bytes(up),
                "down_human": human_bytes(down),
                "total_human": human_bytes(total),
            }
        )
    result["nodes"].sort(key=lambda x: (x["total"], x["down"], x["up"], x["name"].lower()), reverse=True)
    result["note"] = "baseline_ok"
    return result

def get_top_last_hours_struct(hours: int, n: int) -> dict | None:
    """
    基于 samples.json 计算最近 N 小时的 Top 榜（结构化）。
    """
    if hours <= 0:
        return None

    ensure_dirs()
    take_sample_if_due(force=True)

    now_ts = int(time.time())
    target_ts = now_ts - hours * 3600
    base = get_sample_at_or_before(target_ts)
    if base is None:
        return {
            "hours": hours,
            "error": "no_base_sample",
            "message": f"还没有足够的采样历史来计算最近 {hours} 小时的数据。",
        }

    data = load_samples()
    samples = data.get("samples", [])
    if not samples:
        return {
            "hours": hours,
            "error": "no_samples",
            "message": "采样数据为空。",
        }

    cur = samples[-1]
    deltas, reset_warnings = compute_delta_from_maps(cur.get("nodes", {}), base.get("nodes", {}))
    skipped = list(dict.fromkeys((base.get("skipped", []) or []) + (cur.get("skipped", []) or [])))

    from_dt = datetime.fromtimestamp(int(base["ts"]), TZ)
    to_dt = datetime.fromtimestamp(int(cur["ts"]), TZ)

    nodes = []
    for v in deltas.values():
        up = int(v.get("up", 0))
        down = int(v.get("down", 0))
        total = up + down
        nodes.append(
            {
                "name": v.get("name", ""),
                "up": up,
                "down": down,
                "total": total,
                "up_human": human_bytes(up),
                "down_human": human_bytes(down),
                "total_human": human_bytes(total),
            }
        )
    nodes.sort(key=lambda x: (x["total"], x["down"], x["up"], x["name"].lower()), reverse=True)
    nodes = nodes[: max(0, int(n))]

    return {
        "hours": hours,
        "from": from_dt.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "to": to_dt.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "nodes": nodes,
        "skipped": skipped,
        "reset_warnings": reset_warnings,
    }


def get_last_hours_nodes_struct(hours: int) -> dict | None:
    """
    返回最近 N 小时所有节点的结构化差分（不截断 Top N）。
    """
    if hours <= 0:
        return None

    ensure_dirs()
    take_sample_if_due(force=True)

    now_ts = int(time.time())
    target_ts = now_ts - hours * 3600
    base = get_sample_at_or_before(target_ts)
    if base is None:
        return {
            "hours": hours,
            "error": "no_base_sample",
            "message": f"还没有足够的采样历史来计算最近 {hours} 小时的数据。",
        }

    data = load_samples()
    samples = data.get("samples", [])
    if not samples:
        return {
            "hours": hours,
            "error": "no_samples",
            "message": "采样数据为空。",
        }

    cur = samples[-1]
    deltas, warnings = compute_strict_sample_delta_from_maps(cur.get("nodes", {}), base.get("nodes", {}))
    skipped = list(dict.fromkeys((base.get("skipped", []) or []) + (cur.get("skipped", []) or [])))

    from_dt = datetime.fromtimestamp(int(base["ts"]), TZ)
    to_dt = datetime.fromtimestamp(int(cur["ts"]), TZ)

    nodes = []
    for v in deltas.values():
        up = int(v.get("up", 0))
        down = int(v.get("down", 0))
        total = up + down
        nodes.append(
            {
                "name": v.get("name", ""),
                "up": up,
                "down": down,
                "total": total,
                "up_human": human_bytes(up),
                "down_human": human_bytes(down),
                "total_human": human_bytes(total),
            }
        )
    nodes.sort(key=lambda x: (x["total"], x["down"], x["up"], x["name"].lower()), reverse=True)

    return {
        "hours": hours,
        "from": from_dt.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "to": to_dt.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "nodes": nodes,
        "skipped": skipped,
        "warnings": warnings,
    }



def build_last_24h_hourly_summary() -> dict:
    """
    基于 samples.json 构造最近 24 小时按小时分桶的总流量分布，
    用于回答“小时级峰谷”问题。
    """
    ensure_dirs()
    take_sample_if_due(force=True)

    data = load_samples()
    samples = sorted(data.get("samples", []), key=lambda x: int(x.get("ts", 0)))
    if len(samples) < 2:
        return {
            "error": "insufficient_samples",
            "message": "采样点不足，无法计算最近 24 小时小时级分布。",
        }

    now_ts = int(time.time())
    from_ts = now_ts - 24 * 3600

    # 从最近一个 <= from_ts 的样本开始累计差分
    prev = None
    for s0 in samples:
        ts0 = int(s0.get("ts", 0))
        if ts0 <= from_ts:
            prev = s0
        else:
            break
    if prev is None:
        prev = samples[0]

    bucket_map: dict[str, dict] = {}

    for cur in samples:
        cur_ts = int(cur.get("ts", 0))
        if cur_ts <= int(prev.get("ts", 0)):
            continue
        if cur_ts < from_ts:
            prev = cur
            continue

        deltas, _warnings = compute_strict_sample_delta_from_maps(cur.get("nodes", {}), prev.get("nodes", {}))
        up = sum(int(v.get("up", 0)) for v in deltas.values())
        down = sum(int(v.get("down", 0)) for v in deltas.values())
        total = up + down

        hour_label = datetime.fromtimestamp(cur_ts, TZ).strftime("%Y-%m-%d %H:00")
        if hour_label not in bucket_map:
            bucket_map[hour_label] = {"hour": hour_label, "up": 0, "down": 0, "total": 0}
        bucket_map[hour_label]["up"] += up
        bucket_map[hour_label]["down"] += down
        bucket_map[hour_label]["total"] += total

        prev = cur

    hours = list(bucket_map.values())
    hours.sort(key=lambda x: x["hour"])
    for h in hours:
        h["up_human"] = human_bytes(h["up"])
        h["down_human"] = human_bytes(h["down"])
        h["total_human"] = human_bytes(h["total"])

    peak_hour = max(hours, key=lambda x: x["total"]) if hours else None
    valley_hour = min(hours, key=lambda x: x["total"]) if hours else None

    return {
        "from": datetime.fromtimestamp(from_ts, TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "to": datetime.fromtimestamp(now_ts, TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "hours": hours,
        "peak_hour": peak_hour,
        "valley_hour": valley_hour,
    }
def build_yesterday_hourly_by_node_summary() -> dict:
    """
    基于 samples.json 统计“昨天 00:00~24:00”各节点小时级走势。
    用于回答“某节点昨天哪个小时最忙、是否有峰谷”。
    """
    ensure_dirs()
    take_sample_if_due(force=True)

    td = today_date()
    yday = td - timedelta(days=1)
    from_ts = int(start_of_day(yday).timestamp())
    to_ts = int(start_of_day(td).timestamp())

    data = load_samples()
    samples = sorted(data.get("samples", []), key=lambda x: int(x.get("ts", 0)))
    if len(samples) < 2:
        return {
            "date": yday.strftime("%Y-%m-%d"),
            "error": "insufficient_samples",
            "message": "采样点不足，无法计算昨天节点小时级分布。",
        }

    prev = None
    for s0 in samples:
        ts0 = int(s0.get("ts", 0))
        if ts0 <= from_ts:
            prev = s0
        else:
            break
    if prev is None:
        prev = samples[0]

    node_hour_map: dict[str, dict] = {}
    for cur in samples:
        cur_ts = int(cur.get("ts", 0))
        if cur_ts <= int(prev.get("ts", 0)):
            continue
        if cur_ts < from_ts:
            prev = cur
            continue
        if cur_ts > to_ts:
            break

        deltas, _warnings = compute_strict_sample_delta_from_maps(cur.get("nodes", {}), prev.get("nodes", {}))
        hour_label = datetime.fromtimestamp(cur_ts, TZ).strftime("%Y-%m-%d %H:00")

        for v in deltas.values():
            name = v.get("name", "")
            up = int(v.get("up", 0))
            down = int(v.get("down", 0))
            total = up + down
            if name not in node_hour_map:
                node_hour_map[name] = {"name": name, "up": 0, "down": 0, "total": 0, "hours_map": {}}
            node_hour_map[name]["up"] += up
            node_hour_map[name]["down"] += down
            node_hour_map[name]["total"] += total
            hm = node_hour_map[name]["hours_map"]
            if hour_label not in hm:
                hm[hour_label] = {"hour": hour_label, "up": 0, "down": 0, "total": 0}
            hm[hour_label]["up"] += up
            hm[hour_label]["down"] += down
            hm[hour_label]["total"] += total

        prev = cur

    nodes = []
    for node in node_hour_map.values():
        hours = list(node["hours_map"].values())
        hours.sort(key=lambda x: x["hour"])
        for h in hours:
            h["up_human"] = human_bytes(h["up"])
            h["down_human"] = human_bytes(h["down"])
            h["total_human"] = human_bytes(h["total"])
        peak_hour = max(hours, key=lambda x: x["total"]) if hours else None
        valley_hour = min(hours, key=lambda x: x["total"]) if hours else None

        nodes.append({
            "name": node["name"],
            "up": node["up"],
            "down": node["down"],
            "total": node["total"],
            "up_human": human_bytes(node["up"]),
            "down_human": human_bytes(node["down"]),
            "total_human": human_bytes(node["total"]),
            "hours": hours,
            "peak_hour": peak_hour,
            "valley_hour": valley_hour,
        })

    nodes.sort(key=lambda x: (x["total"], x["down"], x["up"], x["name"].lower()), reverse=True)
    return {
        "date": yday.strftime("%Y-%m-%d"),
        "from": datetime.fromtimestamp(from_ts, TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "to": datetime.fromtimestamp(to_ts, TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "nodes": nodes,
        "top_nodes": nodes[: max(0, int(TOP_N))],
    }


def build_today_hourly_by_node_summary() -> dict:
    """
    基于 samples.json 统计“今天 00:00~当前”各节点小时级走势。
    """
    ensure_dirs()
    take_sample_if_due(force=True)

    td = today_date()
    from_ts = int(start_of_day(td).timestamp())
    now_ts = int(time.time())

    data = load_samples()
    samples = sorted(data.get("samples", []), key=lambda x: int(x.get("ts", 0)))
    if len(samples) < 2:
        return {
            "date": td.strftime("%Y-%m-%d"),
            "error": "insufficient_samples",
            "message": "采样点不足，无法计算今天节点小时级分布。",
        }

    prev = None
    for s0 in samples:
        ts0 = int(s0.get("ts", 0))
        if ts0 <= from_ts:
            prev = s0
        else:
            break
    if prev is None:
        prev = samples[0]

    node_hour_map: dict[str, dict] = {}
    for cur in samples:
        cur_ts = int(cur.get("ts", 0))
        if cur_ts <= int(prev.get("ts", 0)):
            continue
        if cur_ts < from_ts:
            prev = cur
            continue

        deltas, _warnings = compute_strict_sample_delta_from_maps(cur.get("nodes", {}), prev.get("nodes", {}))
        hour_label = datetime.fromtimestamp(cur_ts, TZ).strftime("%Y-%m-%d %H:00")

        for v in deltas.values():
            name = v.get("name", "")
            up = int(v.get("up", 0))
            down = int(v.get("down", 0))
            total = up + down
            if name not in node_hour_map:
                node_hour_map[name] = {"name": name, "up": 0, "down": 0, "total": 0, "hours_map": {}}
            node_hour_map[name]["up"] += up
            node_hour_map[name]["down"] += down
            node_hour_map[name]["total"] += total
            hm = node_hour_map[name]["hours_map"]
            if hour_label not in hm:
                hm[hour_label] = {"hour": hour_label, "up": 0, "down": 0, "total": 0}
            hm[hour_label]["up"] += up
            hm[hour_label]["down"] += down
            hm[hour_label]["total"] += total

        prev = cur

    nodes = []
    for node in node_hour_map.values():
        hours = list(node["hours_map"].values())
        hours.sort(key=lambda x: x["hour"])
        for h in hours:
            h["up_human"] = human_bytes(h["up"])
            h["down_human"] = human_bytes(h["down"])
            h["total_human"] = human_bytes(h["total"])
        peak_hour = max(hours, key=lambda x: x["total"]) if hours else None
        valley_hour = min(hours, key=lambda x: x["total"]) if hours else None

        nodes.append({
            "name": node["name"],
            "up": node["up"],
            "down": node["down"],
            "total": node["total"],
            "up_human": human_bytes(node["up"]),
            "down_human": human_bytes(node["down"]),
            "total_human": human_bytes(node["total"]),
            "hours": hours,
            "peak_hour": peak_hour,
            "valley_hour": valley_hour,
        })

    nodes.sort(key=lambda x: (x["total"], x["down"], x["up"], x["name"].lower()), reverse=True)
    return {
        "date": td.strftime("%Y-%m-%d"),
        "from": datetime.fromtimestamp(from_ts, TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "to": datetime.fromtimestamp(now_ts, TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "nodes": nodes,
        "top_nodes": nodes[: max(0, int(TOP_N))],
    }


def load_ai_pack_cache() -> dict:
    return load_json(AI_PACK_CACHE_PATH, {"created_at": 0, "pack": {}})


def save_ai_pack_cache(pack: dict):
    save_json_atomic(AI_PACK_CACHE_PATH, {"created_at": int(time.time()), "pack": pack})


def get_ai_data_pack_cached() -> dict:
    if AI_PACK_CACHE_TTL_SECONDS <= 0:
        return build_ai_data_pack()

    cache = load_ai_pack_cache()
    created_at = int(cache.get("created_at", 0))
    now_ts = int(time.time())
    if created_at > 0 and now_ts - created_at <= AI_PACK_CACHE_TTL_SECONDS:
        pack = cache.get("pack") or {}
        if isinstance(pack, dict) and pack:
            return pack

    pack = build_ai_data_pack()
    save_ai_pack_cache(pack)
    return pack


def build_last_7_days_summary() -> dict:
    """
    使用 history.json + 月归档，构造最近 7 天总量按日汇总，
    并提供按节点累计排行，便于回答“7天哪台机器流量最高”。
    """
    ensure_dirs()
    td = today_date()
    days = []
    node_totals: dict[str, dict] = {}

    for i in range(7, 0, -1):
        d = td - timedelta(days=i)
        summed = history_sum(d, d)
        total_up = 0
        total_down = 0
        day_nodes = []

        for uuid, v in summed.items():
            name = v.get("name", uuid)
            up = int(v.get("up", 0))
            down = int(v.get("down", 0))
            total = up + down
            total_up += up
            total_down += down
            day_nodes.append({
                "name": name,
                "up": up,
                "down": down,
                "total": total,
                "up_human": human_bytes(up),
                "down_human": human_bytes(down),
                "total_human": human_bytes(total),
            })

            if name not in node_totals:
                node_totals[name] = {"name": name, "up": 0, "down": 0, "total": 0}
            node_totals[name]["up"] += up
            node_totals[name]["down"] += down
            node_totals[name]["total"] += total

        day_nodes.sort(key=lambda x: (x["total"], x["down"], x["up"], x["name"].lower()), reverse=True)
        total = total_up + total_down
        days.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "total_up": total_up,
                "total_down": total_down,
                "total": total,
                "total_up_human": human_bytes(total_up),
                "total_down_human": human_bytes(total_down),
                "total_human": human_bytes(total),
                "nodes": day_nodes,
            }
        )

    node_totals_list = []
    for v in node_totals.values():
        node_totals_list.append({
            "name": v["name"],
            "up": v["up"],
            "down": v["down"],
            "total": v["total"],
            "up_human": human_bytes(v["up"]),
            "down_human": human_bytes(v["down"]),
            "total_human": human_bytes(v["total"]),
        })
    node_totals_list.sort(key=lambda x: (x["total"], x["down"], x["up"], x["name"].lower()), reverse=True)

    return {
        "days": days,
        "node_totals": node_totals_list,
        "top_nodes": node_totals_list[: max(0, int(TOP_N))],
    }

def build_ai_data_pack() -> dict:
    """
    汇总一份给 AI 用的数据包。
    """
    now = now_dt()
    pack: dict = {
        "now": now.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "stat_tz": STAT_TZ,
    }

    try:
        pack["today"] = build_today_delta_struct()
    except Exception:
        logging.exception("build_today_delta_struct error")
        pack["today"] = {"error": "failed"}

    try:
        pack["last_24h"] = build_records_summary(24)
    except Exception:
        logging.exception("build_records_summary(24) error")
        pack["last_24h"] = {"error": "failed"}

    try:
        pack["last_1h_by_node"] = get_last_hours_nodes_struct(1)
    except Exception:
        logging.exception("get_last_hours_nodes_struct(1) error")
        pack["last_1h_by_node"] = {"error": "failed"}

    try:
        pack["last_24h_hourly"] = build_last_24h_hourly_summary()
    except Exception:
        logging.exception("build_last_24h_hourly_summary error")
        pack["last_24h_hourly"] = {"error": "failed"}

    try:
        pack["today_hourly_by_node"] = build_today_hourly_by_node_summary()
    except Exception:
        logging.exception("build_today_hourly_by_node_summary error")
        pack["today_hourly_by_node"] = {"error": "failed"}

    try:
        pack["yesterday_hourly_by_node"] = build_yesterday_hourly_by_node_summary()
    except Exception:
        logging.exception("build_yesterday_hourly_by_node_summary error")
        pack["yesterday_hourly_by_node"] = {"error": "failed"}

    try:
        pack["last_7d"] = build_records_summary(168)
    except Exception:
        logging.exception("build_records_summary(168) error")
        pack["last_7d"] = {"error": "failed"}

    try:
        pack["last_30d"] = build_records_summary(720)
    except Exception:
        logging.exception("build_records_summary(720) error")
        pack["last_30d"] = {"error": "failed"}

    return pack


def format_report(title: str, period_label: str, deltas: dict, reset_warnings: list[str], skipped: list[str] | None = None, include_top: bool = True) -> str:
    skipped = skipped or []

    lines = [f"📊 <b>{title}</b>（{period_label}）", ""]
    total_up = 0
    total_down = 0

    items = sorted(deltas.values(), key=lambda x: (x.get("name") or "").lower())
    for it in items:
        total_up += int(it["up"])
        total_down += int(it["down"])
        lines.append(
            f"🖥 <b>{it['name']}</b>\n"
            f"⬇️ 下行：{human_bytes(it['down'])}\n"
            f"⬆️ 上行：{human_bytes(it['up'])}\n"
        )

    lines.append("——")
    lines.append(f"📦 <b>总下行</b>：{human_bytes(total_down)}")
    lines.append(f"📦 <b>总上行</b>：{human_bytes(total_up)}")
    lines.append(f"📦 <b>总合计</b>：{human_bytes(total_down + total_up)}")

    if include_top:
        lines.append("")
        lines.append(f"🔥 <b>Top {TOP_N} 消耗榜</b>（上下行合计）")
        lines.extend(top_lines(deltas, n=TOP_N))

    if skipped:
        lines.append("")
        lines.append("⚠️ <b>以下节点因异常被跳过</b>：")
        lines.append("、".join(skipped[:30]) + ("……" if len(skipped) > 30 else ""))

    if reset_warnings:
        lines.append("")
        lines.append("⚠️ <b>检测到计数器可能重置</b>（已兜底）：")
        lines.append("、".join(reset_warnings))

    return "\n".join(lines)


def send_top_only(period_label: str, deltas: dict, reset_warnings: list[str], skipped: list[str] | None = None):
    skipped = skipped or []
    lines = [f"🔥 <b>Top {TOP_N} 消耗榜</b>（上下行合计）", f"⏱ {period_label}", ""]
    lines.extend(top_lines(deltas, n=TOP_N))

    if skipped:
        lines.append("")
        lines.append("⚠️ <b>以下节点因异常被跳过</b>：")
        lines.append("、".join(skipped[:30]) + ("……" if len(skipped) > 30 else ""))

    if reset_warnings:
        lines.append("")
        lines.append("⚠️ <b>检测到计数器可能重置</b>（已兜底）：")
        lines.append("、".join(reset_warnings))

    telegram_send("\n".join(lines))


# -------------------- 历史数据：热存储 + 冷归档（gzip） --------------------

def archive_path_for_month(ym: str) -> str:
    return os.path.join(DATA_DIR, f"history-{ym}.json.gz")


def load_archive_month(ym: str) -> dict:
    path = archive_path_for_month(ym)
    if not os.path.exists(path):
        return {"days": {}}
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def save_archive_month(ym: str, data: dict):
    path = archive_path_for_month(ym)
    tmp = path + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def history_append(day_str: str, deltas: dict):
    hist = load_json(HISTORY_PATH, {"days": {}})
    hist.setdefault("days", {})
    hist["days"][day_str] = deltas
    save_json_atomic(HISTORY_PATH, hist)


def archive_and_prune_history():
    ensure_dirs()
    hist = load_json(HISTORY_PATH, {"days": {}})
    days: dict = hist.get("days", {})
    if not days:
        return

    today = today_date()
    hot_cut = today - timedelta(days=HISTORY_HOT_DAYS)
    retention_cut = today - timedelta(days=HISTORY_RETENTION_DAYS)

    to_keep = {}
    to_archive_by_month: dict[str, dict] = {}

    for k, v in days.items():
        try:
            d = datetime.strptime(k, "%Y-%m-%d").date()
        except Exception:
            continue

        if d < retention_cut:
            continue

        if d < hot_cut:
            ym = yyyymm(d)
            to_archive_by_month.setdefault(ym, {})
            to_archive_by_month[ym][k] = v
        else:
            to_keep[k] = v

    for ym, month_days in to_archive_by_month.items():
        arc = load_archive_month(ym)
        arc.setdefault("days", {})
        arc["days"].update(month_days)

        pruned = {}
        for dk, dv in arc["days"].items():
            try:
                dd = datetime.strptime(dk, "%Y-%m-%d").date()
            except Exception:
                continue
            if dd >= retention_cut:
                pruned[dk] = dv
        arc["days"] = pruned
        save_archive_month(ym, arc)

    save_json_atomic(HISTORY_PATH, {"days": to_keep})


def history_sum(from_day: date, to_day: date) -> dict:
    ensure_dirs()
    summed = {}
    hot = load_json(HISTORY_PATH, {"days": {}}).get("days", {})

    def add_one_day(one: dict):
        for uuid, v in one.items():
            if uuid not in summed:
                summed[uuid] = {"name": v.get("name", uuid), "up": 0, "down": 0}
            summed[uuid]["up"] += int(v.get("up", 0))
            summed[uuid]["down"] += int(v.get("down", 0))

    d = from_day
    while d <= to_day:
        key = d.strftime("%Y-%m-%d")
        if key in hot:
            add_one_day(hot.get(key, {}))
        else:
            ym = yyyymm(d)
            arc = load_archive_month(ym).get("days", {})
            add_one_day(arc.get(key, {}))
        d += timedelta(days=1)

    return summed


# -------------------- Baseline（按 tag） --------------------

def load_baselines():
    base = load_json(BASELINES_PATH, {"baselines": {}})
    if "version" not in base:
        base["version"] = 1
    base.setdefault("baselines", {})
    return base


def save_baseline(tag: str, nodes: dict):
    base = load_baselines()
    base["baselines"][tag] = {
        "nodes": nodes,
        "ts": now_dt().strftime("%Y-%m-%d %H:%M:%S %Z"),
    }
    save_json_atomic(BASELINES_PATH, base)


def _iter_day_baselines_sorted(base: dict):
    entries = []
    for tag, item in base.get("baselines", {}).items():
        try:
            d = datetime.strptime(tag, "%Y-%m-%d").date()
        except Exception:
            continue
        entries.append((d, tag, item))
    entries.sort(key=lambda x: x[0])
    return entries


def rebuild_period_baselines(since_day: date | None = None, dry_run: bool = False) -> tuple[int, int, int]:
    """
    从日基线重建 WEEK-/MONTH- 基线。
    returns: (daily_count, week_rebuilt, month_rebuilt)
    """
    ensure_dirs()
    base = load_baselines()
    entries = _iter_day_baselines_sorted(base)
    if since_day is None:
        since_day = date(2026, 2, 1)

    week_count = 0
    month_count = 0
    daily_count = 0
    for d, _tag, item in entries:
        if d < since_day:
            continue
        daily_count += 1
        nodes = item.get("nodes", {})
        ts = item.get("ts", now_dt().strftime("%Y-%m-%d %H:%M:%S %Z"))

        if d == start_of_week(d):
            week_tag = f"WEEK-{d.strftime('%Y-%m-%d')}"
            if not dry_run:
                base["baselines"][week_tag] = {"nodes": nodes, "ts": ts}
            week_count += 1

        if d == start_of_month(d):
            month_tag = f"MONTH-{d.strftime('%Y-%m-%d')}"
            if not dry_run:
                base["baselines"][month_tag] = {"nodes": nodes, "ts": ts}
            month_count += 1

    if not dry_run:
        save_json_atomic(BASELINES_PATH, base)
    return daily_count, week_count, month_count


def get_baseline_nodes(tag: str) -> dict | None:
    base = load_baselines()
    b = base.get("baselines", {}).get(tag)
    if not b:
        return None
    return b.get("nodes", {})


def set_baseline_to_current(tag: str):
    ensure_dirs()
    current, _skipped = fetch_nodes_and_totals()
    save_baseline(tag, build_nodes_map_from_current(current))


# -------------------- 采样器（用于 /top Nh） --------------------

def load_samples():
    return load_json(SAMPLES_PATH, {"samples": []})


def save_samples(data: dict):
    save_json_atomic(SAMPLES_PATH, data)


def prune_samples(samples: list, now_ts: int):
    keep_after = now_ts - SAMPLE_RETENTION_HOURS * 3600
    pruned = [s for s in samples if int(s.get("ts", 0)) >= keep_after]
    pruned.sort(key=lambda x: int(x.get("ts", 0)))
    return pruned


def take_sample_if_due(force: bool = False):
    """
    由 bot 循环周期性调用：最多每 SAMPLE_INTERVAL_SECONDS 采样一次
    """
    ensure_dirs()
    data = load_samples()
    samples = data.get("samples", [])
    now_ts = int(time.time())

    last_ts = int(samples[-1]["ts"]) if samples else 0
    if (not force) and last_ts and (now_ts - last_ts < SAMPLE_INTERVAL_SECONDS):
        return

    current, skipped = fetch_nodes_and_totals()
    nodes_map = build_nodes_map_from_current(current)

    samples.append({"ts": now_ts, "nodes": nodes_map, "skipped": skipped})
    samples = prune_samples(samples, now_ts)
    save_samples({"samples": samples})


def sample_worker_loop():
    logging.info("sample worker started, interval=%ss", SAMPLE_INTERVAL_SECONDS)
    while not SAMPLE_STOP_EVENT.is_set():
        try:
            take_sample_if_due(force=False)
        except Exception:
            logging.exception("sample worker error")
        SAMPLE_STOP_EVENT.wait(timeout=max(1, SAMPLE_INTERVAL_SECONDS))
    logging.info("sample worker stopped")


def start_sample_worker():
    global SAMPLE_THREAD
    if SAMPLE_THREAD and SAMPLE_THREAD.is_alive():
        return
    SAMPLE_STOP_EVENT.clear()
    SAMPLE_THREAD = threading.Thread(target=sample_worker_loop, name="sample-worker", daemon=True)
    SAMPLE_THREAD.start()


def stop_sample_worker():
    SAMPLE_STOP_EVENT.set()
    if SAMPLE_THREAD and SAMPLE_THREAD.is_alive():
        SAMPLE_THREAD.join(timeout=3)


def get_sample_at_or_before(target_ts: int):
    data = load_samples()
    samples = data.get("samples", [])
    if not samples:
        return None

    lo, hi = 0, len(samples) - 1
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        ts = int(samples[mid].get("ts", 0))
        if ts <= target_ts:
            best = samples[mid]
            lo = mid + 1
        else:
            hi = mid - 1
    return best


# -------------------- 报表任务 --------------------

def run_daily_send_yesterday():
    """
    每天 00:00：发送昨日日报；写入 history；归档；并写入“今日起点 baseline(YYYY-MM-DD)”
    """
    ensure_dirs()
    yday = today_date() - timedelta(days=1)
    yday_label = yday.strftime("%Y-%m-%d")

    baseline_nodes = get_baseline_nodes(yday_label)
    current, skipped = fetch_nodes_and_totals()

    if baseline_nodes is None:
        save_baseline(yday_label, build_nodes_map_from_current(current))
        telegram_send(
            f"⚠️ <b>日报基线缺失</b>（{yday_label}）。\n"
            f"我已把当前累计保存为该日基线。\n"
            f"从下一次 00:00 开始日报将稳定正常。"
        )
        return

    deltas, new_baseline, reset_warnings = compute_delta_from_nodes(current, baseline_nodes)
    telegram_send(format_report("昨日流量日报", yday_label, deltas, reset_warnings, skipped=skipped, include_top=True))

    history_append(yday_label, deltas)
    archive_and_prune_history()

    today_label = today_date().strftime("%Y-%m-%d")
    save_baseline(today_label, new_baseline)


def run_weekly_send_last_week():
    ensure_dirs()
    today = today_date()
    this_week_start = start_of_week(today)
    last_week_end = this_week_start - timedelta(days=1)
    last_week_start = last_week_end - timedelta(days=6)

    summed = history_sum(last_week_start, last_week_end)
    label = f"{last_week_start.strftime('%Y-%m-%d')} → {last_week_end.strftime('%Y-%m-%d')}"
    telegram_send(format_report("上周流量周报", label, summed, [], skipped=[], include_top=True))


def run_monthly_send_last_month():
    ensure_dirs()
    today = today_date()
    this_month_start = start_of_month(today)
    last_month_end = this_month_start - timedelta(days=1)
    last_month_start = date(last_month_end.year, last_month_end.month, 1)

    summed = history_sum(last_month_start, last_month_end)
    label = f"{last_month_start.strftime('%Y-%m-%d')} → {last_month_end.strftime('%Y-%m-%d')}"
    telegram_send(format_report("上月流量月报", label, summed, [], skipped=[], include_top=True))


def run_period_report(from_dt: datetime, to_dt: datetime, tag: str, top_only: bool = False):
    ensure_dirs()
    baseline_nodes = get_baseline_nodes(tag)
    if baseline_nodes is None:
        set_baseline_to_current(tag)
        telegram_send(
            f"⚠️ 当前没有找到 起点快照（{tag}）。\n"
            f"我已把现在的累计值保存为新的起点。\n"
            f"请稍后再发一次命令查看稳定统计。"
        )
        return

    current, skipped = fetch_nodes_and_totals()
    deltas, _new_base, reset_warnings = compute_delta_from_nodes(current, baseline_nodes)
    period_label = f"{from_dt.strftime('%Y-%m-%d %H:%M')} → {to_dt.strftime('%Y-%m-%d %H:%M')}"

    if top_only:
        send_top_only(period_label, deltas, reset_warnings, skipped=skipped)
    else:
        telegram_send(format_report("流量统计", period_label, deltas, reset_warnings, skipped=skipped, include_top=True))


def run_top_last_hours(hours: int):
    """
    /top Nh：最近 N 小时 Top 榜（合计）
    依赖 samples.json（bot 周期采样）
    """
    ensure_dirs()
    if hours <= 0:
        telegram_send("用法：/top 6h（N>0）")
        return

    # 采最新 sample
    take_sample_if_due(force=True)

    now_ts = int(time.time())
    target_ts = now_ts - hours * 3600
    base = get_sample_at_or_before(target_ts)
    if base is None:
        telegram_send(
            "⚠️ 还没有足够的采样历史来计算这个时间范围。\n"
            f"请保持 bot 服务运行一段时间后再试：/top {hours}h"
        )
        return

    data = load_samples()
    samples = data.get("samples", [])
    if not samples:
        telegram_send("⚠️ 采样数据为空，请稍后再试。")
        return

    cur = samples[-1]
    deltas, reset_warnings = compute_delta_from_maps(cur.get("nodes", {}), base.get("nodes", {}))
    skipped = list(dict.fromkeys((base.get("skipped", []) or []) + (cur.get("skipped", []) or [])))

    from_dt = datetime.fromtimestamp(int(base["ts"]), TZ)
    to_dt = datetime.fromtimestamp(int(cur["ts"]), TZ)
    label = f"{from_dt.strftime('%Y-%m-%d %H:%M')} → {to_dt.strftime('%Y-%m-%d %H:%M')}"
    send_top_only(label, deltas, reset_warnings, skipped=skipped)


def bootstrap_period_baselines():
    ensure_dirs()
    td = today_date()
    ws = start_of_week(td)
    ms = start_of_month(td)

    set_baseline_to_current(f"WEEK-{ws.strftime('%Y-%m-%d')}")
    set_baseline_to_current(f"MONTH-{ms.strftime('%Y-%m-%d')}")
    telegram_send("✅ 已建立本周 / 本月起点快照：现在可直接用 /week /month /top week /top month")


def history_has_existing_data_risk() -> tuple[bool, str]:
    hist = load_json(HISTORY_PATH, {"days": {}})
    days = hist.get("days", {})
    valid_days = 0
    for k in days.keys():
        try:
            datetime.strptime(k, "%Y-%m-%d")
            valid_days += 1
        except Exception:
            continue
    if valid_days >= 7:
        return True, f"history.json 中已有 {valid_days} 天记录"

    archives = list(filter(None, [
        p if re.fullmatch(r"history-\d{4}-\d{2}\.json\.gz", p) else None
        for p in os.listdir(DATA_DIR)
    ])) if os.path.isdir(DATA_DIR) else []
    if archives:
        return True, f"存在历史月归档文件 {archives[0]} 等"

    return False, ""


def load_confirm_state() -> dict:
    return load_json(TG_CONFIRM_PATH, {"actions": {}})


def save_confirm_state(data: dict):
    save_json_atomic(TG_CONFIRM_PATH, data)


def set_confirm_action(chat_id: str, action: str, ttl_seconds: int = 600):
    data = load_confirm_state()
    data.setdefault("actions", {})
    code = str(secrets.randbelow(9000) + 1000)
    expires_at = int(time.time()) + ttl_seconds
    key = f"{chat_id}:{action}"
    data["actions"][key] = {"code": code, "expires_at": expires_at}
    save_confirm_state(data)
    return code, expires_at


def consume_confirm_action(chat_id: str, action: str, code: str) -> bool:
    data = load_confirm_state()
    key = f"{chat_id}:{action}"
    item = data.get("actions", {}).get(key)
    if not item:
        return False
    now_ts = int(time.time())
    ok = (str(item.get("code", "")) == str(code).strip()) and (int(item.get("expires_at", 0)) >= now_ts)
    if ok:
        data["actions"].pop(key, None)
        save_confirm_state(data)
        return True
    return False


def parse_chat_ids_env(name: str, default_value: str) -> list[str]:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        raw = str(default_value)
    ids = [x.strip() for x in raw.split(",") if x.strip()]
    return ids


def is_allowed_chat(chat_id: str) -> bool:
    allowed = parse_chat_ids_env("TELEGRAM_ALLOWED_CHAT_IDS", str(TELEGRAM_CHAT_ID))
    return str(chat_id) in allowed


def is_admin(chat_id: str) -> bool:
    admins = parse_chat_ids_env("TELEGRAM_ADMIN_CHAT_IDS", str(TELEGRAM_CHAT_ID))
    return str(chat_id) in admins


# -------------------- Telegram 命令监听 --------------------

def get_updates(offset: int | None):
    """
    Telegram long polling 在公网环境下偶发被对端 reset 是正常的。
    这里做：网络错误自动重试 + 轻量退避，避免刷屏告警/频繁重连。
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params = {"timeout": 50}
    if offset is not None:
        params["offset"] = offset

    # 最多重试 5 次：总等待 ~ (1+2+4+8+16)s + 抖动
    backoff = 1.0
    last_exc = None
    for _ in range(5):
        try:
            r = requests.get(url, params=params, timeout=55)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 409:
                if should_alert("tg_409", 600):
                    safe_telegram_send("⚠️ Telegram 409 Conflict：可能有另一份实例在拉 getUpdates，请确认旧环境是否已停止。")
            raise
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ChunkedEncodingError) as e:
            last_exc = e
            time.sleep(backoff + random.random())
            backoff = min(backoff * 2, 20.0)
            continue

    # 连续失败才抛出，让外层限频告警接管
    raise last_exc


def load_offset() -> int | None:
    try:
        with open(TG_OFFSET_PATH, "r", encoding="utf-8") as f:
            s = f.read().strip()
            return int(s) if s else None
    except FileNotFoundError:
        return None
    except Exception:
        return None


def save_offset(val: int):
    with open(TG_OFFSET_PATH, "w", encoding="utf-8") as f:
        f.write(str(val))


def parse_top_scope(text: str):
    """
    /top
    /top today|week|month
    /top 6h /top 24h /top 168h
    """
    parts = text.strip().split()
    if len(parts) == 1:
        return ("today", None)

    arg = parts[1].strip().lower()
    if arg in ("today", "t"):
        return ("today", None)
    if arg in ("week", "w"):
        return ("week", None)
    if arg in ("month", "m"):
        return ("month", None)

    m = re.fullmatch(r"(\d+)\s*h", arg)
    if m:
        return ("hours", int(m.group(1)))

    return ("unknown", None)


def listen_commands():
    ensure_dirs()
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 未设置")

    logging.info("Komari traffic bot starting (stat_tz=%s)", STAT_TZ)
    if BOT_START_NOTIFY and should_alert("bot_start", 60):
        instance_label = BOT_INSTANCE_NAME or "default"
        allowed_chats = parse_chat_ids_env("TELEGRAM_ALLOWED_CHAT_IDS", str(TELEGRAM_CHAT_ID))
        safe_telegram_send(
            "✅ Komari traffic bot 已启动\n"
            f"🧩 实例：{instance_label}\n"
            f"🕒 统计时区：{STAT_TZ}\n"
            f"💬 可接收命令 chat 数：{len(allowed_chats)}"
        )
    offset = load_offset()

    # 启动先采一次样
    try:
        take_sample_if_due(force=True)
    except Exception:
        pass
    start_sample_worker()

    while True:
        if SHUTTING_DOWN:
            logging.warning("shutdown flag set, exiting listen loop")
            stop_sample_worker()
            return
        try:
            data = get_updates(offset)
            if not data.get("ok"):
                time.sleep(3)
                continue

            for upd in data.get("result", []):
                update_id = upd.get("update_id")
                if update_id is not None:
                    offset = update_id + 1

                msg = upd.get("message") or upd.get("edited_message")
                if not msg:
                    continue

                chat = msg.get("chat", {})
                chat_id = str(chat.get("id", ""))
                if not is_allowed_chat(chat_id):
                    continue

                text = (msg.get("text") or "").strip()
                if not text.startswith("/"):
                    continue

                parts = text.split()
                cmd = parts[0].lower()
                arg_text = parts[1] if len(parts) > 1 else ""

                now = now_dt()
                td = today_date()

                if cmd == "/today":
                    tag = td.strftime("%Y-%m-%d")
                    run_period_report(start_of_day(td), now, tag, top_only=False)

                elif cmd == "/week":
                    ws = start_of_week(td)
                    tag = f"WEEK-{ws.strftime('%Y-%m-%d')}"
                    run_period_report(start_of_day(ws), now, tag, top_only=False)

                elif cmd == "/month":
                    ms = start_of_month(td)
                    tag = f"MONTH-{ms.strftime('%Y-%m-%d')}"
                    run_period_report(start_of_day(ms), now, tag, top_only=False)

                elif cmd == "/top":
                    scope, hours = parse_top_scope(text)
                    if scope == "today":
                        tag = td.strftime("%Y-%m-%d")
                        run_period_report(start_of_day(td), now, tag, top_only=True)
                    elif scope == "week":
                        ws = start_of_week(td)
                        tag = f"WEEK-{ws.strftime('%Y-%m-%d')}"
                        run_period_report(start_of_day(ws), now, tag, top_only=True)
                    elif scope == "month":
                        ms = start_of_month(td)
                        tag = f"MONTH-{ms.strftime('%Y-%m-%d')}"
                        run_period_report(start_of_day(ms), now, tag, top_only=True)
                    elif scope == "hours":
                        run_top_last_hours(int(hours or 0))
                    else:
                        telegram_send("用法：/top  或  /top today|week|month  或  /top 6h")

                elif cmd == "/archive":
                    if not is_admin(chat_id):
                        telegram_send("⛔ 无权限")
                        continue
                    code, _ = set_confirm_action(chat_id, "archive")
                    telegram_send(
                        "⚠️ 准备执行 archive（归档 + 清理 history 热数据）。\n"
                        f"当前时间：{now.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
                        f"如需继续，请发送：/confirm_archive {code}"
                    )

                elif cmd == "/bootstrap":
                    if not is_admin(chat_id):
                        telegram_send("⛔ 无权限")
                        continue
                    risk, reason = history_has_existing_data_risk()
                    if risk:
                        telegram_send(
                            "⛔ 检测到已有历史数据，拒绝执行 bootstrap。\n"
                            f"原因：{reason}\n"
                            "请使用 /rebuild_baselines 或手动脚本修复。"
                        )
                        continue
                    code, _ = set_confirm_action(chat_id, "bootstrap")
                    telegram_send(
                        "⚠️ 准备执行 bootstrap（重建本周/本月起点快照）。\n"
                        f"当前时间：{now.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
                        "建议仅在新部署、无历史时使用。\n"
                        f"如需继续，请发送：/confirm_bootstrap {code}"
                    )

                elif cmd == "/rebuild_baselines":
                    if not is_admin(chat_id):
                        telegram_send("⛔ 无权限")
                        continue
                    code, _ = set_confirm_action(chat_id, "rebuild_baselines")
                    telegram_send(
                        "⚠️ 准备执行 rebuild_baselines（从日基线重建 WEEK/MONTH 起点）。\n"
                        f"当前时间：{now.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
                        f"如需继续，请发送：/confirm_rebuild_baselines {code}"
                    )

                elif cmd == "/confirm_archive":
                    if not is_admin(chat_id):
                        telegram_send("⛔ 无权限")
                        continue
                    code = arg_text.strip()
                    if not consume_confirm_action(chat_id, "archive", code):
                        telegram_send("❌ 确认码无效或已过期")
                        continue
                    archive_and_prune_history()
                    telegram_send("✅ 已执行历史归档压缩")

                elif cmd == "/confirm_bootstrap":
                    if not is_admin(chat_id):
                        telegram_send("⛔ 无权限")
                        continue
                    code = arg_text.strip()
                    if not consume_confirm_action(chat_id, "bootstrap", code):
                        telegram_send("❌ 确认码无效或已过期")
                        continue
                    risk, reason = history_has_existing_data_risk()
                    if risk:
                        telegram_send(
                            "⛔ 确认阶段检测到已有历史数据，拒绝执行 bootstrap。\n"
                            f"原因：{reason}\n"
                            "请使用 /rebuild_baselines。"
                        )
                        continue
                    bootstrap_period_baselines()

                elif cmd == "/confirm_rebuild_baselines":
                    if not is_admin(chat_id):
                        telegram_send("⛔ 无权限")
                        continue
                    code = arg_text.strip()
                    if not consume_confirm_action(chat_id, "rebuild_baselines", code):
                        telegram_send("❌ 确认码无效或已过期")
                        continue
                    daily_count, week_count, month_count = rebuild_period_baselines()
                    telegram_send(
                        "✅ 已从日基线重建 WEEK-/MONTH- 基线（>= 2026-02-01）\n"
                        f"扫描日基线：{daily_count}，重建 WEEK：{week_count}，重建 MONTH：{month_count}"
                    )

                elif cmd in ("/ask", "/ai"):
                    question = text.partition(" ")[2].strip()
                    if not question:
                        telegram_send(
                            "用法：/ask 你的问题\n"
                            "示例：\n"
                            "/ask 今天哪个节点最耗流量？\n"
                            "/ask 最近 7 天流量大概是上升还是下降趋势？\n"
                            "/ask 帮我写一段今天流量情况的总结，适合发到群里。"
                        )
                        continue

                    if question_requires_fresh_ai_pack(question):
                        data_pack = build_ai_data_pack()
                    else:
                        data_pack = get_ai_data_pack_cached()
                    answer = ask_ai_with_data(question, data_pack)
                    telegram_send(normalize_ai_answer_for_telegram(answer))

                elif cmd in ("/help", "/start"):
                    telegram_send(
                        "可用命令：\n"
                        "/today  /week  /month\n"
                        "/top  (默认 today)\n"
                        "/top today|week|month\n"
                        "/top 6h（任意Nh）\n"
                        "/ask 你的问题（或 /ai）\n"
                        "管理员：/archive /bootstrap /rebuild_baselines\n"
                        "确认命令：/confirm_archive /confirm_bootstrap /confirm_rebuild_baselines"
                    )

            if offset is not None:
                save_offset(offset)

        except Exception as e:
            if should_alert("listen", 300):
                alert_exception("listen_loop", "listen", e)
            logging.exception("listen loop error")
            time.sleep(3)

# -------------------- main --------------------

def main():
    if len(sys.argv) < 2:
        raise RuntimeError("Usage: report_daily | report_weekly | report_monthly | listen | bootstrap [--force] | rebuild-baselines [--dry-run] [--since YYYY-MM-DD] | health | config-validate")

    cmd = sys.argv[1].strip().lower()

    if cmd == "report_daily":
        run_daily_send_yesterday()
        return 0
    if cmd == "report_weekly":
        run_weekly_send_last_week()
        return 0
    if cmd == "report_monthly":
        run_monthly_send_last_month()
        return 0
    if cmd == "listen":
        listen_commands()
        return 0
    if cmd == "bootstrap":
        force = any(arg.strip().lower() == "--force" for arg in sys.argv[2:])
        risk, reason = history_has_existing_data_risk()
        if risk and not force:
            raise RuntimeError(
                "检测到已有历史数据，拒绝执行 bootstrap。"
                f"原因：{reason}。"
                "如确认要继续，请使用：bootstrap --force"
            )
        bootstrap_period_baselines()
        return 0
    if cmd == "rebuild-baselines":
        dry_run = False
        since_day = None
        i = 2
        while i < len(sys.argv):
            arg = sys.argv[i].strip().lower()
            if arg == "--dry-run":
                dry_run = True
                i += 1
                continue
            if arg == "--since":
                if i + 1 >= len(sys.argv):
                    raise RuntimeError("--since requires YYYY-MM-DD")
                try:
                    since_day = parse_date_yyyy_mm_dd(sys.argv[i + 1].strip())
                except Exception as e:
                    raise RuntimeError(f"invalid --since date: {sys.argv[i + 1]} ({e})")
                i += 2
                continue
            raise RuntimeError(f"Unknown rebuild-baselines arg: {sys.argv[i]}")

        daily_count, week_count, month_count = rebuild_period_baselines(since_day=since_day, dry_run=dry_run)
        mode = "DRY-RUN" if dry_run else "APPLY"
        since_text = since_day.strftime("%Y-%m-%d") if since_day else "2026-02-01"
        print(
            f"OK {mode} rebuilt baselines from daily snapshots (>= {since_text}): "
            f"days={daily_count}, week={week_count}, month={month_count}"
        )
        return 0
    if cmd == "config-validate":
        validate_config_or_raise()
        print("OK")
        return 0
    if cmd == "health":
        validate_config_or_raise()
        run_healthcheck_or_raise()
        print("OK")
        return 0

    raise RuntimeError("Unknown command")


if __name__ == "__main__":
    setup_logging()
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)
    full_cmd = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "(none)"
    try:
        sys.exit(main())
    except Exception as e:
        key = f"cmd_{(sys.argv[1] if len(sys.argv) > 1 else 'none')}"
        if should_alert(key, 300):
            alert_exception("main", full_cmd, e)
        raise
