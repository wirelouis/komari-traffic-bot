#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import os
import re
import sys
import time
import traceback
import socket
import gzip
import concurrent.futures
import signal
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

# /top Nh 依赖采样快照：bot 运行时自动采样
SAMPLE_INTERVAL_SECONDS = int(os.environ.get("SAMPLE_INTERVAL_SECONDS", "300"))  # 默认 5 分钟
SAMPLE_RETENTION_HOURS = int(os.environ.get("SAMPLE_RETENTION_HOURS", "720"))    # 默认保留 30 天采样

BASELINES_PATH = os.path.join(DATA_DIR, "baselines.json")
HISTORY_PATH = os.path.join(DATA_DIR, "history.json")
SAMPLES_PATH = os.path.join(DATA_DIR, "samples.json")
TG_OFFSET_PATH = os.path.join(DATA_DIR, "tg_offset.txt")

TIMEOUT = int(os.environ.get("KOMARI_TIMEOUT_SECONDS", "15"))  # Komari API timeout（秒）

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.environ.get("LOG_FILE", "").strip()

SHUTTING_DOWN = False


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

    for p in [BASELINES_PATH, HISTORY_PATH, SAMPLES_PATH, TG_OFFSET_PATH]:
        if os.path.exists(p):
            try:
                if p == TG_OFFSET_PATH:
                    _ = load_offset()
                else:
                    load_json(p, {})
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
    for u in units:
        if x < 1024 or u == units[-1]:
            return f"{x:.2f} {u}" if u != "B" else f"{int(x)} B"
        x /= 1024
    return f"{x:.2f} PiB"


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


def save_json_atomic(path: str, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


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
    return load_json(BASELINES_PATH, {"baselines": {}})


def save_baseline(tag: str, nodes: dict):
    base = load_baselines()
    base.setdefault("baselines", {})
    base["baselines"][tag] = {
        "nodes": nodes,
        "ts": now_dt().strftime("%Y-%m-%d %H:%M:%S %Z"),
    }
    save_json_atomic(BASELINES_PATH, base)


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
    td = today_date()
    ws = start_of_week(td)
    ms = start_of_month(td)

    set_baseline_to_current(f"WEEK-{ws.strftime('%Y-%m-%d')}")
    set_baseline_to_current(f"MONTH-{ms.strftime('%Y-%m-%d')}")
    telegram_send("✅ 已建立本周 / 本月起点快照：现在可直接用 /week /month /top week /top month")


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
            r = HTTP_SESSION.get(url, params=params, timeout=TIMEOUT + 60)
            r.raise_for_status()
            return r.json()
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
    offset = load_offset()

    # 启动先采一次样
    try:
        take_sample_if_due(force=True)
    except Exception:
        pass

    while True:
        if SHUTTING_DOWN:
            logging.warning("shutdown flag set, exiting listen loop")
            return
        try:
            # 周期采样：哪怕没人发命令，也会积累 /top Nh 所需数据
            try:
                take_sample_if_due(force=False)
            except Exception:
                pass

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
                if chat_id != str(TELEGRAM_CHAT_ID):
                    continue

                text = (msg.get("text") or "").strip()
                if not text.startswith("/"):
                    continue

                now = now_dt()
                td = today_date()

                if text.startswith("/today"):
                    tag = td.strftime("%Y-%m-%d")
                    run_period_report(start_of_day(td), now, tag, top_only=False)

                elif text.startswith("/week"):
                    ws = start_of_week(td)
                    tag = f"WEEK-{ws.strftime('%Y-%m-%d')}"
                    run_period_report(start_of_day(ws), now, tag, top_only=False)

                elif text.startswith("/month"):
                    ms = start_of_month(td)
                    tag = f"MONTH-{ms.strftime('%Y-%m-%d')}"
                    run_period_report(start_of_day(ms), now, tag, top_only=False)

                elif text.startswith("/top"):
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

                elif text.startswith("/archive"):
                    archive_and_prune_history()
                    telegram_send("✅ 已执行历史归档压缩")

                elif text.startswith("/help") or text.startswith("/start"):
                    telegram_send(
                        "可用命令：\n"
                        "/today  /week  /month\n"
                        "/top  (默认 today)\n"
                        "/top today|week|month\n"
                        "/top 6h（任意Nh）\n"
                        "管理员：/archive；初始化：运行 bootstrap"
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
        raise RuntimeError("Usage: report_daily | report_weekly | report_monthly | listen | bootstrap | health | config-validate")

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
        bootstrap_period_baselines()
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
