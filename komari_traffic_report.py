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
import html
import concurrent.futures
import signal
import secrets
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo

import requests
import random
import sqlite3
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


def parse_bool_value(value, default: bool = False) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text == "":
        return default
    if text in ("1", "true", "yes", "on", "y"):
        return True
    if text in ("0", "false", "no", "off", "n"):
        return False
    raise RuntimeError(f"invalid boolean value: {value}")


def parse_bool_env(name: str, default: bool = False) -> bool:
    return parse_bool_value(os.environ.get(name), default=default)


def parse_bytes_value(value, name: str = "value") -> int:
    if value is None:
        return 0
    text = str(value).strip()
    if not text:
        return 0

    m = re.fullmatch(r"(?i)(\d+(?:\.\d+)?)\s*([kmgtp]?i?b?|b)?", text)
    if not m:
        raise RuntimeError(f"{name} must be bytes or KiB/MiB/GiB/TiB, got: {value}")

    number = float(m.group(1))
    unit = (m.group(2) or "b").lower()
    unit_map = {
        "": 1,
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "kib": 1024,
        "m": 1024 ** 2,
        "mb": 1024 ** 2,
        "mib": 1024 ** 2,
        "g": 1024 ** 3,
        "gb": 1024 ** 3,
        "gib": 1024 ** 3,
        "t": 1024 ** 4,
        "tb": 1024 ** 4,
        "tib": 1024 ** 4,
        "p": 1024 ** 5,
        "pb": 1024 ** 5,
        "pib": 1024 ** 5,
    }
    if unit not in unit_map:
        raise RuntimeError(f"{name} has unsupported unit: {value}")
    return max(0, int(number * unit_map[unit]))


def bytes_config_text(value: int) -> str:
    n = int(value or 0)
    return "" if n <= 0 else human_bytes(n)


def parse_bytes_env(name: str) -> int:
    return parse_bytes_value(os.environ.get(name, ""), name=name)


def validate_silence_windows_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for part in re.split(r"[,;]\s*", text):
        if not part:
            continue
        m = re.fullmatch(r"(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})", part.strip())
        if not m:
            raise RuntimeError(f"ALERT_SILENCE_WINDOWS invalid segment: {part}")
        sh, sm, eh, em = [int(x) for x in m.groups()]
        if not (0 <= sh <= 23 and 0 <= eh <= 23 and 0 <= sm <= 59 and 0 <= em <= 59):
            raise RuntimeError(f"ALERT_SILENCE_WINDOWS time out of range: {part}")
        if sh * 60 + sm == eh * 60 + em:
            raise RuntimeError(f"ALERT_SILENCE_WINDOWS empty segment: {part}")
    return text


STAT_TZ = os.environ.get("STAT_TZ", "Asia/Shanghai")


def load_stat_timezone(name: str):
    try:
        return ZoneInfo(name)
    except Exception:
        if name in ("Asia/Shanghai", "Asia/Chongqing", "Asia/Harbin"):
            return timezone(timedelta(hours=8), name)
        if name.upper() in ("UTC", "Etc/UTC"):
            return timezone.utc
        raise


TZ = load_stat_timezone(STAT_TZ)  # 统计时区

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
ALERTS_STATE_PATH = os.path.join(DATA_DIR, "alerts_state.json")
REPORT_SCHEDULES_PATH = os.path.join(DATA_DIR, "report_schedules.json")
TRAFFIC_DB_PATH = os.path.join(DATA_DIR, "traffic.db")

TIMEOUT = int(os.environ.get("KOMARI_TIMEOUT_SECONDS", "15"))  # Komari API timeout（秒）

APP_VERSION = os.environ.get("APP_VERSION", "dev").strip() or "dev"
GIT_COMMIT = os.environ.get("GIT_COMMIT", "").strip()
BUILD_DATE = os.environ.get("BUILD_DATE", "").strip()
IMAGE_SOURCE = os.environ.get("IMAGE_SOURCE", "ghcr.io/wirelouis/komari-traffic-bot").strip()
TASK_RUN_RETENTION_DAYS = max(0, int(os.environ.get("TASK_RUN_RETENTION_DAYS", "90")))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.environ.get("LOG_FILE", "").strip()

BOT_INSTANCE_NAME = os.environ.get("BOT_INSTANCE_NAME", "").strip()
BOT_START_NOTIFY = parse_bool_env("BOT_START_NOTIFY", True)
AI_PACK_CACHE_TTL_SECONDS = max(0, int(os.environ.get("AI_PACK_CACHE_TTL_SECONDS", "3600")))

ALERTS_ENABLED = parse_bool_env("ALERTS_ENABLED", True)
TELEGRAM_ALERT_CHAT_ID = os.environ.get("TELEGRAM_ALERT_CHAT_ID", "").strip()
ALERT_COOLDOWN_SECONDS = int(os.environ.get("ALERT_COOLDOWN_SECONDS", "1800"))
ALERT_SILENCE_WINDOWS = os.environ.get("ALERT_SILENCE_WINDOWS", "").strip()
ALERT_NODE_MISSING_SAMPLES = int(os.environ.get("ALERT_NODE_MISSING_SAMPLES", "2"))
ALERT_WINDOW_MINUTES = int(os.environ.get("ALERT_WINDOW_MINUTES", "60"))
ALERT_TOTAL_WINDOW_BYTES = parse_bytes_env("ALERT_TOTAL_WINDOW_BYTES")
ALERT_NODE_WINDOW_BYTES = parse_bytes_env("ALERT_NODE_WINDOW_BYTES")
ALERT_DAILY_TOTAL_BYTES = parse_bytes_env("ALERT_DAILY_TOTAL_BYTES")
ALERT_DAILY_NODE_BYTES = parse_bytes_env("ALERT_DAILY_NODE_BYTES")
ALERT_RECOVERY_NOTIFY = parse_bool_env("ALERT_RECOVERY_NOTIFY", True)

SHUTTING_DOWN = False
SAMPLE_THREAD: threading.Thread | None = None
SAMPLE_STOP_EVENT = threading.Event()
SCHEDULER_THREAD: threading.Thread | None = None
SCHEDULER_STOP_EVENT = threading.Event()


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


def _require_non_negative_int(name: str, value: int):
    if value < 0:
        raise RuntimeError(f"{name} must be >= 0")


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
    _require_non_negative_int("ALERT_COOLDOWN_SECONDS", ALERT_COOLDOWN_SECONDS)
    _require_positive_int("ALERT_NODE_MISSING_SAMPLES", ALERT_NODE_MISSING_SAMPLES)
    _require_positive_int("ALERT_WINDOW_MINUTES", ALERT_WINDOW_MINUTES)
    for name, value in (
        ("ALERT_TOTAL_WINDOW_BYTES", ALERT_TOTAL_WINDOW_BYTES),
        ("ALERT_NODE_WINDOW_BYTES", ALERT_NODE_WINDOW_BYTES),
        ("ALERT_DAILY_TOTAL_BYTES", ALERT_DAILY_TOTAL_BYTES),
        ("ALERT_DAILY_NODE_BYTES", ALERT_DAILY_NODE_BYTES),
    ):
        _require_non_negative_int(name, value)
    if TELEGRAM_ALERT_CHAT_ID and any(ch.isspace() for ch in TELEGRAM_ALERT_CHAT_ID):
        raise RuntimeError("TELEGRAM_ALERT_CHAT_ID must be a single chat id without whitespace")
    parse_silence_windows(ALERT_SILENCE_WINDOWS)


def run_healthcheck_or_raise():
    ensure_dirs()

    test_path = os.path.join(DATA_DIR, ".health_write_test")
    try:
        with open(test_path, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(test_path)
    except Exception as e:
        raise RuntimeError(f"DATA_DIR not writable: {DATA_DIR}: {e}")

    for p in [BASELINES_PATH, HISTORY_PATH, SAMPLES_PATH, TG_OFFSET_PATH, TG_CONFIRM_PATH, ALERTS_STATE_PATH]:
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


def runtime_config_path() -> str:
    return os.path.join(DATA_DIR, "runtime_config.json")


def _parse_editable_int(payload: dict, key: str, current: int, min_value: int, max_value: int) -> int:
    if key not in payload:
        return int(current)
    try:
        value = int(payload.get(key))
    except Exception:
        raise RuntimeError(f"{key} must be an integer")
    if value < min_value or value > max_value:
        raise RuntimeError(f"{key} must be between {min_value} and {max_value}")
    return value


def _parse_editable_bool(payload: dict, key: str, current: bool) -> bool:
    if key not in payload:
        return bool(current)
    return parse_bool_value(payload.get(key), default=bool(current))


def _parse_editable_text(payload: dict, key: str, current: str, max_len: int = 240) -> str:
    if key not in payload:
        return str(current or "").strip()
    return str(payload.get(key) or "").strip()[:max_len]


def _parse_editable_bytes(payload: dict, key: str, current: int, max_value: int = 1024 ** 6) -> int:
    if key not in payload:
        return int(current)
    value = parse_bytes_value(payload.get(key), name=key)
    if value < 0 or value > max_value:
        raise RuntimeError(f"{key} must be between 0 and {human_bytes(max_value)}")
    return value


def validate_runtime_config(payload: dict) -> dict:
    payload = payload if isinstance(payload, dict) else {}
    instance_name = str(payload.get("bot_instance_name", BOT_INSTANCE_NAME) or "").strip()[:80]
    komari_base_url = _parse_editable_text(payload, "komari_base_url", KOMARI_BASE_URL).rstrip("/")
    telegram_chat_id = _parse_editable_text(payload, "telegram_chat_id", TELEGRAM_CHAT_ID, 120)
    telegram_alert_chat_id = _parse_editable_text(payload, "telegram_alert_chat_id", TELEGRAM_ALERT_CHAT_ID, 120)
    ai_api_base = _parse_editable_text(payload, "ai_api_base", AI_API_BASE).rstrip("/")
    ai_model = _parse_editable_text(payload, "ai_model", AI_MODEL, 120)
    for key, value in (("telegram_chat_id", telegram_chat_id), ("telegram_alert_chat_id", telegram_alert_chat_id)):
        if value and any(ch.isspace() for ch in value):
            raise RuntimeError(f"{key} must be a single chat id without whitespace")
    return {
        "bot_instance_name": instance_name,
        "komari_base_url": komari_base_url,
        "telegram_chat_id": telegram_chat_id,
        "telegram_alert_chat_id": telegram_alert_chat_id,
        "ai_api_base": ai_api_base,
        "ai_model": ai_model,
        "top_n": _parse_editable_int(payload, "top_n", TOP_N, 1, 50),
        "komari_timeout_seconds": _parse_editable_int(payload, "komari_timeout_seconds", TIMEOUT, 3, 120),
        "komari_fetch_workers": _parse_editable_int(payload, "komari_fetch_workers", KOMARI_FETCH_WORKERS, 1, 32),
        "sample_interval_seconds": _parse_editable_int(payload, "sample_interval_seconds", SAMPLE_INTERVAL_SECONDS, 60, 3600),
        "sample_retention_hours": _parse_editable_int(payload, "sample_retention_hours", SAMPLE_RETENTION_HOURS, 1, 168),
        "ai_pack_cache_ttl_seconds": _parse_editable_int(payload, "ai_pack_cache_ttl_seconds", AI_PACK_CACHE_TTL_SECONDS, 0, 86400),
        "task_run_retention_days": _parse_editable_int(payload, "task_run_retention_days", TASK_RUN_RETENTION_DAYS, 0, 3650),
        "alerts_enabled": _parse_editable_bool(payload, "alerts_enabled", ALERTS_ENABLED),
        "alert_recovery_notify": _parse_editable_bool(payload, "alert_recovery_notify", ALERT_RECOVERY_NOTIFY),
        "alert_cooldown_seconds": _parse_editable_int(payload, "alert_cooldown_seconds", ALERT_COOLDOWN_SECONDS, 0, 86400),
        "alert_window_minutes": _parse_editable_int(payload, "alert_window_minutes", ALERT_WINDOW_MINUTES, 5, 1440),
        "alert_node_missing_samples": _parse_editable_int(payload, "alert_node_missing_samples", ALERT_NODE_MISSING_SAMPLES, 1, 20),
        "alert_silence_windows": validate_silence_windows_text(payload.get("alert_silence_windows", ALERT_SILENCE_WINDOWS)),
        "alert_total_window_bytes": _parse_editable_bytes(payload, "alert_total_window_bytes", ALERT_TOTAL_WINDOW_BYTES),
        "alert_node_window_bytes": _parse_editable_bytes(payload, "alert_node_window_bytes", ALERT_NODE_WINDOW_BYTES),
        "alert_daily_total_bytes": _parse_editable_bytes(payload, "alert_daily_total_bytes", ALERT_DAILY_TOTAL_BYTES),
        "alert_daily_node_bytes": _parse_editable_bytes(payload, "alert_daily_node_bytes", ALERT_DAILY_NODE_BYTES),
    }


def apply_runtime_config(config: dict) -> dict:
    global BOT_INSTANCE_NAME, TOP_N, TIMEOUT, KOMARI_FETCH_WORKERS, SAMPLE_INTERVAL_SECONDS, SAMPLE_RETENTION_HOURS
    global AI_PACK_CACHE_TTL_SECONDS, TASK_RUN_RETENTION_DAYS
    global KOMARI_BASE_URL, TELEGRAM_CHAT_ID, TELEGRAM_ALERT_CHAT_ID, AI_API_BASE, AI_MODEL
    global ALERTS_ENABLED, ALERT_RECOVERY_NOTIFY, ALERT_COOLDOWN_SECONDS, ALERT_WINDOW_MINUTES, ALERT_NODE_MISSING_SAMPLES
    global ALERT_SILENCE_WINDOWS, ALERT_TOTAL_WINDOW_BYTES, ALERT_NODE_WINDOW_BYTES, ALERT_DAILY_TOTAL_BYTES, ALERT_DAILY_NODE_BYTES
    clean = validate_runtime_config(config)
    BOT_INSTANCE_NAME = clean["bot_instance_name"]
    KOMARI_BASE_URL = clean["komari_base_url"]
    TELEGRAM_CHAT_ID = clean["telegram_chat_id"]
    TELEGRAM_ALERT_CHAT_ID = clean["telegram_alert_chat_id"]
    AI_API_BASE = clean["ai_api_base"]
    AI_MODEL = clean["ai_model"]
    TOP_N = clean["top_n"]
    TIMEOUT = clean["komari_timeout_seconds"]
    KOMARI_FETCH_WORKERS = clean["komari_fetch_workers"]
    SAMPLE_INTERVAL_SECONDS = clean["sample_interval_seconds"]
    SAMPLE_RETENTION_HOURS = clean["sample_retention_hours"]
    AI_PACK_CACHE_TTL_SECONDS = clean["ai_pack_cache_ttl_seconds"]
    TASK_RUN_RETENTION_DAYS = clean["task_run_retention_days"]
    ALERTS_ENABLED = clean["alerts_enabled"]
    ALERT_RECOVERY_NOTIFY = clean["alert_recovery_notify"]
    ALERT_COOLDOWN_SECONDS = clean["alert_cooldown_seconds"]
    ALERT_WINDOW_MINUTES = clean["alert_window_minutes"]
    ALERT_NODE_MISSING_SAMPLES = clean["alert_node_missing_samples"]
    ALERT_SILENCE_WINDOWS = clean["alert_silence_windows"]
    ALERT_TOTAL_WINDOW_BYTES = clean["alert_total_window_bytes"]
    ALERT_NODE_WINDOW_BYTES = clean["alert_node_window_bytes"]
    ALERT_DAILY_TOTAL_BYTES = clean["alert_daily_total_bytes"]
    ALERT_DAILY_NODE_BYTES = clean["alert_daily_node_bytes"]
    return clean


def load_runtime_config() -> dict:
    stored = load_json(runtime_config_path(), {})
    if isinstance(stored, dict) and isinstance(stored.get("config"), dict):
        return validate_runtime_config(stored.get("config", {}))
    return validate_runtime_config(stored if isinstance(stored, dict) else {})


def save_runtime_config(config: dict) -> dict:
    ensure_dirs()
    clean = apply_runtime_config(config)
    save_json_atomic(runtime_config_path(), {"version": 1, "config": clean, "updated_at": int(time.time())})
    return clean


def current_runtime_config() -> dict:
    stored = load_json(runtime_config_path(), {})
    stored_config = stored.get("config", {}) if isinstance(stored, dict) else {}
    clean = validate_runtime_config({
        "bot_instance_name": stored_config.get("bot_instance_name", BOT_INSTANCE_NAME),
        "komari_base_url": stored_config.get("komari_base_url", KOMARI_BASE_URL),
        "telegram_chat_id": stored_config.get("telegram_chat_id", TELEGRAM_CHAT_ID),
        "telegram_alert_chat_id": stored_config.get("telegram_alert_chat_id", TELEGRAM_ALERT_CHAT_ID),
        "ai_api_base": stored_config.get("ai_api_base", AI_API_BASE),
        "ai_model": stored_config.get("ai_model", AI_MODEL),
        "top_n": stored_config.get("top_n", TOP_N),
        "komari_timeout_seconds": stored_config.get("komari_timeout_seconds", TIMEOUT),
        "komari_fetch_workers": stored_config.get("komari_fetch_workers", KOMARI_FETCH_WORKERS),
        "sample_interval_seconds": stored_config.get("sample_interval_seconds", SAMPLE_INTERVAL_SECONDS),
        "sample_retention_hours": stored_config.get("sample_retention_hours", SAMPLE_RETENTION_HOURS),
        "ai_pack_cache_ttl_seconds": stored_config.get("ai_pack_cache_ttl_seconds", AI_PACK_CACHE_TTL_SECONDS),
        "task_run_retention_days": stored_config.get("task_run_retention_days", TASK_RUN_RETENTION_DAYS),
        "alerts_enabled": stored_config.get("alerts_enabled", ALERTS_ENABLED),
        "alert_recovery_notify": stored_config.get("alert_recovery_notify", ALERT_RECOVERY_NOTIFY),
        "alert_cooldown_seconds": stored_config.get("alert_cooldown_seconds", ALERT_COOLDOWN_SECONDS),
        "alert_window_minutes": stored_config.get("alert_window_minutes", ALERT_WINDOW_MINUTES),
        "alert_node_missing_samples": stored_config.get("alert_node_missing_samples", ALERT_NODE_MISSING_SAMPLES),
        "alert_silence_windows": stored_config.get("alert_silence_windows", ALERT_SILENCE_WINDOWS),
        "alert_total_window_bytes": stored_config.get("alert_total_window_bytes", ALERT_TOTAL_WINDOW_BYTES),
        "alert_node_window_bytes": stored_config.get("alert_node_window_bytes", ALERT_NODE_WINDOW_BYTES),
        "alert_daily_total_bytes": stored_config.get("alert_daily_total_bytes", ALERT_DAILY_TOTAL_BYTES),
        "alert_daily_node_bytes": stored_config.get("alert_daily_node_bytes", ALERT_DAILY_NODE_BYTES),
    })
    def field(key: str, label: str, field_type: str = "text", note: str = "", **extra) -> dict:
        value = clean[key]
        if field_type == "bytes":
            value = bytes_config_text(value)
        item = {"key": key, "label": label, "type": field_type, "value": value, "note": note}
        item.update(extra)
        return item

    return {
        "path": runtime_config_path(),
        "values": clean,
        "editable": [
            field("bot_instance_name", "实例名", note="用于 Web 面板和 Telegram 报表标题。", group="基础"),
            field("komari_base_url", "Komari 地址", note="只保存面板访问地址，不包含 API token。", group="基础"),
            field("telegram_chat_id", "默认推送 Chat", note="只保存 Chat ID，不包含 Bot Token。", group="基础"),
            field("telegram_alert_chat_id", "告警推送 Chat", note="留空时沿用默认推送 Chat。", group="基础"),
            field("ai_api_base", "AI 接口地址", note="只保存接口地址，不包含 API Key。", group="基础"),
            field("ai_model", "AI 模型", note="例如 gpt-5.4-mini，可直接显示和修改。", group="基础"),
            field("top_n", "Top 节点数", "number", "影响 Top 报表和面板排行。", min=1, max=50, group="基础"),
            field("komari_timeout_seconds", "Komari 超时（秒）", "number", "探针接口慢时可适当调大。", min=3, max=120, group="基础"),
            field("komari_fetch_workers", "节点并发数", "number", "节点很多时可适当调大，太大会增加探针压力。", min=1, max=32, group="基础"),
            field("sample_interval_seconds", "采样间隔（秒）", "number", "用于短时间 Top 和告警检测。", min=60, max=3600, group="基础"),
            field("sample_retention_hours", "短时采样保留（小时）", "number", "用于最近 Nh 查询，不影响长期 SQLite 汇总。", min=1, max=168, group="基础"),
            field("ai_pack_cache_ttl_seconds", "AI 缓存 TTL（秒）", "number", "0 表示每次实时生成。", min=0, max=86400, group="基础"),
            field("task_run_retention_days", "任务记录保留天数", "number", "0 表示关闭清理。", min=0, max=3650, group="基础"),
            field("alerts_enabled", "启用告警", "boolean", "关闭后不会产生新的告警事件。", group="告警"),
            field("alert_recovery_notify", "恢复后通知", "boolean", "异常恢复时是否发送恢复提示。", group="告警"),
            field("alert_cooldown_seconds", "重复提醒冷却（秒）", "number", "同一个异常多久后才再次提醒。", min=0, max=86400, group="告警"),
            field("alert_window_minutes", "窗口检测范围（分钟）", "number", "用于最近窗口流量阈值。", min=5, max=1440, group="告警"),
            field("alert_node_missing_samples", "节点失败次数阈值", "number", "节点连续采样失败达到这个次数才告警。", min=1, max=20, group="告警"),
            field("alert_silence_windows", "静默时段", note="格式如 23:00-07:00；多个用逗号分隔。", group="告警"),
            field("alert_total_window_bytes", "窗口总流量阈值", "bytes", "留空或 0 表示关闭；支持 MiB/GiB/TiB。", group="告警"),
            field("alert_node_window_bytes", "窗口单节点阈值", "bytes", "留空或 0 表示关闭；支持 MiB/GiB/TiB。", group="告警"),
            field("alert_daily_total_bytes", "今日总流量阈值", "bytes", "留空或 0 表示关闭；支持 MiB/GiB/TiB。", group="告警"),
            field("alert_daily_node_bytes", "今日单节点阈值", "bytes", "留空或 0 表示关闭；支持 MiB/GiB/TiB。", group="告警"),
        ],
    }


try:
    stored_runtime = load_json(runtime_config_path(), {})
    if isinstance(stored_runtime, dict) and isinstance(stored_runtime.get("config"), dict):
        apply_runtime_config(stored_runtime.get("config", {}))
except Exception:
    pass


def traffic_db_connect():
    ensure_dirs()
    conn = sqlite3.connect(TRAFFIC_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def traffic_db_session():
    conn = traffic_db_connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_traffic_db():
    with traffic_db_session() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
              version INTEGER PRIMARY KEY,
              applied_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS node_daily_usage (
              day TEXT NOT NULL,
              uuid TEXT NOT NULL,
              name TEXT NOT NULL,
              up INTEGER NOT NULL DEFAULT 0,
              down INTEGER NOT NULL DEFAULT 0,
              total INTEGER NOT NULL DEFAULT 0,
              source TEXT NOT NULL DEFAULT 'history',
              source_from TEXT NOT NULL DEFAULT '',
              source_to TEXT NOT NULL DEFAULT '',
              reset_warnings TEXT NOT NULL DEFAULT '[]',
              skipped TEXT NOT NULL DEFAULT '[]',
              updated_at INTEGER NOT NULL,
              PRIMARY KEY (day, uuid)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS period_rollups (
              period_type TEXT NOT NULL,
              period_key TEXT NOT NULL,
              uuid TEXT NOT NULL,
              name TEXT NOT NULL,
              up INTEGER NOT NULL DEFAULT 0,
              down INTEGER NOT NULL DEFAULT 0,
              total INTEGER NOT NULL DEFAULT 0,
              updated_at INTEGER NOT NULL,
              PRIMARY KEY (period_type, period_key, uuid)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS traffic_snapshots (
              ts INTEGER NOT NULL,
              uuid TEXT NOT NULL,
              name TEXT NOT NULL,
              up INTEGER NOT NULL DEFAULT 0,
              down INTEGER NOT NULL DEFAULT 0,
              skipped TEXT NOT NULL DEFAULT '[]',
              PRIMARY KEY (ts, uuid)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS task_runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              type TEXT NOT NULL,
              source TEXT NOT NULL,
              status TEXT NOT NULL,
              summary TEXT NOT NULL DEFAULT '',
              error TEXT NOT NULL DEFAULT '',
              started_at INTEGER NOT NULL,
              finished_at INTEGER NOT NULL,
              duration_ms INTEGER NOT NULL DEFAULT 0,
              metadata TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_task_runs_type_started ON task_runs(type, started_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_task_runs_started ON task_runs(started_at DESC)")
        conn.execute("INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(1, ?)", (now_dt().isoformat(),))
        conn.execute("INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(2, ?)", (now_dt().isoformat(),))


def _json_dumps_compact(data) -> str:
    try:
        return json.dumps(data if data is not None else {}, ensure_ascii=False, separators=(",", ":"))
    except TypeError:
        return json.dumps({"value": str(data)}, ensure_ascii=False, separators=(",", ":"))


def _json_loads_object(text: str) -> dict:
    try:
        value = json.loads(text or "{}")
        return value if isinstance(value, dict) else {"value": value}
    except Exception:
        return {}


def redact_sensitive_text(value) -> str:
    text = str(value or "")
    secrets_to_mask = [
        TELEGRAM_BOT_TOKEN,
        KOMARI_API_TOKEN,
        AI_API_KEY,
        TELEGRAM_CHAT_ID,
        TELEGRAM_ALERT_CHAT_ID,
    ]
    for secret in secrets_to_mask:
        secret = str(secret or "").strip()
        if not secret:
            continue
        masked = "***" if len(secret) <= 6 else f"{secret[:3]}***{secret[-3:]}"
        text = text.replace(secret, masked)
    return text


def redact_sensitive_data(value):
    if isinstance(value, dict):
        return {str(key): redact_sensitive_data(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_sensitive_data(item) for item in value]
    if isinstance(value, str):
        return redact_sensitive_text(value)
    return value


def record_task_run(
    task_type: str,
    source: str,
    status: str,
    started_at: int | float | None = None,
    finished_at: int | float | None = None,
    summary: str = "",
    error: str = "",
    metadata: dict | None = None,
) -> dict:
    init_traffic_db()
    now_raw = time.time()
    started_raw = float(started_at if started_at is not None else now_raw)
    finished_raw = float(finished_at if finished_at is not None else now_raw)
    started = int(started_raw)
    finished = int(finished_raw)
    duration_ms = max(0, int((finished_raw - started_raw) * 1000))
    task_type = str(task_type or "other").strip().lower()[:40] or "other"
    source = str(source or "unknown").strip()[:120] or "unknown"
    status = str(status or "unknown").strip().lower()[:40] or "unknown"
    summary = redact_sensitive_text(summary)[:600]
    error = redact_sensitive_text(error)[:1200]
    safe_metadata = redact_sensitive_data(metadata or {})
    metadata_json = _json_dumps_compact(safe_metadata)
    with traffic_db_session() as conn:
        cur = conn.execute(
            """
            INSERT INTO task_runs(type, source, status, summary, error, started_at, finished_at, duration_ms, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (task_type, source, status, summary, error, started, finished, duration_ms, metadata_json),
        )
        run_id = int(cur.lastrowid)
    return {
        "id": run_id,
        "type": task_type,
        "source": source,
        "status": status,
        "summary": summary,
        "error": error,
        "started_at": started,
        "finished_at": finished,
        "duration_ms": duration_ms,
        "metadata": safe_metadata,
    }


def safe_record_task_run(*args, **kwargs) -> dict | None:
    try:
        return record_task_run(*args, **kwargs)
    except Exception:
        logging.exception("failed to record task run")
        return None


def list_task_runs(limit: int = 50, task_type: str | None = None) -> list[dict]:
    init_traffic_db()
    limit = min(200, max(1, int(limit or 50)))
    task_type = str(task_type or "").strip().lower()
    params: list = []
    where = ""
    if task_type:
        where = "WHERE type = ?"
        params.append(task_type)
    params.append(limit)
    with traffic_db_session() as conn:
        rows = conn.execute(
            f"""
            SELECT id, type, source, status, summary, error, started_at, finished_at, duration_ms, metadata
            FROM task_runs
            {where}
            ORDER BY started_at DESC, id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    runs = []
    for row in rows:
        runs.append({
            "id": int(row["id"]),
            "type": row["type"],
            "source": row["source"],
            "status": row["status"],
            "summary": row["summary"],
            "error": row["error"],
            "started_at": int(row["started_at"] or 0),
            "finished_at": int(row["finished_at"] or 0),
            "duration_ms": int(row["duration_ms"] or 0),
            "metadata": _json_loads_object(row["metadata"]),
        })
    return runs


def count_task_runs(task_type: str | None = None, before_ts: int | float | None = None) -> int:
    init_traffic_db()
    task_type = str(task_type or "").strip().lower()
    clauses = []
    params: list = []
    if task_type:
        clauses.append("type = ?")
        params.append(task_type)
    if before_ts is not None:
        clauses.append("started_at < ?")
        params.append(int(before_ts))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with traffic_db_session() as conn:
        row = conn.execute(f"SELECT COUNT(*) AS c FROM task_runs {where}", params).fetchone()
    return int(row["c"] or 0)


def prune_task_runs(retention_days: int | None = None, now_ts: int | float | None = None) -> dict:
    init_traffic_db()
    days = TASK_RUN_RETENTION_DAYS if retention_days is None else int(retention_days)
    if days < 0:
        raise RuntimeError("retention_days must be >= 0")
    if days == 0:
        return {
            "enabled": False,
            "retention_days": 0,
            "cutoff": 0,
            "cutoff_text": "",
            "deleted": 0,
            "remaining": count_task_runs(),
        }
    now_value = int(now_ts if now_ts is not None else time.time())
    cutoff = now_value - days * 86400
    with traffic_db_session() as conn:
        cur = conn.execute("DELETE FROM task_runs WHERE started_at < ?", (cutoff,))
        deleted = int(cur.rowcount or 0)
        row = conn.execute("SELECT COUNT(*) AS c FROM task_runs").fetchone()
        remaining = int(row["c"] or 0)
    return {
        "enabled": True,
        "retention_days": days,
        "cutoff": cutoff,
        "cutoff_text": datetime.fromtimestamp(cutoff, TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "deleted": deleted,
        "remaining": remaining,
    }


def traffic_db_table_counts() -> dict:
    init_traffic_db()
    tables = ("node_daily_usage", "period_rollups", "traffic_snapshots", "task_runs")
    counts = {}
    with traffic_db_session() as conn:
        for table in tables:
            row = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
            counts[table] = int(row["c"] or 0)
    return counts


def traffic_db_maintenance_status(retention_days: int | None = None, now_ts: int | float | None = None) -> dict:
    init_traffic_db()
    days = TASK_RUN_RETENTION_DAYS if retention_days is None else int(retention_days)
    if days < 0:
        raise RuntimeError("retention_days must be >= 0")
    now_value = int(now_ts if now_ts is not None else time.time())
    cutoff = now_value - days * 86400 if days else 0
    size = os.path.getsize(TRAFFIC_DB_PATH) if os.path.exists(TRAFFIC_DB_PATH) else 0
    counts = traffic_db_table_counts()
    return {
        "retention_days": days,
        "retention_enabled": days > 0,
        "cutoff": cutoff,
        "cutoff_text": datetime.fromtimestamp(cutoff, TZ).strftime("%Y-%m-%d %H:%M:%S %Z") if cutoff else "",
        "old_task_runs": count_task_runs(before_ts=cutoff) if cutoff else 0,
        "task_runs": counts.get("task_runs", 0),
        "table_counts": counts,
        "db_size": size,
        "db_size_human": human_bytes(size),
    }


def vacuum_traffic_db() -> dict:
    init_traffic_db()
    before = os.path.getsize(TRAFFIC_DB_PATH) if os.path.exists(TRAFFIC_DB_PATH) else 0
    conn = traffic_db_connect()
    try:
        conn.execute("VACUUM")
        conn.commit()
    finally:
        conn.close()
    after = os.path.getsize(TRAFFIC_DB_PATH) if os.path.exists(TRAFFIC_DB_PATH) else 0
    return {
        "before_size": before,
        "after_size": after,
        "before_size_human": human_bytes(before),
        "after_size_human": human_bytes(after),
        "saved_bytes": max(0, before - after),
        "saved_human": human_bytes(max(0, before - after)),
        "table_counts": traffic_db_table_counts(),
    }


def latest_task_run(task_type: str | None = None, source_prefix: str | None = None, metadata_key: str | None = None, metadata_value=None) -> dict | None:
    runs = list_task_runs(limit=200, task_type=task_type)
    for run in runs:
        if source_prefix and not str(run.get("source", "")).startswith(source_prefix):
            continue
        if metadata_key:
            if str((run.get("metadata") or {}).get(metadata_key, "")) != str(metadata_value):
                continue
        return run
    return None


def run_with_task_record(task_type: str, source: str, func, summary_func=None, metadata: dict | None = None):
    started = time.time()
    try:
        result = func()
        summary = ""
        if summary_func:
            summary = str(summary_func(result) or "")
        elif isinstance(result, dict):
            summary = str(result.get("summary") or result.get("label") or "")
        run = safe_record_task_run(
            task_type,
            source,
            "success",
            started_at=started,
            finished_at=time.time(),
            summary=summary,
            metadata=metadata or {},
        )
        if isinstance(result, dict) and run:
            result = dict(result)
            result["task_run"] = run
        return result
    except Exception as exc:
        safe_record_task_run(
            task_type,
            source,
            "failed",
            started_at=started,
            finished_at=time.time(),
            summary="",
            error=str(exc),
            metadata=metadata or {},
        )
        raise


def upsert_daily_usage(day_str: str, deltas: dict, source: str = "history", source_from: str = "", source_to: str = "", reset_warnings: list[str] | None = None, skipped: list[str] | None = None):
    if not deltas:
        return
    init_traffic_db()
    updated_at = int(time.time())
    reset_json = json.dumps(reset_warnings or [], ensure_ascii=False)
    skipped_json = json.dumps(skipped or [], ensure_ascii=False)
    rows = []
    for uuid, item in deltas.items():
        up = max(0, int(item.get("up", 0) or 0))
        down = max(0, int(item.get("down", 0) or 0))
        rows.append((
            day_str,
            str(uuid),
            str(item.get("name") or uuid),
            up,
            down,
            up + down,
            source,
            source_from,
            source_to,
            reset_json,
            skipped_json,
            updated_at,
        ))
    with traffic_db_session() as conn:
        conn.executemany(
            """
            INSERT INTO node_daily_usage(day, uuid, name, up, down, total, source, source_from, source_to, reset_warnings, skipped, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(day, uuid) DO UPDATE SET
              name=excluded.name,
              up=excluded.up,
              down=excluded.down,
              total=excluded.total,
              source=excluded.source,
              source_from=excluded.source_from,
              source_to=excluded.source_to,
              reset_warnings=excluded.reset_warnings,
              skipped=excluded.skipped,
              updated_at=excluded.updated_at
            """,
            rows,
        )


def traffic_db_has_day(day_str: str) -> bool:
    init_traffic_db()
    with traffic_db_session() as conn:
        row = conn.execute("SELECT 1 FROM node_daily_usage WHERE day = ? LIMIT 1", (day_str,)).fetchone()
    return row is not None


def aggregate_daily_usage(from_day: date, to_day: date) -> dict:
    init_traffic_db()
    start = from_day.strftime("%Y-%m-%d")
    end = to_day.strftime("%Y-%m-%d")
    with traffic_db_session() as conn:
        rows = conn.execute(
            """
            SELECT uuid, COALESCE(NULLIF(name, ''), uuid) AS name, SUM(up) AS up, SUM(down) AS down
            FROM node_daily_usage
            WHERE day >= ? AND day <= ?
            GROUP BY uuid
            """,
            (start, end),
        ).fetchall()
    result = {}
    for row in rows:
        result[str(row["uuid"])] = {
            "name": row["name"] or row["uuid"],
            "up": int(row["up"] or 0),
            "down": int(row["down"] or 0),
        }
    return result


def _traffic_node_rows_from_map(nodes_map: dict) -> list[dict]:
    rows = []
    for uuid, item in nodes_map.items():
        up = max(0, int(item.get("up", 0) or 0))
        down = max(0, int(item.get("down", 0) or 0))
        total = up + down
        rows.append({
            "uuid": str(uuid),
            "name": str(item.get("name") or uuid),
            "up": up,
            "down": down,
            "total": total,
            "up_human": human_bytes(up),
            "down_human": human_bytes(down),
            "total_human": human_bytes(total),
        })
    rows.sort(key=lambda item: (item["total"], item["down"], item["up"], item["name"].lower()), reverse=True)
    return rows


def _traffic_total_from_rows(rows: list[dict]) -> dict:
    up = sum(int(item.get("up", 0) or 0) for item in rows)
    down = sum(int(item.get("down", 0) or 0) for item in rows)
    total = up + down
    return {
        "up": up,
        "down": down,
        "total": total,
        "up_human": human_bytes(up),
        "down_human": human_bytes(down),
        "total_human": human_bytes(total),
    }


def _traffic_group_key(day_value: date, group: str) -> tuple[str, str]:
    if group == "weekly":
        start = start_of_week(day_value)
        end = start + timedelta(days=6)
        return start.strftime("%Y-%m-%d"), f"{start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}"
    if group == "monthly":
        return yyyymm(day_value), yyyymm(day_value)
    return day_value.strftime("%Y-%m-%d"), day_value.strftime("%Y-%m-%d")


def traffic_range_summary(from_day: date, to_day: date, group: str = "daily") -> dict:
    if from_day > to_day:
        raise RuntimeError("from must be <= to")
    group = str(group or "daily").strip().lower()
    if group not in ("daily", "weekly", "monthly"):
        raise RuntimeError("group must be daily, weekly, or monthly")

    ensure_dirs()
    migrate_history_to_traffic_db()
    start = from_day.strftime("%Y-%m-%d")
    end = to_day.strftime("%Y-%m-%d")
    with traffic_db_session() as conn:
        rows = conn.execute(
            """
            SELECT day, uuid, COALESCE(NULLIF(name, ''), uuid) AS name, SUM(up) AS up, SUM(down) AS down
            FROM node_daily_usage
            WHERE day >= ? AND day <= ?
            GROUP BY day, uuid
            ORDER BY day ASC, (SUM(up) + SUM(down)) DESC
            """,
            (start, end),
        ).fetchall()

    total_nodes: dict[str, dict] = {}
    group_nodes: dict[str, dict] = {}
    group_labels: dict[str, str] = {}
    covered_days = set()
    for row in rows:
        day_text = str(row["day"])
        try:
            row_day = parse_date_yyyy_mm_dd(day_text)
        except Exception:
            continue
        covered_days.add(day_text)
        uuid = str(row["uuid"])
        name = str(row["name"] or uuid)
        up = int(row["up"] or 0)
        down = int(row["down"] or 0)

        if uuid not in total_nodes:
            total_nodes[uuid] = {"name": name, "up": 0, "down": 0}
        total_nodes[uuid]["up"] += up
        total_nodes[uuid]["down"] += down
        total_nodes[uuid]["name"] = name or total_nodes[uuid].get("name") or uuid

        key, label = _traffic_group_key(row_day, group)
        group_labels[key] = label
        bucket = group_nodes.setdefault(key, {})
        if uuid not in bucket:
            bucket[uuid] = {"name": name, "up": 0, "down": 0}
        bucket[uuid]["up"] += up
        bucket[uuid]["down"] += down
        bucket[uuid]["name"] = name or bucket[uuid].get("name") or uuid

    node_rows = _traffic_node_rows_from_map(total_nodes)
    groups = []
    for key in sorted(group_nodes.keys()):
        rows_for_group = _traffic_node_rows_from_map(group_nodes[key])
        groups.append({
            "key": key,
            "label": group_labels.get(key, key),
            "nodes": rows_for_group,
            "top_nodes": rows_for_group[: max(0, int(TOP_N))],
            "total": _traffic_total_from_rows(rows_for_group),
        })

    return {
        "from": start,
        "to": end,
        "group": group,
        "days": sorted(covered_days),
        "day_count": len(covered_days),
        "nodes": node_rows,
        "top_nodes": node_rows[: max(0, int(TOP_N))],
        "total": _traffic_total_from_rows(node_rows),
        "groups": groups,
        "source": "traffic_db",
    }


def migrate_history_to_traffic_db():
    init_traffic_db()
    hot = load_json(HISTORY_PATH, {"days": {}}).get("days", {})
    for day_str, deltas in (hot or {}).items():
        if isinstance(deltas, dict):
            upsert_daily_usage(day_str, deltas, source="history_json")
    if not os.path.isdir(DATA_DIR):
        return
    for filename in os.listdir(DATA_DIR):
        if not re.fullmatch(r"history-\d{4}-\d{2}\.json\.gz", filename):
            continue
        ym = filename.removeprefix("history-").removesuffix(".json.gz")
        try:
            arc = load_archive_month(ym).get("days", {})
        except Exception:
            continue
        for day_str, deltas in (arc or {}).items():
            if isinstance(deltas, dict):
                upsert_daily_usage(day_str, deltas, source="history_archive")


def default_report_schedules() -> dict:
    return {"version": 1, "schedules": [], "last_runs": {}}


def _parse_hhmm(value: str) -> tuple[int, int]:
    text = str(value or "").strip()
    match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", text)
    if not match:
        raise RuntimeError("time must be HH:mm")
    return int(match.group(1)), int(match.group(2))


def normalize_report_schedule(item: dict) -> dict:
    scope = str(item.get("scope") or "daily").strip().lower()
    if scope not in ("daily", "weekly", "monthly"):
        scope = "daily"
    mode = str(item.get("mode") or "full").strip().lower()
    if mode not in ("full", "top"):
        mode = "full"
    schedule_id = str(item.get("id") or secrets.token_urlsafe(8)).strip()
    time_text = str(item.get("time") or "09:00").strip()
    _parse_hhmm(time_text)
    weekday = min(6, max(0, int(item.get("weekday", 0) or 0)))
    month_day = min(31, max(1, int(item.get("month_day", 1) or 1)))
    return {
        "id": schedule_id,
        "enabled": bool(item.get("enabled", True)),
        "scope": scope,
        "mode": mode,
        "time": time_text,
        "weekday": weekday,
        "month_day": month_day,
        "chat": str(item.get("chat") or "").strip(),
        "updated_at": int(item.get("updated_at") or int(time.time())),
    }


def validate_report_schedule(item: dict) -> dict:
    scope = str(item.get("scope") or "").strip().lower()
    if scope not in ("daily", "weekly", "monthly"):
        raise RuntimeError("scope must be daily, weekly, or monthly")
    mode = str(item.get("mode") or "full").strip().lower()
    if mode not in ("full", "top"):
        raise RuntimeError("mode must be full or top")
    _parse_hhmm(str(item.get("time") or ""))
    raw_weekday = item.get("weekday", 0)
    weekday = int(raw_weekday if raw_weekday not in (None, "") else 0)
    if weekday < 0 or weekday > 6:
        raise RuntimeError("weekday must be 0-6")
    raw_month_day = item.get("month_day", 1)
    month_day = int(raw_month_day if raw_month_day not in (None, "") else 1)
    if month_day < 1 or month_day > 31:
        raise RuntimeError("month_day must be 1-31")
    payload = dict(item)
    payload["scope"] = scope
    payload["mode"] = mode
    payload["weekday"] = weekday
    payload["month_day"] = month_day
    payload["enabled"] = bool(item.get("enabled", True))
    payload["updated_at"] = int(time.time())
    return normalize_report_schedule(payload)


def load_report_schedules() -> dict:
    data = load_json(REPORT_SCHEDULES_PATH, default_report_schedules())
    schedules = data.get("schedules", []) if isinstance(data, dict) else []
    last_runs = data.get("last_runs", {}) if isinstance(data, dict) else {}
    if not isinstance(schedules, list):
        schedules = []
    if not isinstance(last_runs, dict):
        last_runs = {}
    return {
        "version": 1,
        "schedules": [normalize_report_schedule(item) for item in schedules if isinstance(item, dict)],
        "last_runs": last_runs,
    }


def save_report_schedules(data: dict):
    ensure_dirs()
    payload = {
        "version": 1,
        "schedules": [normalize_report_schedule(item) for item in data.get("schedules", []) if isinstance(item, dict)],
        "last_runs": data.get("last_runs", {}) if isinstance(data.get("last_runs", {}), dict) else {},
    }
    save_json_atomic(REPORT_SCHEDULES_PATH, payload)


def schedule_label(item: dict) -> str:
    mode = "Top" if item.get("mode") == "top" else "完整"
    if item.get("scope") == "daily":
        return f"每日 {item.get('time')} 发送{mode}日报"
    if item.get("scope") == "weekly":
        names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        return f"每周{names[int(item.get('weekday', 0))]} {item.get('time')} 发送{mode}周报"
    return f"每月 {int(item.get('month_day', 1))} 号 {item.get('time')} 发送{mode}月报"


def schedule_due_key(item: dict, now: datetime) -> str | None:
    hour, minute = _parse_hhmm(item.get("time", ""))
    if now.hour != hour or now.minute != minute:
        return None
    if item.get("scope") == "weekly" and now.weekday() != int(item.get("weekday", 0)):
        return None
    if item.get("scope") == "monthly" and now.day != int(item.get("month_day", 1)):
        return None
    return f"{item.get('id')}:{now.strftime('%Y-%m-%d %H:%M')}"


def schedule_next_run_at(item: dict, now: datetime | None = None, horizon_days: int = 370) -> int | None:
    schedule = normalize_report_schedule(item)
    if not schedule.get("enabled"):
        return None
    now = now or now_dt()
    hour, minute = _parse_hhmm(schedule.get("time", ""))
    today = now.date()
    for offset in range(max(1, int(horizon_days)) + 1):
        day = today + timedelta(days=offset)
        if schedule.get("scope") == "weekly" and day.weekday() != int(schedule.get("weekday", 0)):
            continue
        if schedule.get("scope") == "monthly" and day.day != int(schedule.get("month_day", 1)):
            continue
        candidate = datetime(day.year, day.month, day.day, hour, minute, tzinfo=TZ)
        if candidate > now:
            return int(candidate.timestamp())
    return None


def get_json(url: str):
    r = HTTP_SESSION.get(url, timeout=TIMEOUT, headers=build_komari_headers())
    r.raise_for_status()
    return r.json()


def post_json(url: str, payload: dict):
    r = requests.post(url, json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def telegram_send_to_chat(text: str, chat_id: str, parse_mode: str | None = "HTML"):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 未设置")
    if not str(chat_id).strip():
        raise RuntimeError("chat_id 未设置")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": str(chat_id).strip(),
        "text": text,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    return post_json(url, payload)


def telegram_send(text: str):
    return telegram_send_to_chat(text, TELEGRAM_CHAT_ID, parse_mode="HTML")


def telegram_send_plain(text: str):
    return telegram_send_to_chat(text, TELEGRAM_CHAT_ID, parse_mode=None)


def telegram_alert_chat_id() -> str:
    return TELEGRAM_ALERT_CHAT_ID or TELEGRAM_CHAT_ID


def telegram_send_alert(text: str):
    return telegram_send_to_chat(text, telegram_alert_chat_id(), parse_mode="HTML")


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

    # Telegram HTML 安全转义：先全量转义，再还原白名单标签
    out = out.replace("&", "&amp;")
    out = out.replace("<", "&lt;")
    out = out.replace(">", "&gt;")

    allow_map = {
        "&lt;b&gt;": "<b>",
        "&lt;/b&gt;": "</b>",
        "&lt;i&gt;": "<i>",
        "&lt;/i&gt;": "</i>",
        "&lt;code&gt;": "<code>",
        "&lt;/code&gt;": "</code>",
        "&lt;pre&gt;": "<pre>",
        "&lt;/pre&gt;": "</pre>",
    }
    for escaped, raw in allow_map.items():
        out = out.replace(escaped, raw)
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


def normalize_percent_metric(value, total=None) -> float | None:
    if isinstance(value, dict):
        used = value.get("used")
        capacity = value.get("total", value.get("capacity", total))
        if used is not None and capacity:
            try:
                capacity_f = float(capacity)
                if capacity_f > 0:
                    pct = float(used) / capacity_f * 100
                    return round(pct, 2) if 0 <= pct <= 100 else None
            except Exception:
                return None
        for key in ("percent", "percentage", "usage", "value"):
            if key in value:
                return normalize_percent_metric(value.get(key), total=total)
        return None

    try:
        number = float(value)
    except Exception:
        return None
    if 0 <= number <= 100:
        return round(number, 2)
    if total:
        try:
            total_f = float(total)
            if total_f > 0:
                pct = number / total_f * 100
                return round(pct, 2) if 0 <= pct <= 100 else None
        except Exception:
            return None
    return None


def _record_time_label(record: dict) -> str | None:
    if not isinstance(record, dict):
        return None
    for key in ("time", "timestamp", "created_at", "createdAt", "ts"):
        v = record.get(key)
        if v is not None:
            return str(v)
    return None


def compute_traffic_from_records(records: list[dict]) -> dict:
    if records:
        logging.debug("records fields: %s", list(records[0].keys()))
    if not records:
        up_delta = down_delta = 0
        first = last = None
    else:
        seen = set()
        deduped = []
        for r in records:
            t = r.get("time", "")
            if t not in seen:
                seen.add(t)
                deduped.append(r)
        records_sorted = sorted(deduped, key=lambda r: r.get("time", ""))
        first, last = records_sorted[0], records_sorted[-1]
        up_delta = max(0, int(last.get("net_total_up", 0)) - int(first.get("net_total_up", 0)))
        down_delta = max(0, int(last.get("net_total_down", 0)) - int(first.get("net_total_down", 0)))

    total = up_delta + down_delta
    cpu_values = []
    ram_values = []
    disk_values = []
    for rec in records:
        cpu = _to_float_safe(rec.get("cpu"))
        ram = normalize_percent_metric(rec.get("ram"), rec.get("ram_total"))
        disk = normalize_percent_metric(rec.get("disk"), rec.get("disk_total"))
        if cpu is not None:
            cpu_values.append(cpu)
        if ram is not None:
            ram_values.append(ram)
        if disk is not None:
            disk_values.append(disk)

    return {
        "up": up_delta,
        "down": down_delta,
        "total": total,
        "up_human": human_bytes(up_delta),
        "down_human": human_bytes(down_delta),
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


def format_top_only_message(period_label: str, deltas: dict, reset_warnings: list[str], skipped: list[str] | None = None) -> str:
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

    return "\n".join(lines)


def send_top_only(period_label: str, deltas: dict, reset_warnings: list[str], skipped: list[str] | None = None):
    telegram_send(format_top_only_message(period_label, deltas, reset_warnings, skipped=skipped))


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
    upsert_daily_usage(day_str, deltas, source="daily_report")


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
    migrate_history_to_traffic_db()
    db_summed = aggregate_daily_usage(from_day, to_day)
    if db_summed:
        return db_summed

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


def take_sample_if_due(force: bool = False, record: bool = True, source: str = "sample-worker"):
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

    started = time.time()
    try:
        current, skipped = fetch_nodes_and_totals()
        nodes_map = build_nodes_map_from_current(current)

        samples.append({"ts": now_ts, "nodes": nodes_map, "skipped": skipped})
        samples = prune_samples(samples, now_ts)
        save_samples({"samples": samples})
        if record:
            safe_record_task_run(
                "sample",
                source,
                "success",
                started_at=started,
                finished_at=time.time(),
                summary=f"采样 {len(nodes_map)} 个节点，跳过 {len(skipped)} 个",
                metadata={"nodes": len(nodes_map), "skipped": skipped, "force": bool(force)},
            )
    except Exception as exc:
        if record:
            safe_record_task_run(
                "sample",
                source,
                "failed",
                started_at=started,
                finished_at=time.time(),
                summary="采样失败",
                error=str(exc),
                metadata={"force": bool(force)},
            )
        raise


def sample_worker_loop():
    logging.info("sample worker started, interval=%ss", SAMPLE_INTERVAL_SECONDS)
    while not SAMPLE_STOP_EVENT.is_set():
        try:
            take_sample_if_due(force=False)
            run_alert_check(dry_run=False, notify=True, force_sample=False)
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


# -------------------- 智能告警 --------------------

def parse_silence_windows(value: str) -> list[tuple[int, int]]:
    """
    解析静默窗口，格式：HH:MM-HH:MM,23:00-07:00。
    返回当天分钟数区间，支持跨午夜。
    """
    text = validate_silence_windows_text(value)
    if not text:
        return []

    windows = []
    for part in re.split(r"[,;]\s*", text):
        if not part:
            continue
        m = re.fullmatch(r"(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})", part.strip())
        sh, sm, eh, em = [int(x) for x in m.groups()]
        start = sh * 60 + sm
        end = eh * 60 + em
        windows.append((start, end))
    return windows


def is_in_silence_window(now: datetime | None = None, windows_text: str | None = None) -> bool:
    windows = parse_silence_windows(ALERT_SILENCE_WINDOWS if windows_text is None else windows_text)
    if not windows:
        return False
    now = now or now_dt()
    minute = now.hour * 60 + now.minute
    for start, end in windows:
        if start < end and start <= minute < end:
            return True
        if start > end and (minute >= start or minute < end):
            return True
    return False


def load_alerts_state() -> dict:
    data = load_json(ALERTS_STATE_PATH, {})
    if not isinstance(data, dict):
        data = {}
    data.setdefault("version", 1)
    data.setdefault("active", {})
    data.setdefault("node_skips", {})
    data.setdefault("muted_until", 0)
    return data


def save_alerts_state(data: dict):
    save_json_atomic(ALERTS_STATE_PATH, data)


def alerts_muted_until_dt(state: dict) -> datetime | None:
    muted_until = int(state.get("muted_until", 0) or 0)
    if muted_until <= int(time.time()):
        return None
    return datetime.fromtimestamp(muted_until, TZ)


def set_alerts_muted_for(hours: int) -> datetime:
    if hours <= 0:
        raise RuntimeError("mute hours must be > 0")
    ensure_dirs()
    state = load_alerts_state()
    muted_until = int(time.time()) + hours * 3600
    state["muted_until"] = muted_until
    save_alerts_state(state)
    return datetime.fromtimestamp(muted_until, TZ)


def clear_alerts_muted():
    ensure_dirs()
    state = load_alerts_state()
    state["muted_until"] = 0
    save_alerts_state(state)


def parse_mute_hours_arg(value: str) -> int:
    text = (value or "").strip().lower()
    m = re.fullmatch(r"(\d+)\s*h?", text)
    if not m:
        raise RuntimeError("用法：/mute_alerts 2h")
    hours = int(m.group(1))
    if hours <= 0:
        raise RuntimeError("用法：/mute_alerts 2h（N>0）")
    return hours


def _alert_escape(value) -> str:
    return html.escape(str(value), quote=False)


def _alert_event(key: str, alert_type: str, title: str, body: str) -> dict:
    return {
        "key": key,
        "type": alert_type,
        "title": title,
        "body": body,
    }


def _skip_name(skip: str) -> str:
    text = str(skip or "").strip()
    if not text:
        return "unknown"
    return re.sub(r"\([^)]*\)$", "", text).strip() or text


def _sum_deltas(deltas: dict) -> tuple[int, int, int]:
    total_up = 0
    total_down = 0
    for item in deltas.values():
        total_up += int(item.get("up", 0))
        total_down += int(item.get("down", 0))
    total = total_up + total_down
    return total_up, total_down, total


def collect_alert_candidates(state: dict, now_ts: int | None = None) -> list[dict]:
    """
    生成当前应该处于 active 状态的告警候选，并更新连续节点失败计数。
    """
    now_ts = now_ts or int(time.time())
    candidates: list[dict] = []

    samples_data = load_samples()
    samples = samples_data.get("samples", []) if isinstance(samples_data, dict) else []
    latest = samples[-1] if samples else None

    if latest:
        latest_ts = int(latest.get("ts", now_ts) or now_ts)
        skipped = [str(x) for x in (latest.get("skipped", []) or [])]
        skipped_names = {_skip_name(x): x for x in skipped}
        node_skips = state.setdefault("node_skips", {})

        for name, raw in skipped_names.items():
            rec = node_skips.get(name, {})
            if int(rec.get("last_sample_ts", 0) or 0) == latest_ts:
                count = int(rec.get("count", 0))
            else:
                count = int(rec.get("count", 0)) + 1
            node_skips[name] = {
                "count": count,
                "last_seen": now_ts,
                "last_sample_ts": latest_ts,
                "last_reason": raw,
            }
            if count >= ALERT_NODE_MISSING_SAMPLES:
                candidates.append(_alert_event(
                    f"node_missing:{name}",
                    "node_missing",
                    f"节点连续采样异常：{name}",
                    (
                        f"节点 <b>{_alert_escape(name)}</b> 已连续 "
                        f"<b>{count}</b> 次采样失败。\n"
                        f"最近原因：<code>{_alert_escape(raw)}</code>"
                    ),
                ))

        for name in list(node_skips.keys()):
            if name not in skipped_names:
                node_skips[name]["count"] = 0

        window_threshold_enabled = ALERT_TOTAL_WINDOW_BYTES > 0 or ALERT_NODE_WINDOW_BYTES > 0
        if window_threshold_enabled and len(samples) >= 2:
            target_ts = int(latest.get("ts", now_ts)) - ALERT_WINDOW_MINUTES * 60
            base = get_sample_at_or_before(target_ts)
            if base is not None and base is not latest:
                deltas, reset_warnings = compute_strict_sample_delta_from_maps(
                    latest.get("nodes", {}),
                    base.get("nodes", {}),
                )
                total_up, total_down, total = _sum_deltas(deltas)
                from_dt = datetime.fromtimestamp(int(base.get("ts", target_ts)), TZ)
                to_dt = datetime.fromtimestamp(int(latest.get("ts", now_ts)), TZ)
                label = f"{from_dt.strftime('%Y-%m-%d %H:%M')} -> {to_dt.strftime('%Y-%m-%d %H:%M')}"

                if ALERT_TOTAL_WINDOW_BYTES > 0 and total >= ALERT_TOTAL_WINDOW_BYTES:
                    body = (
                        f"窗口：<code>{_alert_escape(label)}</code>\n"
                        f"合计：<b>{human_bytes(total)}</b>"
                        f"（下行 {human_bytes(total_down)} / 上行 {human_bytes(total_up)}）\n"
                        f"阈值：<b>{human_bytes(ALERT_TOTAL_WINDOW_BYTES)}</b>"
                    )
                    if reset_warnings:
                        body += f"\n计数器重置/缺样节点：{_alert_escape('、'.join(reset_warnings[:10]))}"
                    candidates.append(_alert_event("window_total", "window_total", "窗口总流量超阈值", body))

                if ALERT_NODE_WINDOW_BYTES > 0:
                    for uuid, item in deltas.items():
                        up = int(item.get("up", 0))
                        down = int(item.get("down", 0))
                        total_node = up + down
                        if total_node < ALERT_NODE_WINDOW_BYTES:
                            continue
                        name = item.get("name") or uuid
                        body = (
                            f"节点：<b>{_alert_escape(name)}</b>\n"
                            f"窗口：<code>{_alert_escape(label)}</code>\n"
                            f"合计：<b>{human_bytes(total_node)}</b>"
                            f"（下行 {human_bytes(down)} / 上行 {human_bytes(up)}）\n"
                            f"阈值：<b>{human_bytes(ALERT_NODE_WINDOW_BYTES)}</b>"
                        )
                        candidates.append(_alert_event(
                            f"window_node:{uuid}",
                            "window_node",
                            f"节点窗口流量超阈值：{name}",
                            body,
                        ))

    if ALERT_DAILY_TOTAL_BYTES > 0 or ALERT_DAILY_NODE_BYTES > 0:
        today = build_today_delta_struct()
        if isinstance(today, dict) and today.get("note") == "baseline_ok":
            nodes = today.get("nodes", []) or []
            total_up = sum(int(n.get("up", 0)) for n in nodes)
            total_down = sum(int(n.get("down", 0)) for n in nodes)
            total = total_up + total_down
            if ALERT_DAILY_TOTAL_BYTES > 0 and total >= ALERT_DAILY_TOTAL_BYTES:
                candidates.append(_alert_event(
                    "daily_total",
                    "daily_total",
                    "今日总流量超阈值",
                    (
                        f"日期：<code>{_alert_escape(today.get('date', 'today'))}</code>\n"
                        f"合计：<b>{human_bytes(total)}</b>"
                        f"（下行 {human_bytes(total_down)} / 上行 {human_bytes(total_up)}）\n"
                        f"阈值：<b>{human_bytes(ALERT_DAILY_TOTAL_BYTES)}</b>"
                    ),
                ))

            if ALERT_DAILY_NODE_BYTES > 0:
                for item in nodes:
                    total_node = int(item.get("total", 0))
                    if total_node < ALERT_DAILY_NODE_BYTES:
                        continue
                    name = item.get("name") or item.get("uuid") or "unknown"
                    candidates.append(_alert_event(
                        f"daily_node:{item.get('uuid', name)}",
                        "daily_node",
                        f"节点今日流量超阈值：{name}",
                        (
                            f"节点：<b>{_alert_escape(name)}</b>\n"
                            f"今日合计：<b>{human_bytes(total_node)}</b>"
                            f"（下行 {human_bytes(int(item.get('down', 0)))} / "
                            f"上行 {human_bytes(int(item.get('up', 0)))})\n"
                            f"阈值：<b>{human_bytes(ALERT_DAILY_NODE_BYTES)}</b>"
                        ),
                    ))

    return candidates


def format_alert_message(event: dict, now_ts: int, repeated: bool = False) -> str:
    ts = datetime.fromtimestamp(now_ts, TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    prefix = "⚠️ <b>Komari 告警</b>"
    if repeated:
        prefix = "⚠️ <b>Komari 告警仍在持续</b>"
    return (
        f"{prefix}\n"
        f"🕒 {ts}\n"
        f"📌 <b>{_alert_escape(event.get('title', '告警'))}</b>\n"
        f"{event.get('body', '')}"
    )


def format_recovery_message(record: dict, now_ts: int) -> str:
    ts = datetime.fromtimestamp(now_ts, TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    title = record.get("title", "告警")
    return (
        "✅ <b>Komari 告警恢复</b>\n"
        f"🕒 {ts}\n"
        f"📌 <b>{_alert_escape(title)}</b>\n"
        "当前规则已不再触发。"
    )


def apply_alert_candidates(state: dict, candidates: list[dict], now_ts: int, dry_run: bool = False) -> list[dict]:
    active = state.setdefault("active", {})
    candidate_keys = {c["key"] for c in candidates}
    muted_until = int(state.get("muted_until", 0) or 0)
    muted = muted_until > now_ts or is_in_silence_window(datetime.fromtimestamp(now_ts, TZ))
    events: list[dict] = []

    for candidate in candidates:
        key = candidate["key"]
        rec = active.get(key)
        is_new = rec is None
        if rec is None:
            rec = {
                "type": candidate.get("type"),
                "title": candidate.get("title"),
                "body": candidate.get("body"),
                "first_seen": now_ts,
                "last_seen": now_ts,
                "last_sent": 0,
            }
            active[key] = rec
        else:
            rec["title"] = candidate.get("title")
            rec["body"] = candidate.get("body")
            rec["last_seen"] = now_ts

        last_sent = int(rec.get("last_sent", 0) or 0)
        due = last_sent == 0 or (now_ts - last_sent >= ALERT_COOLDOWN_SECONDS)
        if due:
            repeated = (not is_new) and last_sent > 0
            events.append({
                "kind": "alert",
                "key": key,
                "title": rec.get("title", key),
                "message": format_alert_message(candidate, now_ts, repeated=repeated),
                "suppressed": muted,
                "reason": "muted" if muted else "",
            })
            if not dry_run and not muted:
                rec["last_sent"] = now_ts

    for key, rec in list(active.items()):
        if key in candidate_keys:
            continue
        if ALERT_RECOVERY_NOTIFY:
            events.append({
                "kind": "recovery",
                "key": key,
                "title": rec.get("title", key),
                "message": format_recovery_message(rec, now_ts),
                "suppressed": muted,
                "reason": "muted" if muted else "",
            })
        if not dry_run:
            del active[key]

    return events


def _run_alert_check_impl(dry_run: bool = False, notify: bool = True, force_sample: bool = False) -> dict:
    ensure_dirs()
    now_ts = int(time.time())
    if not ALERTS_ENABLED:
        return {"enabled": False, "events": [], "active_count": 0}

    if force_sample:
        take_sample_if_due(force=True, record=False, source="alert-check")

    state = load_alerts_state()
    work_state = json.loads(json.dumps(state)) if dry_run else state
    candidates = collect_alert_candidates(work_state, now_ts=now_ts)
    events = apply_alert_candidates(work_state, candidates, now_ts=now_ts, dry_run=dry_run)

    if not dry_run:
        save_alerts_state(work_state)
        if notify:
            for event in events:
                if event.get("suppressed"):
                    continue
                try:
                    telegram_send_alert(event["message"])
                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code == 400:
                        telegram_send_to_chat(re.sub(r"</?[^>]+>", "", event["message"]), telegram_alert_chat_id(), parse_mode=None)
                    else:
                        raise

    return {
        "enabled": True,
        "events": events,
        "active_count": len(work_state.get("active", {})),
        "muted_until": int(work_state.get("muted_until", 0) or 0),
        "dry_run": dry_run,
    }


def run_alert_check(dry_run: bool = False, notify: bool = True, force_sample: bool = False, record: bool = True, source: str = "alert-check") -> dict:
    started = time.time()
    try:
        result = _run_alert_check_impl(dry_run=dry_run, notify=notify, force_sample=force_sample)
        events = result.get("events", []) or []
        summary = "告警未启用" if not result.get("enabled", True) else f"事件 {len(events)} 个，active {int(result.get('active_count', 0) or 0)} 个"
        if record:
            safe_record_task_run(
                "alert",
                source,
                "success",
                started_at=started,
                finished_at=time.time(),
                summary=summary,
                metadata={
                    "dry_run": bool(dry_run),
                    "notify": bool(notify),
                    "force_sample": bool(force_sample),
                    "events": len(events),
                    "active_count": int(result.get("active_count", 0) or 0),
                },
            )
        return result
    except Exception as exc:
        if record:
            safe_record_task_run(
                "alert",
                source,
                "failed",
                started_at=started,
                finished_at=time.time(),
                summary="告警检查失败",
                error=str(exc),
                metadata={"dry_run": bool(dry_run), "notify": bool(notify), "force_sample": bool(force_sample)},
            )
        raise


def format_alert_check_result(result: dict) -> str:
    if not result.get("enabled", True):
        return "alerts disabled"
    events = result.get("events", []) or []
    lines = [
        f"alerts active={int(result.get('active_count', 0))}",
        f"events={len(events)}",
    ]
    for event in events:
        suffix = " suppressed" if event.get("suppressed") else ""
        lines.append(f"- {event.get('kind')} {event.get('key')}: {event.get('title')}{suffix}")
    return "\n".join(lines)


def format_alert_status() -> str:
    state = load_alerts_state()
    active = state.get("active", {}) or {}
    muted_until = alerts_muted_until_dt(state)

    lines = ["🚨 <b>告警状态</b>"]
    lines.append(f"启用：{'是' if ALERTS_ENABLED else '否'}")
    lines.append(f"告警 chat：<code>{_alert_escape(telegram_alert_chat_id())}</code>")
    if muted_until:
        lines.append(f"静默至：<code>{muted_until.strftime('%Y-%m-%d %H:%M:%S %Z')}</code>")
    elif is_in_silence_window():
        lines.append("当前处于静默时段")
    else:
        lines.append("静默：否")

    thresholds = [
        ("窗口总流量", ALERT_TOTAL_WINDOW_BYTES),
        ("节点窗口流量", ALERT_NODE_WINDOW_BYTES),
        ("今日总流量", ALERT_DAILY_TOTAL_BYTES),
        ("节点今日流量", ALERT_DAILY_NODE_BYTES),
    ]
    enabled_thresholds = [f"{name} {human_bytes(value)}" for name, value in thresholds if value > 0]
    lines.append(f"窗口：{ALERT_WINDOW_MINUTES} 分钟，冷却：{ALERT_COOLDOWN_SECONDS} 秒")
    lines.append("阈值：" + ("；".join(enabled_thresholds) if enabled_thresholds else "未配置流量阈值"))

    if not active:
        lines.append("")
        lines.append("当前无 active 告警。")
        return "\n".join(lines)

    lines.append("")
    lines.append(f"Active 告警：{len(active)}")
    now_ts = int(time.time())
    for rec in active.values():
        last_seen = datetime.fromtimestamp(int(rec.get("last_seen", now_ts)), TZ).strftime("%m-%d %H:%M")
        lines.append(f"- <b>{_alert_escape(rec.get('title', '告警'))}</b>（last seen {last_seen}）")
    return "\n".join(lines)


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


def build_period_report_message(from_dt: datetime, to_dt: datetime, tag: str, top_only: bool = False) -> str:
    ensure_dirs()
    baseline_nodes = get_baseline_nodes(tag)
    if baseline_nodes is None:
        set_baseline_to_current(tag)
        return (
            f"⚠️ 当前没有找到 起点快照（{tag}）。\n"
            f"我已把现在的累计值保存为新的起点。\n"
            f"请稍后再发一次命令查看稳定统计。"
        )

    current, skipped = fetch_nodes_and_totals()
    deltas, _new_base, reset_warnings = compute_delta_from_nodes(current, baseline_nodes)
    period_label = f"{from_dt.strftime('%Y-%m-%d %H:%M')} → {to_dt.strftime('%Y-%m-%d %H:%M')}"

    if top_only:
        return format_top_only_message(period_label, deltas, reset_warnings, skipped=skipped)
    return format_report("流量统计", period_label, deltas, reset_warnings, skipped=skipped, include_top=True)


def run_period_report(from_dt: datetime, to_dt: datetime, tag: str, top_only: bool = False):
    telegram_send(build_period_report_message(from_dt, to_dt, tag, top_only=top_only))


def scheduled_report_period_parts(scope: str):
    td = today_date()
    now = now_dt()
    if scope == "daily":
        return start_of_day(td), now, td.strftime("%Y-%m-%d")
    if scope == "weekly":
        ws = start_of_week(td)
        return start_of_day(ws), now, f"WEEK-{ws.strftime('%Y-%m-%d')}"
    if scope == "monthly":
        ms = start_of_month(td)
        return start_of_day(ms), now, f"MONTH-{ms.strftime('%Y-%m-%d')}"
    raise RuntimeError("scope must be daily, weekly, or monthly")


def _run_report_schedule_impl(item: dict) -> dict:
    schedule = normalize_report_schedule(item)
    start, now, tag = scheduled_report_period_parts(schedule["scope"])
    message = build_period_report_message(start, now, tag, top_only=(schedule["mode"] == "top"))
    chat = schedule.get("chat") or TELEGRAM_CHAT_ID
    telegram_send_to_chat(message, chat)
    return {"sent": True, "chat": chat, "schedule": schedule, "label": schedule_label(schedule)}


def run_report_schedule(item: dict, source: str = "scheduler", record: bool = True) -> dict:
    schedule = normalize_report_schedule(item)
    metadata = {
        "schedule_id": schedule.get("id", ""),
        "scope": schedule.get("scope", ""),
        "mode": schedule.get("mode", ""),
        "label": schedule_label(schedule),
    }
    if not record:
        return _run_report_schedule_impl(schedule)
    return run_with_task_record(
        "report",
        source,
        lambda: _run_report_schedule_impl(schedule),
        summary_func=lambda result: result.get("label", "") if isinstance(result, dict) else "",
        metadata=metadata,
    )


def scheduler_worker_loop():
    logging.info("report scheduler started")
    while not SCHEDULER_STOP_EVENT.is_set():
        try:
            data = load_report_schedules()
            changed = False
            now = now_dt()
            for item in data.get("schedules", []):
                if not item.get("enabled"):
                    continue
                due_key = schedule_due_key(item, now)
                if not due_key or data["last_runs"].get(item["id"]) == due_key:
                    continue
                run_report_schedule(item, source="scheduler")
                data["last_runs"][item["id"]] = due_key
                changed = True
            if changed:
                save_report_schedules(data)
        except Exception:
            logging.exception("report scheduler error")
        SCHEDULER_STOP_EVENT.wait(timeout=30)
    logging.info("report scheduler stopped")


def start_report_scheduler():
    global SCHEDULER_THREAD
    if SCHEDULER_THREAD and SCHEDULER_THREAD.is_alive():
        return
    SCHEDULER_STOP_EVENT.clear()
    SCHEDULER_THREAD = threading.Thread(target=scheduler_worker_loop, name="report-scheduler", daemon=True)
    SCHEDULER_THREAD.start()


def stop_report_scheduler():
    SCHEDULER_STOP_EVENT.set()
    if SCHEDULER_THREAD and SCHEDULER_THREAD.is_alive():
        SCHEDULER_THREAD.join(timeout=3)


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
        run_alert_check(dry_run=False, notify=True, force_sample=False)
    except Exception:
        pass
    start_sample_worker()
    start_report_scheduler()

    while True:
        if SHUTTING_DOWN:
            logging.warning("shutdown flag set, exiting listen loop")
            stop_sample_worker()
            stop_report_scheduler()
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

                elif cmd == "/alerts":
                    if not is_admin(chat_id):
                        telegram_send("⛔ 无权限")
                        continue
                    telegram_send(format_alert_status())

                elif cmd == "/mute_alerts":
                    if not is_admin(chat_id):
                        telegram_send("⛔ 无权限")
                        continue
                    try:
                        hours = parse_mute_hours_arg(arg_text)
                    except Exception as e:
                        telegram_send(str(e))
                        continue
                    muted_until = set_alerts_muted_for(hours)
                    telegram_send(f"✅ 已静默告警至：<code>{muted_until.strftime('%Y-%m-%d %H:%M:%S %Z')}</code>")

                elif cmd == "/unmute_alerts":
                    if not is_admin(chat_id):
                        telegram_send("⛔ 无权限")
                        continue
                    clear_alerts_muted()
                    telegram_send("✅ 已解除告警静默")

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
                    ai_text = normalize_ai_answer_for_telegram(answer)
                    try:
                        telegram_send(ai_text)
                    except requests.exceptions.HTTPError as e:
                        if e.response is not None and e.response.status_code == 400:
                            logging.warning("telegram html parse failed, fallback to plain text send")
                            telegram_send_plain(re.sub(r"</?[^>]+>", "", ai_text))
                        else:
                            raise

                elif cmd in ("/help", "/start"):
                    telegram_send(
                        "可用命令：\n"
                        "/today  /week  /month\n"
                        "/top  (默认 today)\n"
                        "/top today|week|month\n"
                        "/top 6h（任意Nh）\n"
                        "/ask 你的问题（或 /ai）\n"
                        "管理员：/alerts /mute_alerts 2h /unmute_alerts\n"
                        "/archive /bootstrap /rebuild_baselines\n"
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
        raise RuntimeError("Usage: report_daily | report_weekly | report_monthly | listen | bootstrap [--force] | rebuild-baselines [--dry-run] [--since YYYY-MM-DD] | check_alerts [--dry-run] | health | config-validate")

    cmd = sys.argv[1].strip().lower()

    if cmd == "report_daily":
        run_with_task_record("report", "cli:report_daily", run_daily_send_yesterday, summary_func=lambda _result: "昨日流量日报")
        return 0
    if cmd == "report_weekly":
        run_with_task_record("report", "cli:report_weekly", run_weekly_send_last_week, summary_func=lambda _result: "上周流量周报")
        return 0
    if cmd == "report_monthly":
        run_with_task_record("report", "cli:report_monthly", run_monthly_send_last_month, summary_func=lambda _result: "上月流量月报")
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
    if cmd in ("check_alerts", "check-alerts"):
        dry_run = False
        for arg in sys.argv[2:]:
            if arg.strip().lower() == "--dry-run":
                dry_run = True
                continue
            raise RuntimeError(f"Unknown check_alerts arg: {arg}")
        validate_config_or_raise()
        result = run_alert_check(dry_run=dry_run, notify=not dry_run, force_sample=True)
        print(format_alert_check_result(result))
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
