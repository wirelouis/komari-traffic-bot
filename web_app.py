#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import base64
import hashlib
import hmac
import os
import re
import secrets
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import komari_traffic_report as k


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
SESSION_COOKIE = "komari_traffic_session"
SESSION_MAX_AGE_SECONDS = 7 * 24 * 3600
WEB_SESSION_SECRET = os.environ.get("WEB_SESSION_SECRET", "").strip() or secrets.token_urlsafe(32)
WEB_SESSION_SECRET_TEMPORARY = not bool(os.environ.get("WEB_SESSION_SECRET", "").strip())

app = FastAPI(title="Komari Traffic Web", docs_url=None, redoc_url=None)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class LoginRequest(BaseModel):
    username: str = ""
    password: str = ""


class AlertCheckRequest(BaseModel):
    notify: bool = False


class AlertMuteRequest(BaseModel):
    hours: int


class TelegramReportRequest(BaseModel):
    scope: str
    mode: str = "full"


class AiAskRequest(BaseModel):
    question: str


class NodeBindingRequest(BaseModel):
    source_id: str
    komari_uuid: str = ""


def api_ok(data: Any = None, **extra):
    payload = {"ok": True, "data": data}
    payload.update(extra)
    return JSONResponse(payload)


def api_error(message: str, status_code: int = 400, code: str = "error", **extra):
    payload = {"ok": False, "error": {"code": code, "message": str(message)}}
    payload.update(extra)
    return JSONResponse(payload, status_code=status_code)


@app.exception_handler(HTTPException)
async def http_error_handler(_request: Request, exc: HTTPException):
    message = exc.detail if isinstance(exc.detail, str) else "request failed"
    code = "unauthorized" if exc.status_code == 401 else "http_error"
    return api_error(message, status_code=exc.status_code, code=code)


@app.exception_handler(Exception)
async def error_handler(_request: Request, exc: Exception):
    return api_error(str(exc), status_code=500, code=type(exc).__name__)


def web_username() -> str:
    return os.environ.get("WEB_USERNAME", "admin").strip() or "admin"


def web_password() -> str:
    return os.environ.get("WEB_PASSWORD", "").strip()


def web_password_configured() -> bool:
    return bool(web_password())


def _sign_session(body: str) -> str:
    return hmac.new(WEB_SESSION_SECRET.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()


def create_session_token(username: str) -> str:
    issued = int(time.time())
    nonce = secrets.token_urlsafe(12)
    body = f"{username}|{issued}|{nonce}"
    token = f"{body}|{_sign_session(body)}"
    return base64.urlsafe_b64encode(token.encode("utf-8")).decode("ascii")


def validate_session_token(token: str | None) -> str | None:
    if not token:
        return None
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
        username, issued_text, nonce, signature = raw.split("|", 3)
        body = f"{username}|{issued_text}|{nonce}"
        if not hmac.compare_digest(signature, _sign_session(body)):
            return None
        issued = int(issued_text)
        if issued <= 0 or int(time.time()) - issued > SESSION_MAX_AGE_SECONDS:
            return None
        if username != web_username():
            return None
        return username
    except Exception:
        return None


def current_user(request: Request) -> str:
    username = validate_session_token(request.cookies.get(SESSION_COOKIE))
    if not username:
        raise HTTPException(status_code=401, detail="unauthorized")
    return username


def mask_value(value: str, visible: int = 3) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= visible * 2:
        return "*" * len(text)
    return f"{text[:visible]}***{text[-visible:]}"


def safe_call(func, *args, **kwargs) -> dict:
    try:
        return {"ok": True, "data": func(*args, **kwargs)}
    except Exception as exc:
        return {"ok": False, "error": {"code": type(exc).__name__, "message": str(exc)}}


def node_bindings_path() -> Path:
    return Path(k.DATA_DIR) / "node_bindings.json"


def load_node_bindings() -> dict:
    data = k.load_json(str(node_bindings_path()), {"version": 1, "bindings": {}})
    bindings = data.get("bindings", {}) if isinstance(data, dict) else {}
    if not isinstance(bindings, dict):
        bindings = {}
    return {"version": 1, "bindings": bindings}


def save_node_bindings(data: dict):
    k.ensure_dirs()
    payload = {"version": 1, "bindings": data.get("bindings", {}) if isinstance(data, dict) else {}}
    k.save_json_atomic(str(node_bindings_path()), payload)


def komari_server_url(uuid: str) -> str:
    base = k.KOMARI_BASE_URL.rstrip("/")
    if not base or not uuid:
        return ""
    return f"{base}/server/{quote(str(uuid), safe='')}"


def normalize_machine(node: dict) -> dict:
    uuid = str(node.get("uuid") or "")
    name = str(node.get("name") or uuid or "unknown")
    machine = {
        "uuid": uuid,
        "name": name,
        "region": node.get("region") or "",
        "group": node.get("group") or "",
        "os": node.get("os") or "",
        "arch": node.get("arch") or "",
        "version": node.get("version") or "",
        "tags": node.get("tags") if isinstance(node.get("tags"), list) else [],
        "web_url": komari_server_url(uuid),
    }
    return machine


def fetch_komari_machines() -> list[dict]:
    if not k.KOMARI_BASE_URL:
        raise RuntimeError("KOMARI_BASE_URL 未设置")
    nodes_resp = k.get_json(f"{k.KOMARI_BASE_URL}/api/nodes")
    if not (isinstance(nodes_resp, dict) and nodes_resp.get("status") == "success"):
        raise RuntimeError(f"/api/nodes 返回异常：{nodes_resp}")
    nodes = nodes_resp.get("data", [])
    if not isinstance(nodes, list):
        raise RuntimeError(f"/api/nodes data 非列表：{nodes_resp}")
    machines = [normalize_machine(node) for node in nodes if isinstance(node, dict) and node.get("uuid")]
    machines.sort(key=lambda item: (str(item.get("name", "")).lower(), str(item.get("uuid", ""))))
    return machines


def machine_context() -> tuple[list[dict], dict[str, dict]]:
    machines = fetch_komari_machines()
    return machines, {str(machine.get("uuid")): machine for machine in machines}


def resolve_node_binding(source_id: str, machine_index: dict[str, dict], bindings: dict) -> dict:
    source_id = str(source_id or "")
    manual = bindings.get(source_id) if isinstance(bindings, dict) else None
    manual_uuid = str((manual or {}).get("komari_uuid") or "")
    if manual_uuid:
        machine = machine_index.get(manual_uuid)
        return {
            "source_id": source_id,
            "mode": "manual",
            "komari_uuid": manual_uuid,
            "stale": machine is None,
            "updated_at": int((manual or {}).get("updated_at") or 0),
        }
    machine = machine_index.get(source_id)
    return {
        "source_id": source_id,
        "mode": "auto" if machine else "missing",
        "komari_uuid": source_id if machine else "",
        "stale": machine is None,
        "updated_at": 0,
    }


def enrich_nodes_with_komari(nodes: list[dict], machines: list[dict] | None = None, bindings_data: dict | None = None) -> tuple[list[dict], list[dict]]:
    if machines is None:
        try:
            machines, machine_index = machine_context()
        except Exception:
            machines, machine_index = [], {}
    else:
        machine_index = {str(machine.get("uuid")): machine for machine in machines}
    bindings = (bindings_data or load_node_bindings()).get("bindings", {})
    enriched = []
    for node in nodes or []:
        item = dict(node)
        source_id = str(item.get("uuid") or item.get("name") or "")
        binding = resolve_node_binding(source_id, machine_index, bindings)
        machine = machine_index.get(binding.get("komari_uuid", ""))
        item["binding"] = binding
        item["komari"] = {
            "machine": machine,
            "web_url": machine.get("web_url", "") if machine else "",
        }
        enriched.append(item)
    return enriched, machines


def enrich_records_summary(summary: dict) -> dict:
    payload = dict(summary or {})
    try:
        machines, _machine_index = machine_context()
    except Exception:
        machines = []
    bindings_data = load_node_bindings()
    nodes, machines = enrich_nodes_with_komari(payload.get("nodes", []), machines, bindings_data)
    top_nodes, _machines = enrich_nodes_with_komari(payload.get("top_nodes", []), machines, bindings_data)
    payload["nodes"] = nodes
    payload["top_nodes"] = top_nodes
    payload["machines"] = machines
    return payload


def enrich_period_result(result: dict) -> dict:
    if not isinstance(result, dict) or not result.get("ok"):
        return result
    data = result.get("data")
    if not isinstance(data, dict):
        return result
    result = dict(result)
    result["data"] = enrich_records_summary(data)
    return result


def seconds_text(seconds: int) -> str:
    seconds = max(0, int(seconds or 0))
    if seconds >= 3600:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    if seconds >= 60:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def timestamp_text(ts: int | float | str | None) -> str:
    try:
        value = int(float(ts or 0))
    except Exception:
        value = 0
    if value <= 0:
        return ""
    return datetime.fromtimestamp(value, k.TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def read_crontab_entries() -> list[dict]:
    path = BASE_DIR / "crontab"
    if not path.exists():
        return []
    entries = []
    labels = {
        "report_daily": "昨日流量日报",
        "report_weekly": "上周流量周报",
        "report_monthly": "上月流量月报",
    }
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(maxsplit=5)
        if len(parts) < 6:
            continue
        command = parts[5]
        label = next((value for key, value in labels.items() if key in command), command)
        entries.append({"schedule": " ".join(parts[:5]), "command": command, "label": label})
    return entries


def build_telegram_status_struct() -> dict:
    return {
        "configured": bool(k.TELEGRAM_BOT_TOKEN and k.TELEGRAM_CHAT_ID),
        "bot_token_configured": bool(k.TELEGRAM_BOT_TOKEN),
        "chat": mask_value(k.TELEGRAM_CHAT_ID),
        "alert_chat": mask_value(k.telegram_alert_chat_id()),
        "schedules": read_crontab_entries(),
    }


def report_message_from_request(req: TelegramReportRequest) -> tuple[str, str, str]:
    scope = req.scope.strip().lower()
    mode = req.mode.strip().lower()
    if scope not in ("today", "week", "month"):
        raise ValueError("scope must be today, week, or month")
    if mode not in ("full", "top"):
        raise ValueError("mode must be full or top")
    start, now, tag = period_parts(scope)
    message = k.build_period_report_message(start, now, tag, top_only=(mode == "top"))
    return scope, mode, message


def build_ai_status_struct() -> dict:
    cache = k.load_ai_pack_cache()
    created_at = int(cache.get("created_at", 0) or 0) if isinstance(cache, dict) else 0
    pack = cache.get("pack", {}) if isinstance(cache, dict) else {}
    now_ts = int(time.time())
    data_sources = []
    if isinstance(pack, dict):
        for key, value in pack.items():
            if key in ("now", "stat_tz"):
                continue
            failed = isinstance(value, dict) and value.get("error") == "failed"
            count = 0
            if isinstance(value, dict):
                if isinstance(value.get("nodes"), list):
                    count = len(value.get("nodes") or [])
                elif isinstance(value.get("top_nodes"), list):
                    count = len(value.get("top_nodes") or [])
                elif isinstance(value.get("days"), list):
                    count = len(value.get("days") or [])
            data_sources.append({
                "key": key,
                "status": "failed" if failed else "ok",
                "count": count,
            })
    return {
        "configured": k.ai_enabled(),
        "model": mask_value(k.AI_MODEL, visible=2) if k.AI_MODEL else "",
        "cache_created_at": created_at,
        "cache_created_at_text": timestamp_text(created_at),
        "cache_age_seconds": max(0, now_ts - created_at) if created_at else 0,
        "cache_ttl_seconds": k.AI_PACK_CACHE_TTL_SECONDS,
        "cache_valid": bool(created_at and (k.AI_PACK_CACHE_TTL_SECONDS <= 0 or now_ts - created_at <= k.AI_PACK_CACHE_TTL_SECONDS)),
        "data_sources": data_sources,
    }


def to_node_rows(deltas: dict) -> list[dict]:
    rows = []
    for uuid, item in deltas.items():
        up = int(item.get("up", 0))
        down = int(item.get("down", 0))
        total = up + down
        rows.append({
            "uuid": uuid,
            "name": item.get("name") or uuid,
            "up": up,
            "down": down,
            "total": total,
            "up_human": k.human_bytes(up),
            "down_human": k.human_bytes(down),
            "total_human": k.human_bytes(total),
        })
    rows.sort(key=lambda x: (x["total"], x["down"], x["up"], x["name"].lower()), reverse=True)
    return rows


def total_from_nodes(nodes: list[dict]) -> dict:
    up = sum(int(n.get("up", 0)) for n in nodes)
    down = sum(int(n.get("down", 0)) for n in nodes)
    total = up + down
    return {
        "up": up,
        "down": down,
        "total": total,
        "up_human": k.human_bytes(up),
        "down_human": k.human_bytes(down),
        "total_human": k.human_bytes(total),
    }


def period_parts(scope: str):
    now = k.now_dt()
    today = k.today_date()
    if scope == "today":
        start = k.start_of_day(today)
        tag = today.strftime("%Y-%m-%d")
    elif scope == "week":
        week_start = k.start_of_week(today)
        start = k.start_of_day(week_start)
        tag = f"WEEK-{week_start.strftime('%Y-%m-%d')}"
    elif scope == "month":
        month_start = k.start_of_month(today)
        start = k.start_of_day(month_start)
        tag = f"MONTH-{month_start.strftime('%Y-%m-%d')}"
    else:
        raise RuntimeError("scope must be today, week, or month")
    return start, now, tag


def build_period_summary(scope: str) -> dict:
    start, now, tag = period_parts(scope)
    baseline = k.get_baseline_nodes(tag)
    if baseline is None:
        return {
            "scope": scope,
            "tag": tag,
            "from": start.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "to": now.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "note": "baseline_missing",
            "nodes": [],
            "top_nodes": [],
            "total": total_from_nodes([]),
            "skipped": [],
            "reset_warnings": [],
        }
    current, skipped = k.fetch_nodes_and_totals()
    deltas, _new_baseline, reset_warnings = k.compute_delta_from_nodes(current, baseline)
    nodes = to_node_rows(deltas)
    return {
        "scope": scope,
        "tag": tag,
        "from": start.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "to": now.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "note": "baseline_ok",
        "nodes": nodes,
        "top_nodes": nodes[: max(0, int(k.TOP_N))],
        "total": total_from_nodes(nodes),
        "skipped": skipped,
        "reset_warnings": reset_warnings,
    }


def build_alert_status_struct() -> dict:
    state = k.load_alerts_state()
    muted_until = k.alerts_muted_until_dt(state)
    active = []
    for key, rec in (state.get("active", {}) or {}).items():
        active.append({
            "key": key,
            "title": rec.get("title", key),
            "type": rec.get("type", ""),
            "last_seen": rec.get("last_seen", 0),
            "last_sent": rec.get("last_sent", 0),
            "last_seen_text": timestamp_text(rec.get("last_seen", 0)),
            "last_sent_text": timestamp_text(rec.get("last_sent", 0)),
        })
    active.sort(key=lambda item: int(item.get("last_seen") or 0), reverse=True)
    return {
        "enabled": bool(k.ALERTS_ENABLED),
        "active_count": len(active),
        "active": active,
        "muted_until": muted_until.strftime("%Y-%m-%d %H:%M:%S %Z") if muted_until else "",
        "in_silence_window": k.is_in_silence_window(),
        "silence_windows": k.ALERT_SILENCE_WINDOWS,
        "alert_chat": mask_value(k.telegram_alert_chat_id()),
        "cooldown_seconds": k.ALERT_COOLDOWN_SECONDS,
        "cooldown_text": seconds_text(k.ALERT_COOLDOWN_SECONDS),
        "window_minutes": k.ALERT_WINDOW_MINUTES,
        "thresholds": {
            "total_window": k.human_bytes(k.ALERT_TOTAL_WINDOW_BYTES) if k.ALERT_TOTAL_WINDOW_BYTES else "",
            "node_window": k.human_bytes(k.ALERT_NODE_WINDOW_BYTES) if k.ALERT_NODE_WINDOW_BYTES else "",
            "daily_total": k.human_bytes(k.ALERT_DAILY_TOTAL_BYTES) if k.ALERT_DAILY_TOTAL_BYTES else "",
            "daily_node": k.human_bytes(k.ALERT_DAILY_NODE_BYTES) if k.ALERT_DAILY_NODE_BYTES else "",
        },
    }


def safe_records_summary(hours: int) -> dict:
    if hours not in (1, 6, 24, 168, 720):
        raise RuntimeError("hours must be one of 1, 6, 24, 168, 720")
    return enrich_records_summary(k.build_records_summary(hours))


@app.get("/")
@app.get("/nodes")
@app.get("/alerts")
@app.get("/telegram")
@app.get("/ai")
async def index():
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        return api_error("static/index.html not found", status_code=500, code="static_missing")
    return FileResponse(str(index_path), headers={"Cache-Control": "no-store"})


@app.post("/api/auth/login")
async def login(req: LoginRequest):
    if not web_password_configured():
        return api_error("WEB_PASSWORD is not configured", status_code=503, code="web_password_missing")
    if req.username != web_username() or not hmac.compare_digest(req.password, web_password()):
        return api_error("invalid username or password", status_code=401, code="invalid_login")
    response = api_ok({
        "username": web_username(),
        "session_secret_temporary": WEB_SESSION_SECRET_TEMPORARY,
    })
    response.set_cookie(
        SESSION_COOKIE,
        create_session_token(web_username()),
        max_age=SESSION_MAX_AGE_SECONDS,
        httponly=True,
        samesite="lax",
    )
    return response


@app.post("/api/auth/logout")
async def logout():
    response = api_ok({"logged_out": True})
    response.delete_cookie(SESSION_COOKIE)
    return response


@app.get("/api/auth/session")
async def session(request: Request):
    username = validate_session_token(request.cookies.get(SESSION_COOKIE))
    return api_ok({
        "authenticated": bool(username),
        "username": username or "",
        "web_password_configured": web_password_configured(),
        "session_secret_temporary": WEB_SESSION_SECRET_TEMPORARY,
    })


@app.get("/api/overview")
async def overview(_user: str = Depends(current_user)):
    today = enrich_period_result(safe_call(build_period_summary, "today"))
    week = enrich_period_result(safe_call(build_period_summary, "week"))
    month = enrich_period_result(safe_call(build_period_summary, "month"))
    last_24h = safe_call(safe_records_summary, 24)
    last_7d = safe_call(safe_records_summary, 168)
    return api_ok({
        "now": k.now_dt().strftime("%Y-%m-%d %H:%M:%S %Z"),
        "stat_tz": k.STAT_TZ,
        "instance": k.BOT_INSTANCE_NAME or "default",
        "session_secret_temporary": WEB_SESSION_SECRET_TEMPORARY,
        "services": {
            "komari": {"configured": bool(k.KOMARI_BASE_URL), "base_url": k.KOMARI_BASE_URL},
            "telegram": {"configured": bool(k.TELEGRAM_BOT_TOKEN and k.TELEGRAM_CHAT_ID), "chat": mask_value(k.TELEGRAM_CHAT_ID)},
            "ai": {"configured": k.ai_enabled(), "model": mask_value(k.AI_MODEL, visible=2) if k.AI_MODEL else ""},
            "alerts": {"enabled": bool(k.ALERTS_ENABLED)},
        },
        "periods": {"today": today, "week": week, "month": month},
        "records": {"last_24h": last_24h, "last_7d": last_7d},
    })


@app.get("/api/nodes")
async def nodes(hours: int = 24, _user: str = Depends(current_user)):
    result = safe_call(safe_records_summary, hours)
    if not result["ok"]:
        return api_error(result["error"]["message"], status_code=502, code=result["error"]["code"], data=result)
    return api_ok(result["data"])


@app.get("/api/komari/machines")
async def komari_machines(_user: str = Depends(current_user)):
    result = safe_call(fetch_komari_machines)
    if not result["ok"]:
        return api_error(result["error"]["message"], status_code=502, code=result["error"]["code"], data=result)
    return api_ok({
        "configured": bool(k.KOMARI_BASE_URL),
        "base_url": k.KOMARI_BASE_URL,
        "machines": result["data"],
    })


@app.get("/api/node-bindings")
async def node_bindings(_user: str = Depends(current_user)):
    bindings_data = load_node_bindings()
    machines_result = safe_call(fetch_komari_machines)
    machines = machines_result["data"] if machines_result["ok"] else []
    machine_index = {str(machine.get("uuid")): machine for machine in machines}
    resolved = {
        source_id: resolve_node_binding(source_id, machine_index, bindings_data.get("bindings", {}))
        for source_id in bindings_data.get("bindings", {}).keys()
    }
    return api_ok({
        "configured": bool(k.KOMARI_BASE_URL),
        "base_url": k.KOMARI_BASE_URL,
        "machines": machines,
        "bindings": bindings_data.get("bindings", {}),
        "resolved": resolved,
        "machine_error": "" if machines_result["ok"] else machines_result["error"]["message"],
    })


@app.post("/api/node-bindings")
async def save_node_binding(req: NodeBindingRequest, _user: str = Depends(current_user)):
    source_id = req.source_id.strip()
    komari_uuid = req.komari_uuid.strip()
    if not source_id:
        return api_error("source_id is required", status_code=400, code="missing_source_id")
    bindings_data = load_node_bindings()
    bindings = bindings_data.get("bindings", {})
    if not komari_uuid:
        bindings.pop(source_id, None)
        save_node_bindings({"bindings": bindings})
        return api_ok({"source_id": source_id, "cleared": True})
    machines_result = safe_call(fetch_komari_machines)
    if not machines_result["ok"]:
        return api_error(machines_result["error"]["message"], status_code=502, code=machines_result["error"]["code"], data=machines_result)
    machine_index = {str(machine.get("uuid")): machine for machine in machines_result["data"]}
    if komari_uuid not in machine_index:
        return api_error("komari_uuid not found", status_code=404, code="komari_uuid_not_found")
    bindings[source_id] = {
        "komari_uuid": komari_uuid,
        "updated_at": int(time.time()),
    }
    save_node_bindings({"bindings": bindings})
    return api_ok({
        "source_id": source_id,
        "binding": resolve_node_binding(source_id, machine_index, bindings),
    })


@app.get("/api/nodes/{uuid}")
async def node_detail(uuid: str, hours: int = 24, _user: str = Depends(current_user)):
    summary = safe_call(safe_records_summary, hours)
    if not summary["ok"]:
        return api_error(summary["error"]["message"], status_code=502, code=summary["error"]["code"], data=summary)
    nodes_data = summary["data"].get("nodes", [])
    matched = next((n for n in nodes_data if str(n.get("uuid")) == uuid), None)
    if matched is None:
        safe_uuid = re.sub(r"[^a-zA-Z0-9_.:-]", "", uuid)
        matched = next((n for n in nodes_data if str(n.get("name")) == safe_uuid), None)
    if matched is None:
        return api_error("node not found in selected range", status_code=404, code="node_not_found")
    hourly = safe_call(k.build_last_24h_hourly_summary) if hours == 24 else {"ok": True, "data": None}
    return api_ok({"node": matched, "range": summary["data"], "hourly": hourly})


@app.get("/api/alerts")
async def alerts(_user: str = Depends(current_user)):
    return api_ok(build_alert_status_struct())


@app.post("/api/alerts/check")
async def alerts_check(req: AlertCheckRequest, _user: str = Depends(current_user)):
    result = k.run_alert_check(dry_run=not req.notify, notify=req.notify, force_sample=True)
    return api_ok(result)


@app.post("/api/alerts/mute")
async def alerts_mute(req: AlertMuteRequest, _user: str = Depends(current_user)):
    if req.hours <= 0:
        return api_error("hours must be > 0", status_code=400, code="invalid_hours")
    muted_until = k.set_alerts_muted_for(req.hours)
    return api_ok({"muted_until": muted_until.strftime("%Y-%m-%d %H:%M:%S %Z")})


@app.post("/api/alerts/unmute")
async def alerts_unmute(_user: str = Depends(current_user)):
    k.clear_alerts_muted()
    return api_ok({"muted": False})


@app.post("/api/telegram/test")
async def telegram_test(_user: str = Depends(current_user)):
    message = (
        "✅ Komari traffic Web 面板测试\n"
        f"实例：{k.BOT_INSTANCE_NAME or 'default'}\n"
        f"时间：{k.now_dt().strftime('%Y-%m-%d %H:%M:%S %Z')}"
    )
    k.telegram_send_to_chat(message, k.TELEGRAM_CHAT_ID, parse_mode=None)
    return api_ok({"sent": True, "chat": mask_value(k.TELEGRAM_CHAT_ID)})


@app.get("/api/telegram/status")
async def telegram_status(_user: str = Depends(current_user)):
    return api_ok(build_telegram_status_struct())


@app.post("/api/telegram/preview")
async def telegram_preview(req: TelegramReportRequest, _user: str = Depends(current_user)):
    try:
        scope, mode, message = report_message_from_request(req)
    except ValueError as exc:
        return api_error(str(exc), status_code=400, code="invalid_report_request")
    return api_ok({
        "scope": scope,
        "mode": mode,
        "message": message,
        "chat": mask_value(k.TELEGRAM_CHAT_ID),
    })


@app.post("/api/telegram/report")
async def telegram_report(req: TelegramReportRequest, _user: str = Depends(current_user)):
    try:
        scope, mode, message = report_message_from_request(req)
    except ValueError as exc:
        return api_error(str(exc), status_code=400, code="invalid_report_request")
    k.telegram_send(message)
    return api_ok({"sent": True, "scope": scope, "mode": mode, "chat": mask_value(k.TELEGRAM_CHAT_ID)})


@app.get("/api/ai/status")
async def ai_status(_user: str = Depends(current_user)):
    return api_ok(build_ai_status_struct())


@app.post("/api/ai/refresh")
async def ai_refresh(_user: str = Depends(current_user)):
    if not k.ai_enabled():
        return api_error("AI is not configured", status_code=400, code="ai_disabled")
    pack = k.build_ai_data_pack()
    k.save_ai_pack_cache(pack)
    return api_ok(build_ai_status_struct())


@app.post("/api/ai/ask")
async def ai_ask(req: AiAskRequest, _user: str = Depends(current_user)):
    question = req.question.strip()
    if not question:
        return api_error("question is required", status_code=400, code="empty_question")
    if not k.ai_enabled():
        return api_error("AI is not configured", status_code=400, code="ai_disabled")
    data_pack = k.build_ai_data_pack() if k.question_requires_fresh_ai_pack(question) else k.get_ai_data_pack_cached()
    answer = k.normalize_ai_answer_for_telegram(k.ask_ai_with_data(question, data_pack))
    return api_ok({"answer": answer})
