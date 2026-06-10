import os
import base64
import tempfile
import time
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import Mock, patch

os.environ.setdefault("WEB_USERNAME", "admin")
os.environ.setdefault("WEB_PASSWORD", "test-password")
os.environ.setdefault("WEB_SESSION_SECRET", "test-session-secret")
os.environ.setdefault("STAT_TZ", "UTC")

from fastapi.testclient import TestClient  # noqa: E402

import komari_traffic_report as k  # noqa: E402
import web_app as w  # noqa: E402


class WebAppTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)
        self.patchers = []
        self.patch_env("WEB_USERNAME", "admin")
        self.patch_env("WEB_PASSWORD", "test-password")
        w.LOGIN_FAILURES.clear()
        self.point_runtime_paths()
        self.configure_alerts()
        self.client = TestClient(w.app)

    def tearDown(self):
        for patcher in reversed(self.patchers):
            patcher.stop()
        self.tmp.cleanup()

    def patch_attr(self, target, name, value):
        patcher = patch.object(target, name, value)
        patcher.start()
        self.patchers.append(patcher)

    def patch_env(self, name, value):
        patcher = patch.dict(os.environ, {name: value})
        patcher.start()
        self.patchers.append(patcher)

    def point_runtime_paths(self):
        self.patch_attr(k, "DATA_DIR", str(self.tmp_path))
        self.patch_attr(k, "SAMPLES_PATH", str(self.tmp_path / "samples.json"))
        self.patch_attr(k, "ALERTS_STATE_PATH", str(self.tmp_path / "alerts_state.json"))
        self.patch_attr(k, "HISTORY_PATH", str(self.tmp_path / "history.json"))
        self.patch_attr(k, "REPORT_SCHEDULES_PATH", str(self.tmp_path / "report_schedules.json"))
        self.patch_attr(k, "TRAFFIC_DB_PATH", str(self.tmp_path / "traffic.db"))
        self.patch_attr(k, "TG_OFFSET_PATH", str(self.tmp_path / "tg_offset.txt"))
        self.patch_attr(k, "TG_CONFIRM_PATH", str(self.tmp_path / "tg_confirm.json"))
        self.patch_attr(k, "AI_PACK_CACHE_PATH", str(self.tmp_path / "ai_pack_cache.json"))

    def configure_alerts(self):
        self.patch_attr(k, "ALERTS_ENABLED", True)
        self.patch_attr(k, "ALERT_COOLDOWN_SECONDS", 1800)
        self.patch_attr(k, "ALERT_SILENCE_WINDOWS", "")
        self.patch_attr(k, "ALERT_NODE_MISSING_SAMPLES", 2)
        self.patch_attr(k, "ALERT_WINDOW_MINUTES", 60)
        self.patch_attr(k, "ALERT_TOTAL_WINDOW_BYTES", 0)
        self.patch_attr(k, "ALERT_NODE_WINDOW_BYTES", 0)
        self.patch_attr(k, "ALERT_DAILY_TOTAL_BYTES", 0)
        self.patch_attr(k, "ALERT_DAILY_NODE_BYTES", 0)
        self.patch_attr(k, "ALERT_RECOVERY_NOTIFY", True)
        self.patch_attr(k, "TELEGRAM_CHAT_ID", "123456789")
        self.patch_attr(k, "TELEGRAM_BOT_TOKEN", "secret-telegram-token")
        self.patch_attr(k, "KOMARI_API_TOKEN", "secret-komari-token")
        self.patch_attr(k, "AI_API_BASE", "https://ai.example/v1")
        self.patch_attr(k, "AI_API_KEY", "secret-ai-key")
        self.patch_attr(k, "KOMARI_BASE_URL", "https://komari.example")
        self.patch_attr(k, "AI_MODEL", "deepseek-test-model")
        self.patch_attr(k, "AI_PACK_CACHE_TTL_SECONDS", 3600)
        self.patch_attr(k, "BOT_INSTANCE_NAME", "")
        self.patch_attr(k, "TOP_N", 3)
        self.patch_attr(k, "TASK_RUN_RETENTION_DAYS", 90)
        self.patch_attr(k, "TRAFFIC_SNAPSHOT_RETENTION_DAYS", 45)

    def login(self):
        response = self.client.post("/api/auth/login", json={"username": "admin", "password": "test-password"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["ok"])

    def test_unauthorized_api_returns_json_401(self):
        response = self.client.get("/api/overview")

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.headers["content-type"].split(";")[0], "application/json")
        self.assertFalse(response.json()["ok"])

    def test_login_and_session(self):
        self.login()

        response = self.client.get("/api/auth/session")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["data"]["authenticated"])
        self.assertEqual(payload["data"]["username"], "admin")

    def test_login_sets_secure_cookie_when_forwarded_https(self):
        response = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "test-password"},
            headers={"x-forwarded-proto": "https"},
        )

        self.assertEqual(response.status_code, 200, response.text)
        cookie = response.headers["set-cookie"].lower()
        self.assertIn("httponly", cookie)
        self.assertIn("samesite=lax", cookie)
        self.assertIn("secure", cookie)

    def test_forwarded_same_origin_unsafe_request_is_allowed(self):
        response = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "test-password"},
            headers={
                "origin": "https://panel.example",
                "x-forwarded-proto": "https",
                "x-forwarded-host": "panel.example",
            },
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["ok"])
        self.assertIn("secure", response.headers["set-cookie"].lower())

    def test_cross_site_unsafe_requests_are_rejected(self):
        response = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "test-password"},
            headers={"origin": "https://evil.example"},
        )

        self.assertEqual(response.status_code, 403, response.text)
        self.assertEqual(response.json()["error"]["code"], "csrf_blocked")
        self.assertEqual(response.headers["x-frame-options"], "DENY")

        self.login()
        response = self.client.post(
            "/api/alerts/mute",
            json={"hours": 1},
            headers={"origin": "https://evil.example"},
        )

        self.assertEqual(response.status_code, 403, response.text)
        self.assertEqual(response.json()["error"]["code"], "csrf_blocked")

    def test_login_uses_session_cookie_by_default(self):
        response = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "test-password"},
        )

        self.assertEqual(response.status_code, 200, response.text)
        cookie = response.headers["set-cookie"].lower()
        self.assertIn("httponly", cookie)
        self.assertIn("samesite=lax", cookie)
        self.assertNotIn("max-age", cookie)

    def test_login_remember_sets_persistent_cookie(self):
        response = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "test-password", "remember": True},
        )

        self.assertEqual(response.status_code, 200, response.text)
        cookie = response.headers["set-cookie"].lower()
        self.assertIn("httponly", cookie)
        self.assertIn("samesite=lax", cookie)
        self.assertIn(f"max-age={w.SESSION_REMEMBER_SECONDS}", cookie)

    def test_session_token_expiry_and_remember_duration(self):
        with patch.object(w.time, "time", lambda: 1000):
            browser_token = w.create_session_token("admin", w.SESSION_BROWSER_SECONDS)
            remember_token = w.create_session_token("admin", w.SESSION_REMEMBER_SECONDS)

        with patch.object(w.time, "time", lambda: 1000 + w.SESSION_BROWSER_SECONDS - 1):
            self.assertEqual(w.validate_session_token(browser_token), "admin")
        with patch.object(w.time, "time", lambda: 1000 + w.SESSION_BROWSER_SECONDS):
            self.assertIsNone(w.validate_session_token(browser_token))

        with patch.object(w.time, "time", lambda: 1000 + w.SESSION_REMEMBER_SECONDS - 1):
            self.assertEqual(w.validate_session_token(remember_token), "admin")
        with patch.object(w.time, "time", lambda: 1000 + w.SESSION_REMEMBER_SECONDS):
            self.assertIsNone(w.validate_session_token(remember_token))

    def test_legacy_session_token_remains_valid_for_compat_window(self):
        body = "admin|1000|legacy-nonce"
        token = base64.urlsafe_b64encode(f"{body}|{w._sign_session(body)}".encode("utf-8")).decode("ascii")

        with patch.object(w.time, "time", lambda: 1000 + w.LEGACY_SESSION_MAX_AGE_SECONDS - 1):
            self.assertEqual(w.validate_session_token(token), "admin")
        with patch.object(w.time, "time", lambda: 1000 + w.LEGACY_SESSION_MAX_AGE_SECONDS + 1):
            self.assertIsNone(w.validate_session_token(token))

    def test_login_rate_limits_repeated_failures_and_clears_on_success(self):
        headers = {"x-forwarded-for": "198.51.100.7"}
        for _index in range(w.LOGIN_RATE_LIMIT_ATTEMPTS):
            response = self.client.post("/api/auth/login", json={"username": "admin", "password": "bad"}, headers=headers)
            self.assertEqual(response.status_code, 401, response.text)

        response = self.client.post("/api/auth/login", json={"username": "admin", "password": "bad"}, headers=headers)
        self.assertEqual(response.status_code, 429, response.text)
        self.assertEqual(response.json()["error"]["code"], "login_rate_limited")

        w.LOGIN_FAILURES.clear()
        response = self.client.post("/api/auth/login", json={"username": "admin", "password": "test-password"}, headers=headers)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn("198.51.100.7", w.LOGIN_FAILURES)

    def test_login_failure_state_prunes_stale_and_excess_entries(self):
        self.patch_attr(w, "LOGIN_RATE_LIMIT_MAX_KEYS", 3)
        w.LOGIN_FAILURES.update({
            "stale": {"count": 1, "first": 0, "locked_until": 0},
            "expired-lock": {"count": 5, "first": 10, "locked_until": 20},
            "fresh": {"count": 1, "first": 995, "locked_until": 0},
        })

        w.prune_login_failures(now_ts=1000)

        self.assertNotIn("stale", w.LOGIN_FAILURES)
        self.assertNotIn("expired-lock", w.LOGIN_FAILURES)
        self.assertIn("fresh", w.LOGIN_FAILURES)

        w.LOGIN_FAILURES.clear()
        for index in range(5):
            w.LOGIN_FAILURES[f"key-{index}"] = {"count": 1, "first": index, "locked_until": 0}

        w.prune_login_failures(now_ts=10)

        self.assertLessEqual(len(w.LOGIN_FAILURES), 3)
        self.assertNotIn("key-0", w.LOGIN_FAILURES)
        self.assertNotIn("key-1", w.LOGIN_FAILURES)

    def test_frontend_routes_return_index(self):
        for path in ("/", "/nodes", "/alerts", "/telegram", "/ai", "/analytics", "/system"):
            with self.subTest(path=path):
                response = self.client.get(path)

                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.headers["content-type"].split(";")[0], "text/html")
                self.assertEqual(response.headers["cache-control"], "no-store")
                self.assertEqual(response.headers["x-frame-options"], "DENY")
                self.assertEqual(response.headers["x-content-type-options"], "nosniff")
                self.assertEqual(response.headers["referrer-policy"], "no-referrer")
                self.assertIn("Komari Traffic Console", response.text)
                self.assertIn("/static/app.js", response.text)

    def test_frontend_main_page_titles_are_not_duplicated(self):
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200, response.text)
        html = response.text
        self.assertNotIn('id="topbar-eyebrow"', html)
        for title in ("节点流量分析", "告警控制", "推送控制", "数据问答", "系统健康"):
            self.assertNotIn(f"<h2>{title}</h2>", html)
        for label in ("Nodes", "Alerts", "Telegram", "Analytics", "System", "Editable", "Maintenance"):
            self.assertNotIn(f'>{label}</p>', html)

    def test_frontend_login_remember_is_opt_in(self):
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200, response.text)
        html = response.text
        self.assertIn('id="login-remember"', html)
        self.assertNotIn('id="login-remember" type="checkbox" checked', html)
        self.assertIn('name="komari-user-field"', html)
        self.assertIn('name="komari-pass-field"', html)
        self.assertIn('autocomplete="new-password"', html)

        app_js = (w.STATIC_DIR / "app.js").read_text(encoding="utf-8")
        local_storage_lines = [line for line in app_js.splitlines() if "localStorage" in line]
        self.assertFalse(any("password" in line.lower() for line in local_storage_lines))

    def test_frontend_compacts_data_heavy_views_by_default(self):
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200, response.text)
        html = response.text
        self.assertIn("<th>流量</th>", html)
        self.assertIn("<th>健康</th>", html)
        self.assertIn("<th>绑定</th>", html)
        self.assertIn('id="export-traffic-range-btn"', html)
        for old_heading in ("<th>CPU</th>", "<th>RAM</th>", "<th>Disk</th>"):
            self.assertNotIn(old_heading, html)

        app_js = (w.STATIC_DIR / "app.js").read_text(encoding="utf-8")
        self.assertIn("overviewNodes: 8", app_js)
        self.assertIn("analyticsNodes: 10", app_js)
        self.assertIn("compactTrafficRows", app_js)
        self.assertIn("a.compact_other ? 1 : -1", app_js)
        self.assertIn("/api/traffic/range/export.csv", app_js)
        self.assertIn("$('analytics-status-pill').title = message", app_js.replace('"', "'"))
        self.assertIn("console.warn(message);", app_js)
        self.assertNotIn("<h3>节点明细</h3>", app_js)
        self.assertNotIn("调试详情", app_js)

    def test_brand_icon_static_asset(self):
        response = self.client.get("/static/komari-traffic-icon.svg")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers["content-type"].split(";")[0], "image/svg+xml")
        self.assertIn("Komari Traffic", response.text)

    def test_missing_web_password_is_clear(self):
        with patch.dict(os.environ, {"WEB_PASSWORD": ""}):
            response = self.client.post("/api/auth/login", json={"username": "admin", "password": ""})

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"]["code"], "web_password_missing")

    def test_overview_returns_structured_errors_when_komari_unavailable(self):
        self.login()
        self.patch_attr(k, "KOMARI_BASE_URL", "")

        response = self.client.get("/api/overview")

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertTrue(payload["ok"])
        last_24h = payload["data"]["records"]["last_24h"]
        self.assertTrue(last_24h["ok"])
        self.assertEqual(last_24h["data"]["source"], "traffic_segments")
        self.assertEqual(last_24h["data"]["error"], "insufficient_snapshots")
        self.assertFalse(payload["data"]["services"]["komari"]["configured"])

    def test_overview_compacts_large_node_payloads(self):
        self.login()

        def many_nodes(count=12):
            return [
                {
                    "uuid": f"n{index}",
                    "name": f"Node {index}",
                    "up": index,
                    "down": index * 2,
                    "total": index * 3,
                    "up_human": f"{index} B",
                    "down_human": f"{index * 2} B",
                    "total_human": f"{index * 3} B",
                }
                for index in range(count, 0, -1)
            ]

        def period_summary(scope):
            nodes = many_nodes()
            return {
                "scope": scope,
                "nodes": nodes,
                "top_nodes": nodes[:3],
                "total": {"total": 234, "total_human": "234 B"},
            }

        self.patch_attr(w, "build_period_summary", period_summary)
        self.patch_attr(k, "build_records_summary", lambda _hours: {
            "hours": _hours,
            "nodes": many_nodes(),
            "top_nodes": many_nodes()[:3],
            "skipped": [],
        })

        response = self.client.get("/api/overview")

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["data"]
        today = payload["periods"]["today"]["data"]
        last_24h = payload["records"]["last_24h"]["data"]
        self.assertEqual(today["node_count"], 12)
        self.assertEqual(today["hidden_node_count"], 4)
        self.assertEqual(len(today["nodes"]), 9)
        self.assertTrue(today["nodes"][-1]["compact_other"])
        self.assertEqual(last_24h["node_count"], 12)
        self.assertNotIn("machines", last_24h)

    def test_period_summary_uses_sqlite_snapshots_without_marker_file(self):
        self.login()
        now = datetime(2026, 6, 2, 1, 0, tzinfo=k.TZ)
        with patch.object(k, "today_date", return_value=date(2026, 6, 2)), patch.object(k, "now_dt", return_value=now):
            start_ts = int(k.start_of_day(date(2026, 6, 2)).timestamp())
            k.save_traffic_snapshot(start_ts, {
                "n1": {"name": "Node One", "up": 10, "down": 20},
            })
            k.save_traffic_snapshot(start_ts + 3600, {
                "n1": {"name": "Node One", "up": 16, "down": 35},
            })

            response = self.client.get("/api/overview")

        self.assertEqual(response.status_code, 200, response.text)
        today = response.json()["data"]["periods"]["today"]["data"]
        self.assertEqual(today["note"], "snapshot_window")
        self.assertEqual(today["total"]["total"], 21)

    def test_alert_check_dry_run_does_not_persist_state(self):
        self.login()
        self.patch_attr(k, "ALERT_TOTAL_WINDOW_BYTES", 100)
        self.patch_attr(k, "take_sample_if_due", lambda **_kwargs: None)
        self.patch_attr(k.time, "time", lambda: 4600)
        k.save_samples({
            "samples": [
                {"ts": 1000, "nodes": {"u1": {"name": "node-a", "up": 0, "down": 0}}, "skipped": []},
                {"ts": 4600, "nodes": {"u1": {"name": "node-a", "up": 50, "down": 100}}, "skipped": []},
            ]
        })
        k.save_traffic_snapshot(1000, {"u1": {"name": "node-a", "up": 0, "down": 0}})
        k.save_traffic_snapshot(4600, {"u1": {"name": "node-a", "up": 50, "down": 100}})

        response = self.client.post("/api/alerts/check", json={"notify": False})

        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["ok"])
        self.assertFalse((self.tmp_path / "alerts_state.json").exists())
        summary = response.json()["data"]["summary"]
        self.assertEqual(summary["level"], "warn")
        self.assertIn("事件", summary["title"])

    def test_sensitive_values_are_not_leaked(self):
        self.login()
        self.patch_attr(w, "WEB_SESSION_SECRET", "secret-session-value")

        overview = self.client.get("/api/overview").text
        session = self.client.get("/api/auth/session").text
        alerts = self.client.get("/api/alerts").text
        merged = overview + session + alerts

        self.assertNotIn("secret-telegram-token", merged)
        self.assertNotIn("secret-komari-token", merged)
        self.assertNotIn("secret-ai-key", merged)
        self.assertNotIn("test-password", merged)
        self.assertNotIn("secret-session-value", merged)
        self.assertNotIn("123456789", alerts)

    def test_api_errors_redact_web_secrets(self):
        self.patch_attr(w, "WEB_SESSION_SECRET", "secret-session-value")

        response = w.api_error(
            "bad values: test-password secret-session-value",
            status_code=500,
            code="boom",
            data={"detail": "test-password secret-session-value"},
        )
        text = response.body.decode("utf-8")

        self.assertNotIn("test-password", text)
        self.assertNotIn("secret-session-value", text)

    def test_nested_safe_call_errors_redact_secrets(self):
        self.patch_attr(w, "WEB_SESSION_SECRET", "secret-session-value")
        self.login()

        def failing_period_summary(_scope):
            raise RuntimeError(
                "bad values: test-password secret-session-value "
                "secret-telegram-token secret-komari-token secret-ai-key"
            )

        self.patch_attr(w, "build_period_summary", failing_period_summary)
        self.patch_attr(w, "safe_records_summary", lambda _hours, _enrich=False: {"nodes": [], "top_nodes": []})

        response = self.client.get("/api/overview")

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["data"]
        self.assertFalse(payload["periods"]["today"]["ok"])
        text = response.text
        self.assertNotIn("test-password", text)
        self.assertNotIn("secret-session-value", text)
        self.assertNotIn("secret-telegram-token", text)
        self.assertNotIn("secret-komari-token", text)
        self.assertNotIn("secret-ai-key", text)

    def test_komari_machines_are_normalized_with_web_url(self):
        self.login()
        self.patch_attr(k, "get_json", lambda _url: {
            "status": "success",
            "data": [
                {"uuid": "uuid-b", "name": "Beta", "region": "HK", "tags": ["edge"]},
                {"uuid": "uuid-a", "name": "Alpha", "group": "prod"},
            ],
        })

        response = self.client.get("/api/komari/machines")

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["data"]
        self.assertTrue(payload["configured"])
        self.assertEqual([m["uuid"] for m in payload["machines"]], ["uuid-a", "uuid-b"])
        self.assertEqual(payload["machines"][0]["web_url"], "https://komari.example/instance/uuid-a")
        self.assertEqual(payload["machines"][1]["tags"], ["edge"])

    def test_node_binding_save_and_clear(self):
        self.login()
        self.patch_attr(k, "get_json", lambda _url: {
            "status": "success",
            "data": [{"uuid": "machine-1", "name": "Probe One"}],
        })

        save_response = self.client.post(
            "/api/node-bindings",
            json={"source_id": "traffic-node", "komari_uuid": "machine-1"},
        )

        self.assertEqual(save_response.status_code, 200, save_response.text)
        saved = k.load_json(str(self.tmp_path / "node_bindings.json"), {})
        self.assertEqual(saved["bindings"]["traffic-node"]["komari_uuid"], "machine-1")
        self.assertEqual(save_response.json()["data"]["binding"]["mode"], "manual")

        clear_response = self.client.post(
            "/api/node-bindings",
            json={"source_id": "traffic-node", "komari_uuid": ""},
        )

        self.assertEqual(clear_response.status_code, 200, clear_response.text)
        cleared = k.load_json(str(self.tmp_path / "node_bindings.json"), {})
        self.assertNotIn("traffic-node", cleared["bindings"])

    def test_node_bindings_report_stale_manual_binding(self):
        self.login()
        k.save_json_atomic(str(self.tmp_path / "node_bindings.json"), {
            "version": 1,
            "bindings": {"traffic-node": {"komari_uuid": "missing-machine", "updated_at": 111}},
        })
        self.patch_attr(k, "get_json", lambda _url: {
            "status": "success",
            "data": [{"uuid": "other-machine", "name": "Other"}],
        })

        response = self.client.get("/api/node-bindings")

        self.assertEqual(response.status_code, 200, response.text)
        resolved = response.json()["data"]["resolved"]["traffic-node"]
        self.assertEqual(resolved["mode"], "manual")
        self.assertTrue(resolved["stale"])
        self.assertEqual(resolved["komari_uuid"], "missing-machine")

    def test_nodes_api_enriches_komari_binding_and_machine(self):
        self.login()
        self.patch_attr(k, "build_records_summary", lambda _hours: {
            "date": "2026-06-06",
            "from": "from",
            "to": "to",
            "nodes": [
                {"uuid": "traffic-node", "name": "Traffic Node", "up": 1, "down": 2, "total": 3, "total_human": "3 B"},
                {"uuid": "auto-node", "name": "Auto Node", "up": 4, "down": 5, "total": 9, "total_human": "9 B"},
            ],
            "top_nodes": [{"uuid": "traffic-node", "name": "Traffic Node", "up": 1, "down": 2, "total": 3, "total_human": "3 B"}],
        })
        k.save_json_atomic(str(self.tmp_path / "node_bindings.json"), {
            "version": 1,
            "bindings": {"traffic-node": {"komari_uuid": "machine-1", "updated_at": 123}},
        })
        self.patch_attr(k, "get_json", lambda _url: {
            "status": "success",
            "data": [
                {"uuid": "machine-1", "name": "Probe One", "region": "SG"},
                {"uuid": "auto-node", "name": "Auto Probe"},
            ],
        })

        response = self.client.get("/api/nodes?hours=24")

        self.assertEqual(response.status_code, 200, response.text)
        nodes = response.json()["data"]["nodes"]
        by_uuid = {node["uuid"]: node for node in nodes}
        self.assertEqual(by_uuid["traffic-node"]["binding"]["mode"], "manual")
        self.assertEqual(by_uuid["traffic-node"]["komari"]["machine"]["name"], "Probe One")
        self.assertEqual(by_uuid["traffic-node"]["komari"]["web_url"], "https://komari.example/instance/machine-1")
        self.assertEqual(by_uuid["auto-node"]["binding"]["mode"], "auto")
        self.assertEqual(by_uuid["auto-node"]["komari"]["web_url"], "https://komari.example/instance/auto-node")
        self.assertEqual(response.json()["data"]["top_nodes"][0]["komari"]["web_url"], "https://komari.example/instance/machine-1")

    def test_telegram_preview_does_not_send(self):
        self.login()
        self.patch_attr(k, "build_period_report_message", lambda _start, _now, tag, top_only=False: f"preview:{tag}:{top_only}")
        send_mock = Mock()
        self.patch_attr(k, "telegram_send", send_mock)

        response = self.client.post("/api/telegram/preview", json={"scope": "today", "mode": "top"})

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["data"]
        self.assertEqual(payload["scope"], "today")
        self.assertEqual(payload["mode"], "top")
        self.assertIn("preview:", payload["message"])
        send_mock.assert_not_called()

    def test_ai_status_reads_cache_without_secret_leak(self):
        self.login()
        now_ts = int(time.time())
        k.save_json_atomic(str(self.tmp_path / "ai_pack_cache.json"), {
            "created_at": now_ts,
            "pack": {
                "last_24h": {"nodes": [{"uuid": "n1"}, {"uuid": "n2"}]},
                "last_7d": {"top_nodes": [{"uuid": "n1"}]},
                "history": {"days": [{"date": "2026-06-06"}]},
            },
        })

        response = self.client.get("/api/ai/status")

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["data"]
        self.assertTrue(payload["configured"])
        self.assertEqual(payload["model"], "deepseek-test-model")
        self.assertTrue(payload["cache_valid"])
        self.assertNotIn("secret-ai-key", response.text)
        self.assertEqual({item["key"]: item["count"] for item in payload["data_sources"]}, {
            "last_24h": 2,
            "last_7d": 1,
            "history": 1,
        })

    def test_ai_refresh_rebuilds_and_saves_cache(self):
        self.login()
        self.patch_attr(k, "build_ai_data_pack", lambda: {"last_24h": {"nodes": [{"uuid": "fresh"}]}})

        response = self.client.post("/api/ai/refresh")

        self.assertEqual(response.status_code, 200, response.text)
        cache = k.load_json(str(self.tmp_path / "ai_pack_cache.json"), {})
        self.assertEqual(cache["pack"]["last_24h"]["nodes"][0]["uuid"], "fresh")
        self.assertTrue(response.json()["data"]["cache_valid"])

    def test_schedule_api_create_update_delete_and_run_now(self):
        self.login()

        create_response = self.client.post("/api/schedules", json={
            "enabled": True,
            "scope": "daily",
            "mode": "full",
            "time": "08:15",
            "weekday": 0,
            "month_day": 1,
        })

        self.assertEqual(create_response.status_code, 200, create_response.text)
        created = create_response.json()["data"]["schedule"]
        self.assertEqual(created["label"], "每日 08:15 发送完整日报")
        schedule_id = created["id"]

        update_response = self.client.patch(f"/api/schedules/{schedule_id}", json={
            "enabled": False,
            "scope": "weekly",
            "mode": "top",
            "time": "09:30",
            "weekday": 2,
            "month_day": 1,
        })

        self.assertEqual(update_response.status_code, 200, update_response.text)
        updated = update_response.json()["data"]["schedule"]
        self.assertFalse(updated["enabled"])
        self.assertEqual(updated["scope"], "weekly")
        self.assertIn("周三", updated["label"])

        run_mock = Mock(return_value={
            "sent": True,
            "chat": "123456789",
            "label": "每周周三 09:30 发送Top周报",
        })
        self.patch_attr(k, "run_report_schedule", run_mock)
        run_response = self.client.post(f"/api/schedules/{schedule_id}/run-now")

        self.assertEqual(run_response.status_code, 200, run_response.text)
        self.assertEqual(run_response.json()["data"]["chat"], "123***789")
        run_mock.assert_called_once()

        list_response = self.client.get("/api/schedules")
        self.assertEqual(list_response.status_code, 200, list_response.text)
        self.assertEqual(len(list_response.json()["data"]["schedules"]), 1)

        delete_response = self.client.delete(f"/api/schedules/{schedule_id}")
        self.assertEqual(delete_response.status_code, 200, delete_response.text)
        self.assertEqual(delete_response.json()["data"]["schedules"], [])

    def test_schedule_api_rejects_invalid_time(self):
        self.login()

        response = self.client.post("/api/schedules", json={
            "enabled": True,
            "scope": "daily",
            "mode": "full",
            "time": "25:00",
            "weekday": 0,
            "month_day": 1,
        })

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "invalid_schedule")

    def test_schedule_api_masks_custom_chat_values(self):
        self.login()
        create_response = self.client.post("/api/schedules", json={
            "enabled": True,
            "scope": "daily",
            "mode": "full",
            "time": "08:15",
            "weekday": 0,
            "month_day": 1,
            "chat": "987654321",
        })

        self.assertEqual(create_response.status_code, 200, create_response.text)
        created = create_response.json()["data"]["schedule"]
        self.assertEqual(created["chat"], "")
        self.assertFalse(created["uses_default_chat"])
        self.assertEqual(created["chat_masked"], "987***321")
        self.assertNotIn("987654321", create_response.text)

        list_response = self.client.get("/api/schedules")
        self.assertEqual(list_response.status_code, 200, list_response.text)
        self.assertNotIn("987654321", list_response.text)

        schedule_id = created["id"]
        run_mock = Mock(return_value={
            "sent": True,
            "chat": "987654321",
            "schedule": {
                "id": schedule_id,
                "enabled": True,
                "scope": "daily",
                "mode": "full",
                "time": "08:15",
                "weekday": 0,
                "month_day": 1,
                "chat": "987654321",
            },
            "label": "每日 08:15 发送完整日报",
        })
        self.patch_attr(k, "run_report_schedule", run_mock)

        run_response = self.client.post(f"/api/schedules/{schedule_id}/run-now")

        self.assertEqual(run_response.status_code, 200, run_response.text)
        payload = run_response.json()["data"]
        self.assertEqual(payload["chat"], "987***321")
        self.assertEqual(payload["schedule"]["chat"], "")
        self.assertEqual(payload["schedule"]["chat_masked"], "987***321")
        self.assertNotIn("987654321", run_response.text)

    def test_traffic_range_api_returns_sqlite_rollup(self):
        self.login()
        k.upsert_daily_usage("2026-06-01", {
            "n1": {"name": "Node One", "up": 10, "down": 20},
            "n2": {"name": "Node Two", "up": 5, "down": 7},
        }, source="test")
        k.upsert_daily_usage("2026-06-02", {
            "n1": {"name": "Node One", "up": 3, "down": 4},
        }, source="test")

        response = self.client.get("/api/traffic/range?from=2026-06-01&to=2026-06-02&group=weekly")

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["data"]
        self.assertEqual(payload["total"]["total"], 49)
        self.assertEqual(payload["groups"][0]["key"], "2026-06-01")
        self.assertNotIn("nodes", payload["groups"][0])
        self.assertEqual(payload["top_nodes"][0]["uuid"], "n1")
        self.assertTrue(payload["compact"])

        full_response = self.client.get("/api/traffic/range?from=2026-06-01&to=2026-06-02&group=weekly&compact=false")
        self.assertEqual(full_response.status_code, 200, full_response.text)
        full_payload = full_response.json()["data"]
        self.assertIn("nodes", full_payload["groups"][0])
        self.assertNotIn("compact", full_payload)

        csv_response = self.client.get("/api/traffic/range/export.csv?from=2026-06-01&to=2026-06-02&group=weekly")
        self.assertEqual(csv_response.status_code, 200, csv_response.text)
        self.assertEqual(csv_response.headers["content-type"].split(";")[0], "text/csv")
        self.assertIn("attachment;", csv_response.headers["content-disposition"])
        csv_text = csv_response.content.decode("utf-8-sig")
        self.assertIn("uuid,name,down_bytes,up_bytes,total_bytes", csv_text)
        self.assertIn("n1,Node One,24,13,37", csv_text)
        self.assertIn("n2,Node Two,7,5,12", csv_text)

    def test_task_runs_api_filters_and_formats(self):
        self.login()
        k.record_task_run("report", "web:composer", "success", started_at=1000, finished_at=1001, summary="sent")
        k.record_task_run("alert", "web:alerts-check", "failed", started_at=1002, finished_at=1003, error="boom")

        response = self.client.get("/api/tasks/runs?type=report&limit=10")

        self.assertEqual(response.status_code, 200, response.text)
        runs = response.json()["data"]["runs"]
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["type"], "report")
        self.assertEqual(runs[0]["summary"], "sent")
        self.assertIn("started_at_text", runs[0])

    def test_system_status_is_masked_and_reports_db(self):
        self.login()
        self.patch_attr(k, "APP_VERSION", "test-version")
        self.patch_attr(k, "GIT_COMMIT", "abcdefabcdef0000")
        self.patch_attr(k, "BUILD_DATE", "2026-06-06T00:00:00Z")
        k.record_task_run("sample", "sample-worker", "success", started_at=1000, finished_at=1001, summary="采样")

        response = self.client.get("/api/system/status")

        self.assertEqual(response.status_code, 200, response.text)
        text = response.text
        payload = response.json()["data"]
        self.assertEqual(payload["data"]["sqlite"]["task_runs"], 1)
        self.assertEqual(payload["data"]["sqlite"]["quick_check"], "ok")
        self.assertEqual(payload["build"]["version"], "test-version")
        self.assertEqual(payload["build"]["commit_short"], "abcdefabcdef")
        self.assertIn("maintenance", payload["data"])
        self.assertIn("health_items", payload)
        self.assertIn("data_status", payload)
        self.assertIn("warnings", payload["summary"])
        self.assertTrue(payload["config"]["komari_api_token_configured"])
        self.assertEqual(payload["config"]["ai_model"], "deepseek-test-model")
        self.assertNotIn("editable_config", payload)
        self.assertNotIn("secret-telegram-token", text)
        self.assertNotIn("secret-komari-token", text)
        self.assertNotIn("secret-ai-key", text)
        self.assertNotIn("123456789", text)

    def test_system_config_can_update_low_sensitive_runtime_values(self):
        self.login()

        response = self.client.post("/api/system/config", json={
            "bot_instance_name": "prod-panel",
            "komari_base_url": "https://komari-new.example/",
            "telegram_chat_id": "987654321",
            "telegram_alert_chat_id": "123123123",
            "ai_api_base": "https://ai-new.example/v1/",
            "ai_model": "gpt-5.4-mini",
            "top_n": 8,
            "komari_timeout_seconds": 20,
            "sample_interval_seconds": 120,
            "traffic_snapshot_retention_days": 120,
            "ai_pack_cache_ttl_seconds": 7200,
            "task_run_retention_days": 30,
            "alerts_enabled": False,
            "alert_silence_windows": "23:00-07:00",
            "alert_total_window_bytes": "2GiB",
        })

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["data"]
        self.assertEqual(payload["values"]["bot_instance_name"], "prod-panel")
        self.assertEqual(payload["values"]["komari_base_url"], "https://komari-new.example")
        self.assertEqual(payload["values"]["telegram_chat_id"], "987654321")
        self.assertEqual(payload["values"]["ai_model"], "gpt-5.4-mini")
        self.assertEqual(payload["values"]["top_n"], 8)
        self.assertEqual(payload["values"]["traffic_snapshot_retention_days"], 120)
        self.assertFalse(payload["values"]["alerts_enabled"])
        self.assertEqual(payload["values"]["alert_total_window_bytes"], 2 * 1024 ** 3)
        self.assertEqual(payload["config"]["values"]["ai_pack_cache_ttl_seconds"], 7200)
        fields = {item["key"]: item for item in payload["config"]["editable"]}
        self.assertEqual(fields["alert_total_window_bytes"]["type"], "bytes")
        self.assertEqual(fields["alert_total_window_bytes"]["value"], "2.00 GiB")
        self.assertEqual(k.BOT_INSTANCE_NAME, "prod-panel")
        self.assertEqual(k.KOMARI_BASE_URL, "https://komari-new.example")
        self.assertEqual(k.TELEGRAM_CHAT_ID, "987654321")
        self.assertEqual(k.AI_MODEL, "gpt-5.4-mini")
        self.assertEqual(k.TOP_N, 8)
        self.assertEqual(k.SAMPLE_INTERVAL_SECONDS, 120)
        self.assertEqual(k.TRAFFIC_SNAPSHOT_RETENTION_DAYS, 120)
        self.assertFalse(k.ALERTS_ENABLED)
        saved = k.load_json(str(self.tmp_path / "runtime_config.json"), {})
        self.assertEqual(saved["config"]["task_run_retention_days"], 30)
        self.assertEqual(saved["config"]["traffic_snapshot_retention_days"], 120)
        self.assertEqual(saved["config"]["alert_total_window_bytes"], 2 * 1024 ** 3)
        runs = k.list_task_runs(limit=10, task_type="maintenance")
        self.assertEqual(runs[0]["source"], "web:config")

    def test_system_config_rejects_invalid_runtime_values(self):
        self.login()

        response = self.client.post("/api/system/config", json={"top_n": 0})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "invalid_runtime_config")

        response = self.client.post("/api/system/config", json={"alert_silence_windows": "bad"})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "invalid_runtime_config")

        response = self.client.post("/api/system/config", json={"telegram_chat_id": "bad id"})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "invalid_runtime_config")

    def test_system_maintenance_prunes_task_runs_and_records_action(self):
        self.login()
        self.patch_attr(k, "TASK_RUN_RETENTION_DAYS", 1)
        k.upsert_daily_usage("2026-06-01", {
            "n1": {"name": "Node One", "up": 10, "down": 20},
        }, source="test")
        k.record_task_run("report", "old", "success", started_at=1000, finished_at=1001, summary="old")

        response = self.client.post("/api/system/maintenance/prune-task-runs", json={})

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["data"]
        self.assertEqual(payload["result"]["deleted"], 1)
        self.assertEqual(payload["maintenance"]["old_task_runs"], 0)
        runs = k.list_task_runs(limit=10)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["type"], "maintenance")
        self.assertEqual(runs[0]["source"], "web:prune-task-runs")
        daily = k.aggregate_daily_usage(date(2026, 6, 1), date(2026, 6, 1))["n1"]
        self.assertEqual(daily["up"] + daily["down"], 30)

    def test_system_maintenance_vacuum_records_action(self):
        self.login()
        k.upsert_daily_usage("2026-06-01", {
            "n1": {"name": "Node One", "up": 10, "down": 20},
        }, source="test")

        response = self.client.post("/api/system/maintenance/vacuum")

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["data"]
        self.assertIn("after_size_human", payload["result"])
        runs = k.list_task_runs(limit=10, task_type="maintenance")
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["source"], "web:vacuum")

    def test_schedule_run_now_writes_task_run_when_core_runs(self):
        self.login()
        self.patch_attr(k, "build_period_report_message", lambda _start, _now, tag, top_only=False, **_kwargs: f"message:{tag}:{top_only}")
        send_mock = Mock(return_value={"ok": True})
        self.patch_attr(k, "telegram_send_to_chat", send_mock)
        create_response = self.client.post("/api/schedules", json={
            "enabled": True,
            "scope": "daily",
            "mode": "top",
            "time": "08:15",
            "weekday": 0,
            "month_day": 1,
        })
        schedule_id = create_response.json()["data"]["schedule"]["id"]

        run_response = self.client.post(f"/api/schedules/{schedule_id}/run-now")

        self.assertEqual(run_response.status_code, 200, run_response.text)
        self.assertEqual(run_response.json()["data"]["task_run"]["status"], "success")
        runs = k.list_task_runs(limit=10, task_type="report")
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["metadata"]["schedule_id"], schedule_id)
        self.assertEqual(runs[0]["source"], "web:run-now")
        send_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
