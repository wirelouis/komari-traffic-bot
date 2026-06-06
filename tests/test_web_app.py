import os
import tempfile
import time
import unittest
from datetime import date
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
        self.patch_attr(k, "BASELINES_PATH", str(self.tmp_path / "baselines.json"))
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

    def test_frontend_routes_return_index(self):
        for path in ("/", "/nodes", "/alerts", "/telegram", "/ai", "/analytics", "/system"):
            with self.subTest(path=path):
                response = self.client.get(path)

                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.headers["content-type"].split(";")[0], "text/html")
                self.assertEqual(response.headers["cache-control"], "no-store")
                self.assertIn("Komari Traffic Console", response.text)
                self.assertIn("/static/app.js", response.text)

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
        self.assertFalse(last_24h["ok"])
        self.assertIn("error", last_24h)

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
        self.assertEqual(payload["top_nodes"][0]["uuid"], "n1")

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
        self.assertEqual(payload["build"]["version"], "test-version")
        self.assertEqual(payload["build"]["commit_short"], "abcdefabcdef")
        self.assertIn("maintenance", payload["data"])
        self.assertTrue(payload["config"]["komari_api_token_configured"])
        self.assertEqual(payload["config"]["ai_model"], "deepseek-test-model")
        self.assertEqual(payload["editable_config"]["values"]["top_n"], 3)
        self.assertNotIn("secret-telegram-token", text)
        self.assertNotIn("secret-komari-token", text)
        self.assertNotIn("secret-ai-key", text)
        self.assertNotIn("123456789", text)

    def test_system_config_can_update_low_sensitive_runtime_values(self):
        self.login()

        response = self.client.post("/api/system/config", json={
            "bot_instance_name": "prod-panel",
            "top_n": 8,
            "ai_pack_cache_ttl_seconds": 7200,
            "task_run_retention_days": 30,
        })

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["data"]
        self.assertEqual(payload["values"]["bot_instance_name"], "prod-panel")
        self.assertEqual(payload["values"]["top_n"], 8)
        self.assertEqual(payload["config"]["values"]["ai_pack_cache_ttl_seconds"], 7200)
        self.assertEqual(k.BOT_INSTANCE_NAME, "prod-panel")
        self.assertEqual(k.TOP_N, 8)
        saved = k.load_json(str(self.tmp_path / "runtime_config.json"), {})
        self.assertEqual(saved["config"]["task_run_retention_days"], 30)
        runs = k.list_task_runs(limit=10, task_type="maintenance")
        self.assertEqual(runs[0]["source"], "web:config")

    def test_system_config_rejects_invalid_runtime_values(self):
        self.login()

        response = self.client.post("/api/system/config", json={"top_n": 0})

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
        self.patch_attr(k, "build_period_report_message", lambda _start, _now, tag, top_only=False: f"message:{tag}:{top_only}")
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
