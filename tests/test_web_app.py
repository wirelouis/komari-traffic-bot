import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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
        self.patch_attr(k, "AI_API_KEY", "secret-ai-key")

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
        for path in ("/", "/nodes", "/alerts", "/telegram", "/ai"):
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
        self.patch_attr(k, "take_sample_if_due", lambda force=False: None)
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


if __name__ == "__main__":
    unittest.main()
