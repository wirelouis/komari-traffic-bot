import os
import sys
import tempfile
import types
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch


def install_requests_stub_if_needed():
    try:
        import requests  # noqa: F401
        from urllib3.util import Retry  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    requests_mod = types.ModuleType("requests")
    adapters_mod = types.ModuleType("requests.adapters")
    urllib3_mod = types.ModuleType("urllib3")
    urllib3_util_mod = types.ModuleType("urllib3.util")

    class RequestException(Exception):
        pass

    class ReadTimeout(RequestException):
        pass

    class ConnectionError(RequestException):
        pass

    class ChunkedEncodingError(RequestException):
        pass

    class HTTPError(RequestException):
        def __init__(self, *args, response=None):
            super().__init__(*args)
            self.response = response

    class HTTPAdapter:
        def __init__(self, *args, **kwargs):
            pass

    class Retry:
        def __init__(self, *args, **kwargs):
            pass

    class Session:
        def mount(self, *args, **kwargs):
            pass

        def get(self, *args, **kwargs):
            raise RequestException("requests stub cannot perform network calls")

    def post(*args, **kwargs):
        raise RequestException("requests stub cannot perform network calls")

    requests_mod.Session = Session
    requests_mod.post = post
    requests_mod.HTTPError = HTTPError
    requests_mod.exceptions = types.SimpleNamespace(
        RequestException=RequestException,
        ReadTimeout=ReadTimeout,
        ConnectionError=ConnectionError,
        ChunkedEncodingError=ChunkedEncodingError,
        HTTPError=HTTPError,
    )
    adapters_mod.HTTPAdapter = HTTPAdapter
    urllib3_util_mod.Retry = Retry

    sys.modules.setdefault("requests", requests_mod)
    sys.modules.setdefault("requests.adapters", adapters_mod)
    sys.modules.setdefault("urllib3", urllib3_mod)
    sys.modules.setdefault("urllib3.util", urllib3_util_mod)


install_requests_stub_if_needed()
os.environ["STAT_TZ"] = "UTC"

import komari_traffic_report as k  # noqa: E402


class CoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)
        self.patchers = []
        self.point_runtime_paths()
        self.configure_alerts()

    def tearDown(self):
        for patcher in reversed(self.patchers):
            patcher.stop()
        self.tmp.cleanup()

    def patch_attr(self, name, value):
        patcher = patch.object(k, name, value)
        patcher.start()
        self.patchers.append(patcher)

    def point_runtime_paths(self):
        self.patch_attr("DATA_DIR", str(self.tmp_path))
        self.patch_attr("SAMPLES_PATH", str(self.tmp_path / "samples.json"))
        self.patch_attr("ALERTS_STATE_PATH", str(self.tmp_path / "alerts_state.json"))
        self.patch_attr("BASELINES_PATH", str(self.tmp_path / "baselines.json"))
        self.patch_attr("HISTORY_PATH", str(self.tmp_path / "history.json"))
        self.patch_attr("REPORT_SCHEDULES_PATH", str(self.tmp_path / "report_schedules.json"))
        self.patch_attr("TRAFFIC_DB_PATH", str(self.tmp_path / "traffic.db"))
        self.patch_attr("TG_OFFSET_PATH", str(self.tmp_path / "tg_offset.txt"))
        self.patch_attr("TG_CONFIRM_PATH", str(self.tmp_path / "tg_confirm.json"))
        self.patch_attr("AI_PACK_CACHE_PATH", str(self.tmp_path / "ai_pack_cache.json"))

    def configure_alerts(self):
        self.patch_attr("ALERTS_ENABLED", True)
        self.patch_attr("ALERT_COOLDOWN_SECONDS", 1800)
        self.patch_attr("ALERT_SILENCE_WINDOWS", "")
        self.patch_attr("ALERT_NODE_MISSING_SAMPLES", 2)
        self.patch_attr("ALERT_WINDOW_MINUTES", 60)
        self.patch_attr("ALERT_TOTAL_WINDOW_BYTES", 0)
        self.patch_attr("ALERT_NODE_WINDOW_BYTES", 0)
        self.patch_attr("ALERT_DAILY_TOTAL_BYTES", 0)
        self.patch_attr("ALERT_DAILY_NODE_BYTES", 0)
        self.patch_attr("ALERT_RECOVERY_NOTIFY", True)

    def test_parse_bytes_value_supports_units(self):
        self.assertEqual(k.parse_bytes_value(""), 0)
        self.assertEqual(k.parse_bytes_value("1024"), 1024)
        self.assertEqual(k.parse_bytes_value("500MiB"), 500 * 1024 ** 2)
        self.assertEqual(k.parse_bytes_value("2 GiB"), 2 * 1024 ** 3)
        self.assertEqual(k.parse_bytes_value("1.5TiB"), int(1.5 * 1024 ** 4))

    def test_parse_bytes_value_rejects_bad_unit(self):
        with self.assertRaises(RuntimeError):
            k.parse_bytes_value("10XB", "TEST_BYTES")

    def test_silence_window_supports_cross_midnight(self):
        late = datetime(2026, 6, 6, 23, 30, tzinfo=k.TZ)
        early = datetime(2026, 6, 6, 6, 30, tzinfo=k.TZ)
        noon = datetime(2026, 6, 6, 12, 0, tzinfo=k.TZ)

        self.assertTrue(k.is_in_silence_window(late, "23:00-07:00"))
        self.assertTrue(k.is_in_silence_window(early, "23:00-07:00"))
        self.assertFalse(k.is_in_silence_window(noon, "23:00-07:00"))

    def test_compute_delta_handles_counter_reset(self):
        current = [k.NodeTotal(uuid="u1", name="node-a", up=10, down=20)]
        baseline = {"u1": {"name": "node-a", "up": 100, "down": 200}}

        deltas, new_baseline, warnings = k.compute_delta_from_nodes(current, baseline)

        self.assertEqual(deltas["u1"]["up"], 10)
        self.assertEqual(deltas["u1"]["down"], 20)
        self.assertEqual(new_baseline["u1"]["up"], 10)
        self.assertEqual(warnings, ["node-a"])

    def test_top_lines_orders_by_total(self):
        lines = k.top_lines(
            {
                "a": {"name": "small", "up": 1, "down": 1},
                "b": {"name": "big", "up": 10, "down": 20},
                "c": {"name": "middle", "up": 8, "down": 1},
            },
            2,
        )

        self.assertIn("big", lines[0])
        self.assertIn("middle", lines[1])
        self.assertEqual(len(lines), 2)

    def test_node_missing_alert_triggers_and_recovers(self):
        k.save_samples({
            "samples": [
                {"ts": 1000, "nodes": {}, "skipped": ["node-a(timeout)"]},
            ]
        })
        state = k.load_alerts_state()
        first = k.collect_alert_candidates(state, now_ts=1000)
        self.assertEqual(first, [])

        k.save_samples({
            "samples": [
                {"ts": 1000, "nodes": {}, "skipped": ["node-a(timeout)"]},
                {"ts": 1300, "nodes": {}, "skipped": ["node-a(timeout)"]},
            ]
        })
        second = k.collect_alert_candidates(state, now_ts=1300)
        self.assertEqual([c["key"] for c in second], ["node_missing:node-a"])

        events = k.apply_alert_candidates(state, second, now_ts=1300)
        self.assertEqual(events[0]["kind"], "alert")
        self.assertIn("node_missing:node-a", state["active"])

        k.save_samples({
            "samples": [
                {"ts": 1600, "nodes": {"u1": {"name": "node-a", "up": 1, "down": 1}}, "skipped": []},
            ]
        })
        recovered = k.collect_alert_candidates(state, now_ts=1600)
        events = k.apply_alert_candidates(state, recovered, now_ts=1600)
        self.assertEqual(events[0]["kind"], "recovery")
        self.assertEqual(state["active"], {})

    def test_node_missing_does_not_double_count_same_sample(self):
        k.save_samples({
            "samples": [
                {"ts": 1000, "nodes": {}, "skipped": ["node-a(timeout)"]},
            ]
        })
        state = k.load_alerts_state()

        self.assertEqual(k.collect_alert_candidates(state, now_ts=1000), [])
        self.assertEqual(k.collect_alert_candidates(state, now_ts=1010), [])
        self.assertEqual(state["node_skips"]["node-a"]["count"], 1)

    def test_window_total_alert_and_dry_run_does_not_persist(self):
        self.patch_attr("ALERT_TOTAL_WINDOW_BYTES", 100)
        self.patch_attr("ALERT_WINDOW_MINUTES", 60)
        self.patch_attr("telegram_send_alert", lambda message: self.fail("dry-run sent Telegram"))
        time_patcher = patch.object(k.time, "time", return_value=4600)
        time_patcher.start()
        self.patchers.append(time_patcher)

        k.save_samples({
            "samples": [
                {"ts": 1000, "nodes": {"u1": {"name": "node-a", "up": 0, "down": 0}}, "skipped": []},
                {"ts": 4600, "nodes": {"u1": {"name": "node-a", "up": 50, "down": 100}}, "skipped": []},
            ]
        })

        result = k.run_alert_check(dry_run=True, notify=True, force_sample=False)

        self.assertEqual(result["events"][0]["key"], "window_total")
        self.assertFalse((self.tmp_path / "alerts_state.json").exists())

    def test_alert_message_escapes_title(self):
        message = k.format_alert_message(
            {
                "title": "node <bad>",
                "body": "body",
            },
            now_ts=1000,
        )

        self.assertIn("node &lt;bad&gt;", message)

    def test_normalize_percent_metric_rejects_byte_like_values(self):
        self.assertEqual(k.normalize_percent_metric(50), 50)
        self.assertEqual(k.normalize_percent_metric({"used": 1, "total": 2}), 50)
        self.assertEqual(k.normalize_percent_metric(512, total=1024), 50)
        self.assertIsNone(k.normalize_percent_metric(123456789))
        self.assertIsNone(k.normalize_percent_metric({"used": 2, "total": 1}))

    def test_history_sum_migrates_json_to_sqlite_daily_usage(self):
        k.save_json_atomic(str(self.tmp_path / "history.json"), {
            "days": {
                "2026-06-01": {
                    "n1": {"name": "Node One", "up": 10, "down": 20},
                    "n2": {"name": "Node Two", "up": 5, "down": 7},
                },
                "2026-06-02": {
                    "n1": {"name": "Node One", "up": 3, "down": 4},
                },
            }
        })

        summed = k.history_sum(date(2026, 6, 1), date(2026, 6, 2))

        self.assertEqual(summed["n1"], {"name": "Node One", "up": 13, "down": 24})
        self.assertEqual(summed["n2"], {"name": "Node Two", "up": 5, "down": 7})
        self.assertTrue((self.tmp_path / "traffic.db").exists())

    def test_report_schedule_validation_and_due_key(self):
        schedule = k.validate_report_schedule({
            "enabled": True,
            "scope": "weekly",
            "mode": "top",
            "time": "09:30",
            "weekday": 0,
            "month_day": 1,
        })
        now = datetime(2026, 6, 8, 9, 30, tzinfo=k.TZ)

        self.assertEqual(schedule["scope"], "weekly")
        self.assertEqual(k.schedule_due_key(schedule, now), f"{schedule['id']}:2026-06-08 09:30")
        self.assertIsNone(k.schedule_due_key(schedule, datetime(2026, 6, 8, 9, 31, tzinfo=k.TZ)))
        with self.assertRaises(RuntimeError):
            k.validate_report_schedule({"scope": "daily", "mode": "full", "time": "25:00"})
        with self.assertRaises(RuntimeError):
            k.validate_report_schedule({"scope": "weekly", "mode": "full", "time": "09:00", "weekday": 7})
        with self.assertRaises(RuntimeError):
            k.validate_report_schedule({"scope": "monthly", "mode": "full", "time": "09:00", "month_day": 0})


if __name__ == "__main__":
    unittest.main()
