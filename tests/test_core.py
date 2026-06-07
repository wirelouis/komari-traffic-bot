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
        self.patch_attr("BOT_INSTANCE_NAME", "")
        self.patch_attr("KOMARI_BASE_URL", "")
        self.patch_attr("TELEGRAM_CHAT_ID", "")
        self.patch_attr("TELEGRAM_ALERT_CHAT_ID", "")
        self.patch_attr("AI_API_BASE", "")
        self.patch_attr("AI_MODEL", "")
        self.patch_attr("TOP_N", 3)
        self.patch_attr("TIMEOUT", 10)
        self.patch_attr("KOMARI_FETCH_WORKERS", 4)
        self.patch_attr("SAMPLE_INTERVAL_SECONDS", 300)
        self.patch_attr("SAMPLE_RETENTION_HOURS", 48)
        self.patch_attr("AI_PACK_CACHE_TTL_SECONDS", 3600)
        self.patch_attr("TASK_RUN_RETENTION_DAYS", 90)

    def test_parse_bytes_value_supports_units(self):
        self.assertEqual(k.parse_bytes_value(""), 0)
        self.assertEqual(k.parse_bytes_value("1024"), 1024)
        self.assertEqual(k.parse_bytes_value("500MiB"), 500 * 1024 ** 2)
        self.assertEqual(k.parse_bytes_value("2 GiB"), 2 * 1024 ** 3)
        self.assertEqual(k.parse_bytes_value("1.5TiB"), int(1.5 * 1024 ** 4))

    def test_parse_bytes_value_rejects_bad_unit(self):
        with self.assertRaises(RuntimeError):
            k.parse_bytes_value("10XB", "TEST_BYTES")

    def test_save_json_atomic_uses_unique_temp_file(self):
        target = self.tmp_path / "state.json"
        fixed_tmp = self.tmp_path / "state.json.tmp"
        fixed_tmp.write_text("sentinel", encoding="utf-8")

        k.save_json_atomic(str(target), {"ok": True})

        self.assertEqual(k.load_json_strict(str(target)), {"ok": True})
        self.assertEqual(fixed_tmp.read_text(encoding="utf-8"), "sentinel")
        leftovers = [
            path.name
            for path in self.tmp_path.glob("state.json.*.tmp")
            if path.name != "state.json.tmp"
        ]
        self.assertEqual(leftovers, [])

    def test_save_archive_month_uses_unique_temp_file(self):
        fixed_tmp = self.tmp_path / "history-2026-06.json.gz.tmp"
        fixed_tmp.write_text("sentinel", encoding="utf-8")

        k.save_archive_month("2026-06", {"days": {"2026-06-01": {"n1": {"down": 2}}}})

        self.assertEqual(k.load_archive_month("2026-06")["days"]["2026-06-01"]["n1"]["down"], 2)
        self.assertEqual(fixed_tmp.read_text(encoding="utf-8"), "sentinel")
        leftovers = [
            path.name
            for path in self.tmp_path.glob("history-2026-06.json.gz.*.tmp")
            if path.name != "history-2026-06.json.gz.tmp"
        ]
        self.assertEqual(leftovers, [])

    def test_save_offset_uses_atomic_unique_temp_file(self):
        fixed_tmp = self.tmp_path / "tg_offset.txt.tmp"
        fixed_tmp.write_text("sentinel", encoding="utf-8")

        k.save_offset(12345)

        self.assertEqual(k.load_offset(), 12345)
        self.assertEqual(fixed_tmp.read_text(encoding="utf-8"), "sentinel")
        leftovers = [
            path.name
            for path in self.tmp_path.glob("tg_offset.txt.*.tmp")
            if path.name != "tg_offset.txt.tmp"
        ]
        self.assertEqual(leftovers, [])

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

    def test_telegram_reports_escape_dynamic_html(self):
        deltas = {
            "u1": {"name": "node <bad> & edge", "up": 1, "down": 2},
        }

        full = k.format_report(
            "Title <T>",
            "range <1>",
            deltas,
            reset_warnings=["reset <node>"],
            skipped=["skip <node>(timeout)"],
            include_top=True,
        )
        top_only = k.format_top_only_message(
            "range <1>",
            deltas,
            reset_warnings=["reset <node>"],
            skipped=["skip <node>(timeout)"],
        )

        for message in (full, top_only):
            self.assertIn("<b>node &lt;bad&gt; &amp; edge</b>", message)
            self.assertIn("range &lt;1&gt;", message)
            self.assertIn("skip &lt;node&gt;(timeout)", message)
            self.assertIn("reset &lt;node&gt;", message)
            self.assertNotIn("<bad>", message)
            self.assertNotIn("<node>", message)
        self.assertIn("Title &lt;T&gt;", full)

    def test_alert_exception_redacts_and_escapes_failure_details(self):
        sent = []
        self.patch_attr("TELEGRAM_BOT_TOKEN", "secret-telegram-token")
        self.patch_attr("TELEGRAM_CHAT_ID", "123456789")
        self.patch_attr("KOMARI_API_TOKEN", "secret-komari-token")
        self.patch_attr("AI_API_KEY", "secret-ai-key")
        self.patch_attr("telegram_send", lambda text: sent.append(text))

        try:
            raise RuntimeError("bad <tag> secret-telegram-token secret-komari-token secret-ai-key")
        except RuntimeError as exc:
            k.alert_exception("where <x>", "cmd <run> secret-ai-key", exc)

        self.assertEqual(len(sent), 1)
        message = sent[0]
        self.assertIn("where &lt;x&gt;", message)
        self.assertIn("cmd &lt;run&gt;", message)
        self.assertIn("bad &lt;tag&gt;", message)
        self.assertNotIn("<tag>", message)
        self.assertNotIn("<x>", message)
        self.assertNotIn("<run>", message)
        self.assertNotIn("secret-telegram-token", message)
        self.assertNotIn("secret-komari-token", message)
        self.assertNotIn("secret-ai-key", message)

    def test_ai_chat_redacts_error_details(self):
        self.patch_attr("AI_API_BASE", "https://ai.example/v1")
        self.patch_attr("AI_API_KEY", "secret-ai-key")
        self.patch_attr("AI_MODEL", "gpt-test")

        with patch.object(k.requests, "post", side_effect=RuntimeError("boom secret-ai-key <tag>")):
            text = k.ai_chat([{"role": "user", "content": "hello"}])

        self.assertIn("RuntimeError", text)
        self.assertNotIn("secret-ai-key", text)
        normalized = k.normalize_ai_answer_for_telegram(text)
        self.assertIn("&lt;tag&gt;", normalized)
        self.assertNotIn("<tag>", normalized)

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

    def test_task_runs_record_and_filter(self):
        k.record_task_run(
            "report",
            "web:composer",
            "success",
            started_at=1000,
            finished_at=1002.5,
            summary="sent",
            metadata={"scope": "today"},
        )
        k.record_task_run("alert", "web:alerts-check", "failed", started_at=1003, finished_at=1004, error="boom")

        report_runs = k.list_task_runs(limit=10, task_type="report")
        all_runs = k.list_task_runs(limit=10)

        self.assertEqual(len(report_runs), 1)
        self.assertEqual(report_runs[0]["summary"], "sent")
        self.assertEqual(report_runs[0]["metadata"]["scope"], "today")
        self.assertEqual(report_runs[0]["duration_ms"], 2500)
        self.assertEqual(len(all_runs), 2)
        self.assertEqual(all_runs[0]["status"], "failed")

    def test_task_run_prune_and_vacuum_keep_traffic_rollups(self):
        k.upsert_daily_usage("2026-06-01", {
            "n1": {"name": "Node One", "up": 10, "down": 20},
        }, source="test")
        k.record_task_run("report", "old", "success", started_at=1000, finished_at=1001, summary="old")
        k.record_task_run("report", "recent", "success", started_at=150000, finished_at=150001, summary="recent")

        status = k.traffic_db_maintenance_status(retention_days=1, now_ts=200000)
        result = k.prune_task_runs(retention_days=1, now_ts=200000)
        vacuum = k.vacuum_traffic_db()

        self.assertEqual(status["old_task_runs"], 1)
        self.assertEqual(result["deleted"], 1)
        self.assertEqual(result["remaining"], 1)
        self.assertEqual(k.list_task_runs(limit=10)[0]["summary"], "recent")
        self.assertEqual(k.aggregate_daily_usage(date(2026, 6, 1), date(2026, 6, 1))["n1"]["down"], 20)
        self.assertEqual(vacuum["table_counts"]["node_daily_usage"], 1)
        self.assertIn("after_size_human", vacuum)

    def test_runtime_config_save_load_and_validate(self):
        saved = k.save_runtime_config({
            "bot_instance_name": "edge-prod",
            "komari_base_url": "https://komari-new.example/",
            "telegram_chat_id": "987654321",
            "telegram_alert_chat_id": "123123123",
            "ai_api_base": "https://ai-new.example/v1/",
            "ai_model": "gpt-5.4-mini",
            "top_n": 6,
            "komari_timeout_seconds": 20,
            "komari_fetch_workers": 8,
            "sample_interval_seconds": 120,
            "sample_retention_hours": 24,
            "ai_pack_cache_ttl_seconds": 1800,
            "task_run_retention_days": 45,
            "alerts_enabled": False,
            "alert_recovery_notify": False,
            "alert_cooldown_seconds": 600,
            "alert_window_minutes": 30,
            "alert_node_missing_samples": 3,
            "alert_silence_windows": "23:00-07:00",
            "alert_total_window_bytes": "2GiB",
        })

        self.assertEqual(saved["bot_instance_name"], "edge-prod")
        self.assertEqual(saved["komari_base_url"], "https://komari-new.example")
        self.assertEqual(saved["alert_total_window_bytes"], 2 * 1024 ** 3)
        self.assertEqual(k.KOMARI_BASE_URL, "https://komari-new.example")
        self.assertEqual(k.TELEGRAM_CHAT_ID, "987654321")
        self.assertEqual(k.AI_API_BASE, "https://ai-new.example/v1")
        self.assertEqual(k.AI_MODEL, "gpt-5.4-mini")
        self.assertEqual(k.TOP_N, 6)
        self.assertEqual(k.TIMEOUT, 20)
        self.assertFalse(k.ALERTS_ENABLED)
        self.assertEqual(k.ALERT_SILENCE_WINDOWS, "23:00-07:00")
        self.assertEqual(k.ALERT_TOTAL_WINDOW_BYTES, 2 * 1024 ** 3)
        self.assertEqual(k.load_runtime_config()["task_run_retention_days"], 45)
        current = k.current_runtime_config()
        self.assertEqual(current["values"]["ai_pack_cache_ttl_seconds"], 1800)
        self.assertEqual(current["values"]["alert_total_window_bytes"], 2 * 1024 ** 3)
        self.assertTrue(str(self.tmp_path) in current["path"])
        fields = {item["key"]: item for item in current["editable"]}
        self.assertEqual(fields["alerts_enabled"]["type"], "boolean")
        self.assertEqual(fields["alert_total_window_bytes"]["type"], "bytes")
        self.assertEqual(fields["alert_total_window_bytes"]["value"], "2.00 GiB")

        with self.assertRaises(RuntimeError):
            k.validate_runtime_config({"top_n": 0})
        with self.assertRaises(RuntimeError):
            k.validate_runtime_config({"alert_silence_windows": "bad"})
        with self.assertRaises(RuntimeError):
            k.validate_runtime_config({"alert_total_window_bytes": "10XB"})
        with self.assertRaises(RuntimeError):
            k.validate_runtime_config({"telegram_chat_id": "bad id"})

    def test_traffic_range_summary_aggregates_daily_and_weekly(self):
        k.upsert_daily_usage("2026-06-01", {
            "n1": {"name": "Node One", "up": 10, "down": 20},
            "n2": {"name": "Node Two", "up": 5, "down": 7},
        }, source="test")
        k.upsert_daily_usage("2026-06-02", {
            "n1": {"name": "Node One", "up": 3, "down": 4},
        }, source="test")

        daily = k.traffic_range_summary(date(2026, 6, 1), date(2026, 6, 2), group="daily")
        weekly = k.traffic_range_summary(date(2026, 6, 1), date(2026, 6, 2), group="weekly")

        self.assertEqual(daily["total"]["total"], 49)
        self.assertEqual(daily["nodes"][0]["uuid"], "n1")
        self.assertEqual(len(daily["groups"]), 2)
        self.assertEqual(weekly["groups"][0]["key"], "2026-06-01")
        self.assertEqual(weekly["groups"][0]["total"]["total"], 49)

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

    def test_schedule_next_run_at(self):
        daily = k.validate_report_schedule({"scope": "daily", "mode": "full", "time": "09:00"})
        weekly = k.validate_report_schedule({"scope": "weekly", "mode": "top", "time": "08:30", "weekday": 2})
        monthly = k.validate_report_schedule({"scope": "monthly", "mode": "full", "time": "10:15", "month_day": 6})

        daily_next = k.schedule_next_run_at(daily, datetime(2026, 6, 6, 9, 1, tzinfo=k.TZ))
        weekly_next = k.schedule_next_run_at(weekly, datetime(2026, 6, 6, 7, 0, tzinfo=k.TZ))
        monthly_next = k.schedule_next_run_at(monthly, datetime(2026, 6, 6, 10, 16, tzinfo=k.TZ))

        self.assertEqual(datetime.fromtimestamp(daily_next, k.TZ).strftime("%Y-%m-%d %H:%M"), "2026-06-07 09:00")
        self.assertEqual(datetime.fromtimestamp(weekly_next, k.TZ).strftime("%Y-%m-%d %H:%M"), "2026-06-10 08:30")
        self.assertEqual(datetime.fromtimestamp(monthly_next, k.TZ).strftime("%Y-%m-%d %H:%M"), "2026-07-06 10:15")


if __name__ == "__main__":
    unittest.main()
