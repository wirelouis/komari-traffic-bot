#!/usr/bin/env python3
"""
修复缺失天数的流量数据

用法：
    python fix_missing_days.py check 2026-06-07 2026-06-09
    python fix_missing_days.py fix 2026-06-07 2026-06-09
"""
import sys
from datetime import date, datetime, timedelta
import komari_traffic_report as k

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def check_missing_days(from_day: date, to_day: date):
    print(f"检查 {from_day} 到 {to_day} 的数据状态：\n")

    # 检查 snapshots 覆盖
    import sqlite3
    conn = sqlite3.connect(k.TRAFFIC_DB_PATH)
    conn.row_factory = sqlite3.Row

    snapshot_days = conn.execute("""
        SELECT DISTINCT DATE(ts, 'unixepoch', 'localtime') AS day
        FROM traffic_snapshots
        WHERE DATE(ts, 'unixepoch', 'localtime') BETWEEN ? AND ?
        ORDER BY day
    """, (from_day.strftime("%Y-%m-%d"), to_day.strftime("%Y-%m-%d"))).fetchall()

    snapshot_day_set = {row["day"] for row in snapshot_days}

    # 检查 daily rollup 覆盖
    rollup_days = conn.execute("""
        SELECT DISTINCT day
        FROM node_daily_usage
        WHERE day BETWEEN ? AND ?
        ORDER BY day
    """, (from_day.strftime("%Y-%m-%d"), to_day.strftime("%Y-%m-%d"))).fetchall()

    rollup_day_set = {row["day"] for row in rollup_days}
    conn.close()

    # 检查 history.json
    history = k.load_json(k.HISTORY_PATH, {"days": {}}).get("days", {})
    history_day_set = set(history.keys())

    # 逐日报告
    d = from_day
    fixable_days = []
    missing_days = []

    while d <= to_day:
        day_str = d.strftime("%Y-%m-%d")
        has_snapshot = day_str in snapshot_day_set
        has_rollup = day_str in rollup_day_set
        has_history = day_str in history_day_set

        status = []
        if has_rollup:
            status.append("✓ rollup")
        else:
            status.append("✗ rollup")

        if has_snapshot:
            status.append("✓ snapshots")
            if not has_rollup:
                fixable_days.append(d)
        else:
            status.append("✗ snapshots")

        if has_history:
            status.append("✓ history.json")
            if not has_rollup and not has_snapshot:
                fixable_days.append(d)

        print(f"  {day_str}: {', '.join(status)}")

        if not has_rollup and not has_snapshot and not has_history:
            missing_days.append(d)

        d += timedelta(days=1)

    print()
    if fixable_days:
        print(f"✓ 可修复天数：{len(fixable_days)} 天")
        print(f"  运行: python fix_missing_days.py fix {from_day} {to_day}")
    elif missing_days:
        print(f"✗ 无法修复：{len(missing_days)} 天没有任何数据源")
    else:
        print("✓ 所有天数数据完整")

def fix_missing_days(from_day: date, to_day: date):
    print(f"修复 {from_day} 到 {to_day} 的数据：\n")

    # 1. 迁移 history.json
    print("1. 迁移 history.json 到 node_daily_usage...")
    if hasattr(k, 'migrate_history_to_traffic_db'):
        k.migrate_history_to_traffic_db()
        print("   完成")
    else:
        print("   跳过（旧版本不支持）")

    # 2. 从 snapshots 重建 segments 和 daily rollup
    print("2. 从 traffic_snapshots 重建 segments 和 daily rollup...")
    from_ts = int(k.start_of_day(from_day).timestamp())
    to_ts = int(k.start_of_day(to_day + timedelta(days=1)).timestamp())

    if hasattr(k, 'rebuild_traffic_segments_from_snapshots'):
        result = k.rebuild_traffic_segments_from_snapshots(from_ts, to_ts)
        print(f"   生成 {result['segments']} 个 segments，{result['daily_rows']} 条 daily rollup")
    else:
        print("   旧版本：手动重建...")
        # 旧版本兼容：直接调用 ensure_traffic_segments_backfilled
        k.ensure_traffic_segments_backfilled()
        print("   完成 backfill")

    # 3. 验证修复结果
    print("\n3. 验证修复结果：")
    import sqlite3
    conn = sqlite3.connect(k.TRAFFIC_DB_PATH)
    conn.row_factory = sqlite3.Row
    rollup_days = conn.execute("""
        SELECT day, COUNT(*) AS node_count, SUM(total) AS total_bytes
        FROM node_daily_usage
        WHERE day BETWEEN ? AND ?
        GROUP BY day
        ORDER BY day
    """, (from_day.strftime("%Y-%m-%d"), to_day.strftime("%Y-%m-%d"))).fetchall()
    conn.close()

    for row in rollup_days:
        print(f"   {row['day']}: {row['node_count']} 个节点, 总流量 {k.human_bytes(row['total_bytes'])}")

    print("\n✓ 修复完成")

def main():
    if len(sys.argv) < 4:
        print("用法:")
        print("  python fix_missing_days.py check 2026-06-07 2026-06-09")
        print("  python fix_missing_days.py fix 2026-06-07 2026-06-09")
        sys.exit(1)

    cmd = sys.argv[1]
    from_day = parse_date(sys.argv[2])
    to_day = parse_date(sys.argv[3])

    if cmd == "check":
        check_missing_days(from_day, to_day)
    elif cmd == "fix":
        fix_missing_days(from_day, to_day)
    else:
        print(f"未知命令: {cmd}")
        sys.exit(1)

if __name__ == "__main__":
    main()
