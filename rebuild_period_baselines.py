#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重建 WEEK-YYYY-MM-DD / MONTH-YYYY-MM-DD 基线：
- 仅使用 baselines.json 中已有的日基线 (YYYY-MM-DD)
- 对 2026-02-01 及之后的日期：
  - 每逢周一，写入/覆盖 WEEK-YYYY-MM-DD
  - 每逢每月 1 号，写入/覆盖 MONTH-YYYY-MM-DD

注意：
- 会原地修改 data/baselines.json
- 已在同目录提前做好备份：baselines.json.bak-20260312-1438
"""

import json
import os
from datetime import datetime, date, timedelta

BASELINES_PATH = os.path.join("data", "baselines.json")


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def is_date_key(k: str) -> bool:
    try:
        datetime.strptime(k, "%Y-%m-%d")
        return True
    except Exception:
        return False


def main():
    base = load_json(BASELINES_PATH)
    baselines = base.get("baselines", {})

    # 收集所有日基线键
    date_keys = [k for k in baselines.keys() if is_date_key(k)]
    if not date_keys:
        raise SystemExit("没有找到任何 YYYY-MM-DD 形式的日基线，放弃重建。")

    # 转成日期对象方便遍历
    date_objs = sorted(date.fromisoformat(k) for k in date_keys)

    # 只从 2026-02-01 开始处理，防止更早历史被误改
    start_cut = date(2026, 2, 1)
    date_objs = [d for d in date_objs if d >= start_cut]
    if not date_objs:
        raise SystemExit("2026-02-01 之后没有可用的日基线。")

    for d in date_objs:
        k = d.isoformat()  # YYYY-MM-DD
        nodes = baselines.get(k, {}).get("nodes")
        if not isinstance(nodes, dict):
            # 没有 nodes 字段就跳过
            continue

        # 周一 → WEEK-YYYY-MM-DD
        if d.weekday() == 0:  # Monday
            week_key = f"WEEK-{k}"
            baselines[week_key] = {
                "nodes": nodes,
                "ts": f"{d.isoformat()} 00:00:00 (rebuilt-week)",
            }

        # 月初 1 号 → MONTH-YYYY-MM-DD
        if d.day == 1:
            month_key = f"MONTH-{k}"
            baselines[month_key] = {
                "nodes": nodes,
                "ts": f"{d.isoformat()} 00:00:00 (rebuilt-month)",
            }

    base["baselines"] = baselines
    save_json(BASELINES_PATH, base)
    print("OK: rebuilt WEEK-/MONTH- baselines from daily baselines >= 2026-02-01")


if __name__ == "__main__":
    main()
