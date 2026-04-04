#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
兼容脚本：复用主脚本中的 rebuild-baselines 逻辑，避免重复维护两份实现。
"""

import subprocess
import sys


def main() -> int:
    cmd = [sys.executable, "komari_traffic_report.py", "rebuild-baselines", *sys.argv[1:]]
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
