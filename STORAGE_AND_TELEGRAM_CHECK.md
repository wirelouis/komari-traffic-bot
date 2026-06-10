# 数据流与 Telegram 联动检查报告

## 当前数据流架构（已修复）

### 1. 采样层（数据源头）✅
**流程：**
```
每 5 分钟（SAMPLE_INTERVAL_SECONDS=300）
  ↓
take_sample_if_due()
  ↓
├─ save_traffic_snapshot(ts, nodes_map, skipped)
│   → 写入 traffic_snapshots 表（原始累计计数器）
│
├─ materialize_latest_traffic_segment(ts)
│   ├─ 读取最近 2 个 snapshots
│   ├─ 计算增量（处理计数器重置）
│   ├─ 写入 traffic_segments 表
│   └─ materialize_daily_usage_from_segments(days)
│       → 实时更新 node_daily_usage 表（每日汇总）
│
└─ prune_traffic_snapshots(now_ts)
    → 删除 45 天前的 snapshots
```

**关键点：**
- ✅ **数据持久化不依赖报表**：每次采样自动物化 segments 和 daily rollup
- ✅ **计数器重置处理**：负增量按 0 处理，记录 reset_warnings
- ✅ **跨天自动处理**：`_segment_days()` 识别跨午夜的 segment，物化到两天
- ✅ **retention 策略**：snapshots 45 天，segments 永久，daily rollup 永久

### 2. 查询层（统一入口）✅
**流程：**
```
query_usage(from_ts, to_ts, group_by="node")
  ↓
├─ ensure_traffic_segments_backfilled()
│   → 确保 snapshots → segments 完整
│
├─ 判断覆盖率：
│   ├─ segments 覆盖 >= 90% → 直接返回（精确）
│   └─ 覆盖 < 90% 且日期有效 → 按天混合 rollup
│
└─ 返回：nodes map + metadata (from_ts, to_ts, source, group_by)
```

**关键点：**
- ✅ **优先使用 segments**：带 proration，精确到秒
- ✅ **自动回退 rollup**：长期查询（7d/30d）自动使用 daily rollup
- ✅ **24h/7d/30d 不再相同**：修复了主要问题

### 3. 报表层（只读）✅
**流程：**
```
build_scope_report_message(scope)  # scope: daily/weekly/monthly
  ↓
├─ scheduled_report_period_parts(scope)
│   → 计算"上一完整周期"：昨天/上周/上月
│
├─ take_sample_if_due(force=True, source=f"{scope}-report-boundary")
│   → 报表前强制采样（确保边界数据完整）
│
├─ build_period_report_message(start, end, tag)
│   ├─ build_live_period_struct(from_dt, to_dt)
│   │   └─ build_daily_period_usage(from_dt, to_dt)
│   │       → 调用 query_usage，混合 segments + rollup
│   │
│   └─ format_report(title, period_label, deltas, ...)
│       → 格式化为 Telegram HTML 消息
│
└─ telegram_send(message)
    → 发送到 TELEGRAM_CHAT_ID
```

**关键点：**
- ✅ **报表只读**：不再写入 history.json 或 daily rollup
- ✅ **边界采样**：报表前强制采样，避免"报表时刻刚好漏采样"
- ✅ **统一标题**：SCOPE_REPORT_TITLES = {daily: 昨日, weekly: 上周, monthly: 上月}

### 4. Telegram 联动检查 ✅

**推送路径：**
```
1. 内置报表（已弃用 cron）：
   run_daily_send_yesterday/weekly/monthly()
     → build_scope_report_message(scope)
     → telegram_send(message)

2. 自定义计划（Web 面板配置）：
   scheduler_worker_loop() 每 30 秒检查
     → schedule_due_key() 判断是否到期
     → run_report_schedule(item)
         → build_scope_report_message(scope, top_only)
         → telegram_send_to_chat(message, chat)

3. 手动命令：
   /today, /week, /month
     → 调用 build_scope_report_message()

4. Top 榜：
   /top [Nh|today|week|month]
     → get_top_last_hours_struct(hours, n)
         → snapshot_range_usage(from_ts, now_ts)
             → query_usage(from_ts, now_ts)
```

**关键点：**
- ✅ **单一数据源**：所有报表都通过 `query_usage` 读取
- ✅ **自动推送**：scheduler_worker 30秒检查，last_runs 去重
- ✅ **边界采样**：报表前 `force=True` 补采样
- ✅ **Telegram 失败重试**：`telegram_send` 内置重试机制

## 潜在问题与检查项

### ⚠️ 需要确认的点

1. **bot 服务是否持续运行**
   ```bash
   docker compose ps bot
   # 应该显示 Up 状态
   ```

2. **采样是否正常**
   ```bash
   docker compose logs --tail=50 bot | grep "采样"
   # 应该每 5 分钟看到一次采样日志
   ```

3. **daily rollup 是否实时更新**
   ```bash
   python -c "
   import sqlite3
   conn = sqlite3.connect('./data/traffic.db')
   row = conn.execute('''
       SELECT datetime(MAX(updated_at), 'unixepoch', 'localtime') as last_update
       FROM node_daily_usage
       WHERE day = DATE('now', 'localtime')
   ''').fetchone()
   print(f'今天 daily rollup 最后更新: {row[0]}')
   conn.close()
   "
   # 应该是最近 5 分钟内
   ```

4. **自动推送计划是否生效**
   ```bash
   # Web 面板 → 推送控制 → 查看最近运行记录
   # 或查询 task_runs 表
   python -c "
   import sqlite3
   conn = sqlite3.connect('./data/traffic.db')
   rows = conn.execute('''
       SELECT type, source, status, summary, 
              datetime(started_at, 'unixepoch', 'localtime') as time
       FROM task_runs
       WHERE type IN ('report', 'sample')
       ORDER BY started_at DESC
       LIMIT 10
   ''').fetchall()
   for row in rows:
       print(f'{row[4]}: {row[0]} {row[1]} {row[2]} - {row[3]}')
   conn.close()
   "
   ```

5. **报表数据是否正确**
   ```bash
   # 测试今天的报表（应该有数据）
   docker compose exec bot python -c "
   import komari_traffic_report as k
   message = k.build_scope_report_message('daily')
   print(message[:500])  # 打印前 500 字符
   "
   ```

## 结论

✅ **存储模式正确**：
- 采样层持续物化数据（不依赖报表）
- query_usage 统一查询（自动混合 segments + rollup）
- 报表只读（不再写入数据）

✅ **Telegram 联动正确**：
- 所有推送路径都通过 query_usage 读取
- 报表前强制边界采样
- scheduler 自动检查推送

⚠️ **需要用户验证**：
- bot 服务持续运行
- 采样日志正常
- daily rollup 实时更新
- 推送计划正常触发

按上面的检查项在服务器上验证一遍，有问题的话把输出发给我。
