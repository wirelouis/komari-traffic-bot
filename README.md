<p align="center">
  <a href="./README.md">简体中文</a> ｜ 
  <a href="./README_EN.md">English</a>
</p>

# komari-traffic-bot（Docker 版）

基于 **Komari 探针** 的流量统计增强工具，提供：

- 📊 Telegram **流量日报 / 周报 / 月报**
- 🔥 **Top N 流量消耗榜**（支持 `/top 6h`、`/top week` 等任意时间窗口）
- 🤖 Telegram Bot **交互式查询**
- 🧭 **Web 流量分析面板**
- 🐳 **Docker / docker-compose 部署**
- 🕒 统计口径固定为 **北京时间（Asia/Shanghai）**
- 🧱 适合多节点、长期运行场景

> 本项目不替代 Komari 官方功能，而是在其基础上补充  
> **长期统计 / 任意时间窗口 Top 榜 / Telegram 查询能力**。

---

## ✨ 功能特性

- **日报 / 周报 / 月报**
  - 每天 00:00 自动推送昨日日报
  - 每周一推送上周周报
  - 每月 1 号推送上月月报
- **Top 流量消耗榜**
  - `/top`：今日 Top N（上下行合计）
  - `/top 6h`：最近 6 小时 Top
  - `/top week`、`/top month`
- **交互命令**
  - `/today` `/week` `/month`
  - `/top [Nh|today|week|month]`
  - `/ask 你的问题（或 /ai）`
- **Web 面板**
  - 总览今日 / 本周 / 本月流量与节点 Top
  - 查看告警状态，手动检查、静默或恢复告警
  - 测试 Telegram 推送，手动发送报表，并使用 AI 问答
- **智能告警**
  - 节点连续采样失败告警与恢复通知
  - 最近窗口 / 今日总量 / 单节点流量阈值告警
  - 支持冷却时间、静默时段和单独告警 chat
- **稳定性**
  - Komari 节点超时自动跳过，不影响整体报表
  - Telegram 网络异常自动重试
  - 探针/节点重启自动兜底计数器
- **数据管理**
  - 历史数据自动压缩归档
  - 采样数据用于支持任意 Nh 查询

---

## 🧩 依赖说明

- 已部署并可访问的 **Komari 面板**
- Docker + docker-compose
- 一个 Telegram Bot Token
- Telegram Chat ID（个人 / 群组）

---

## 🚀 快速部署（docker-compose）

### 1️⃣ 创建目录并赋予权限
容器默认以**非 root 用户（uid:gid = 10001:10001）**运行，需要对 `data/` 目录具有写权限。
```
bash
mkdir -p komari-traffic && cd komari-traffic
mkdir -p data
sudo chown -R 10001:10001 data
sudo chmod -R u+rwX,go+rX data
```
> 如果启动后日志中出现 `PermissionError: [Errno 13] Permission denied: '/data/...'`，
> 请重新执行上述 `chown` / `chmod` 命令后重启容器。
### 2️⃣ 创建 .env 配置文件
```
cat > .env <<'ENV'
# Komari 面板地址（不要以 / 结尾）
KOMARI_BASE_URL=https://your-komari.example

# Komari API 超时（秒）
KOMARI_TIMEOUT_SECONDS=15

# Komari API 鉴权（可选）
KOMARI_API_TOKEN=
KOMARI_API_TOKEN_HEADER=Authorization
KOMARI_API_TOKEN_PREFIX=Bearer

# Komari 节点并发请求数
KOMARI_FETCH_WORKERS=6

# Telegram
TELEGRAM_BOT_TOKEN=123456:YOUR_BOT_TOKEN
TELEGRAM_CHAT_ID=123456789

# 允许接收命令的 chat（可选，逗号分隔）
TELEGRAM_ALLOWED_CHAT_IDS=

# 管理员 chat（可选，逗号分隔）
TELEGRAM_ADMIN_CHAT_IDS=

# AI（可选，启用 /ask 与 /ai）
AI_API_BASE=
AI_API_KEY=
AI_MODEL=

# AI 数据包缓存时长（秒），默认 3600；设为 0 关闭缓存
AI_PACK_CACHE_TTL_SECONDS=3600

# Web 面板（WEB_PASSWORD 必填）
WEB_USERNAME=admin
WEB_PASSWORD=
# 留空时每次启动生成临时会话密钥；公网部署建议固定填写随机长字符串
WEB_SESSION_SECRET=
WEB_PORT=8080

# 启动通知（可选）
# 设为 0 可关闭启动消息
BOT_START_NOTIFY=1

# 启动通知显示的实例名（可选，建议填机器名/环境名）
BOT_INSTANCE_NAME=

# 容器内数据目录（固定）
DATA_DIR=/data

# 统计时区（默认 Asia/Shanghai）
STAT_TZ=Asia/Shanghai

# Top 榜数量
TOP_N=3

# /top Nh 采样：每 5 分钟采样一次，默认仅保留 2 小时（用于短时差分）
# 长期历史建议依赖 Komari /api/records/load
SAMPLE_INTERVAL_SECONDS=300
SAMPLE_RETENTION_HOURS=2

# 历史数据策略
HISTORY_HOT_DAYS=60
HISTORY_RETENTION_DAYS=400

# 智能告警
ALERTS_ENABLED=1
# 告警发送 chat（可选；留空时复用 TELEGRAM_CHAT_ID）
TELEGRAM_ALERT_CHAT_ID=
# 同一 active 告警的重复提醒冷却时间（秒）
ALERT_COOLDOWN_SECONDS=1800
# 每日静默窗口（可选），格式：23:00-07:00 或 12:00-13:00,23:00-07:00
ALERT_SILENCE_WINDOWS=
# 节点连续采样失败 N 次后告警
ALERT_NODE_MISSING_SAMPLES=2
# 窗口流量阈值统计范围
ALERT_WINDOW_MINUTES=60
# 流量阈值：支持纯字节或 MiB/GiB/TiB；留空或 0 表示关闭对应规则
ALERT_TOTAL_WINDOW_BYTES=
ALERT_NODE_WINDOW_BYTES=
ALERT_DAILY_TOTAL_BYTES=
ALERT_DAILY_NODE_BYTES=
# 告警恢复后是否通知
ALERT_RECOVERY_NOTIFY=1

# 日志
LOG_LEVEL=INFO
LOG_FILE=
ENV
```
### 3️⃣ 准备 crontab
```
cat > crontab <<'CRON'
# 每天 00:00：昨日日报
0 0 * * * python /app/komari_traffic_report.py report_daily

# 每周一 00:05：上周周报
5 0 * * 1 python /app/komari_traffic_report.py report_weekly

# 每月 1 号 00:10：上月月报
10 0 1 * * python /app/komari_traffic_report.py report_monthly
CRON
```
### 4️⃣ 使用 docker-compose 启动
```
version: "3.9"

services:
  bot:
    image: ghcr.io/wirelouis/komari-traffic-bot:latest
    env_file: .env
    environment:
      - TZ=Asia/Shanghai
      - STAT_TZ=Asia/Shanghai
    volumes:
      - ./data:/data
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python", "/app/komari_traffic_report.py", "health"]
      interval: 30s
      timeout: 10s
      retries: 3
    command: ["python", "/app/komari_traffic_report.py", "listen"]

  cron:
    image: ghcr.io/wirelouis/komari-traffic-bot:latest
    env_file: .env
    environment:
      - TZ=Asia/Shanghai
      - STAT_TZ=Asia/Shanghai
    volumes:
      - ./data:/data
      - ./crontab:/app/crontab:ro
    restart: unless-stopped
    command: ["supercronic", "/app/crontab"]

  web:
    image: ghcr.io/wirelouis/komari-traffic-bot:latest
    env_file: .env
    environment:
      - TZ=Asia/Shanghai
      - STAT_TZ=Asia/Shanghai
    volumes:
      - ./data:/data
    ports:
      - "${WEB_PORT:-8080}:8080"
    restart: unless-stopped
    command: ["uvicorn", "web_app:app", "--host", "0.0.0.0", "--port", "8080"]
```
启动：
```
docker compose up -d
docker compose ps
```
Web 面板默认访问：`http://localhost:8080`

### 5️⃣ 初始化（只需一次）
```
docker compose exec bot \
  python /app/komari_traffic_report.py bootstrap
```
## 🧭 Web 面板

Web 面板提供轻量控制台，用于查看总览、节点流量、告警状态、Telegram 推送和 AI 问答。

- 登录账号：`WEB_USERNAME`（默认 `admin`）
- 登录密码：`WEB_PASSWORD`（必须设置）
- 端口：`WEB_PORT`（默认 `8080`）
- 会话密钥：`WEB_SESSION_SECRET`；留空时生成临时密钥，容器重启后登录态会失效

节点页会按 Komari `uuid` 自动绑定探针机器，也可以在面板里手动覆盖绑定。手动覆盖只保存在 `./data/node_bindings.json`，不会修改 Komari 本体配置；点击已绑定节点会打开 `KOMARI_BASE_URL/instance/{uuid}`。

面板不会向前端返回 Telegram Token、Komari Token、AI Key 或 Web 密码。

## 🤖 Telegram 命令示例
| 命令           | 说明               |
| ------------ | ---------------- |
| `/today`     | 今日流量（00:00 → 当前） |
| `/week`      | 本周流量             |
| `/month`     | 本月流量             |
| `/top`       | 今日 Top N         |
| `/top 6h`    | 最近 6 小时 Top      |
| `/top week`  | 本周 Top           |
| `/top month` | 本月 Top           |
| `/ask 问题`   | AI 基于数据包分析回答 |
| `/ai 问题`    | `/ask` 别名         |
| `/alerts`     | 查看告警状态（管理员） |
| `/mute_alerts 2h` | 静默告警 2 小时（管理员） |
| `/unmute_alerts` | 解除告警静默（管理员） |
| `/help`       | 查看命令帮助         |

## 🕒 关于时区
统计口径时区：STAT_TZ（默认 Asia/Shanghai）

定时触发时区：容器 TZ（默认 Asia/Shanghai）

因此：

“每天 0 点” = 北京时间 0 点

与宿主机系统时区无关

## 📦 数据说明
所有数据均保存在 ./data 目录中：

baseline（起点快照）

samples（用于 /top Nh）

history（日报历史 & 压缩归档）

Telegram offset

alerts_state（告警 active 状态 / 冷却 / 静默）

node_bindings（Web 面板节点到 Komari 机器的手动绑定覆盖）

升级 / 重启容器不会丢数据。

## 🚨 智能告警

告警默认启用节点连续采样失败检测；流量阈值类规则只有在你配置对应阈值后才会触发，避免升级后突然刷屏。

- `ALERT_TOTAL_WINDOW_BYTES`：最近 `ALERT_WINDOW_MINUTES` 分钟所有节点合计超阈值
- `ALERT_NODE_WINDOW_BYTES`：最近窗口内单节点超阈值
- `ALERT_DAILY_TOTAL_BYTES`：今日所有节点合计超阈值
- `ALERT_DAILY_NODE_BYTES`：今日单节点超阈值

阈值支持 `500MiB`、`2GiB`、`1TiB` 或纯数字字节；留空或 `0` 表示关闭该规则。

可用命令：

```
/alerts
/mute_alerts 2h
/unmute_alerts
```

也可以在容器内手动检查：

```
python /app/komari_traffic_report.py check_alerts --dry-run
```

## 🔄 升级方式
```
docker pull ghcr.io/wirelouis/komari-traffic-bot:latest
docker compose up -d
```

本版本没有新增必填环境变量。如果你沿用较旧的 `docker-compose.yml`，请确认：

- `web` 服务存在，并且挂载 `./data:/data`
- `.env` 中已设置 `WEB_PASSWORD`
- `KOMARI_BASE_URL` 指向浏览器可访问的 Komari 地址，用于节点详情跳转
- 如需自定义定时任务，`cron` 服务继续挂载 `./crontab:/app/crontab:ro`

`./data/node_bindings.json` 会由 Web 面板自动创建，不需要手动准备。

## ⚠️ 常见问题
/top 6h 没数据？
需要采样积累时间（默认每 5 分钟一次）

Komari 某节点超时？
会被自动跳过，不影响整体报表

Telegram 偶发断连？
已内置自动重试


## 🔐 管理员命令（需管理员 chat）

- `/archive` → 先发确认码，再通过 `/confirm_archive <code>` 执行
- `/bootstrap` → 有历史数据风险时会拒绝；可通过 `/confirm_bootstrap <code>` 执行
- `/rebuild_baselines` → 先发确认码，再通过 `/confirm_rebuild_baselines <code>` 执行
- `/alerts` → 查看告警状态
- `/mute_alerts 2h` / `/unmute_alerts` → 临时静默或恢复告警

> 默认管理员为 `TELEGRAM_CHAT_ID`。配置 `TELEGRAM_ADMIN_CHAT_IDS` 后按该列表生效。

## 🤖 /ask 数据范围说明

`/ask` 与 `/ai` 只基于程序计算出的 `data_pack` 回答，主要包含：

- 今日按节点增量（含可读单位）
- 最近 1 小时按节点明细（可回答“刚刚这一小时哪台机器用了多少”）
- 最近 24 小时 Top（含可读单位）
- 最近 24 小时按小时分桶（含峰值/低谷小时）
- 今天按节点小时级走势（可回答“今天每小时各机器用了多少”）
- 昨天按节点小时级走势（可回答“某节点昨天最忙时段”）
- 最近 7 天按日总量 + 按节点累计排行（含可读单位）

若数据不足，AI 会明确说明无法判断。


## ℹ️ 启动提示说明

默认会发送一条启动提示，内容为“实例名 + 统计时区 + 可接收命令 chat 数”。

- 通过 `BOT_INSTANCE_NAME` 自定义实例标识（如 `hk-vps-prod`）。
- 通过 `BOT_START_NOTIFY=0` 关闭启动提示。


> 说明：数据会持续由 bot 采样/生成并写入本地文件（samples/history 等），但**不会在后台持续主动推送给 AI**；仅在你触发 `/ask` 或 `/ai` 时才会临时组包并调用 AI。

另外：`/ask` 数据包支持本地缓存（默认 1 小时），同一时间段连续追问会复用缓存，过期后自动重建。  
但像“刚刚这一小时 / 今天按小时”这类强时效问题，会自动绕过缓存并实时重建数据包，避免拿旧数据回答。


### 关于 TELEGRAM_ALLOWED_CHAT_IDS / TELEGRAM_ADMIN_CHAT_IDS

这两个变量可以留空；留空时默认回退为 `TELEGRAM_CHAT_ID`，不会导致 bot 收不到消息。
