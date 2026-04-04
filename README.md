<p align="center">
  <a href="./README.md">简体中文</a> ｜ 
  <a href="./README_EN.md">English</a>
</p>

# komari-traffic-bot（Docker 版）

基于 **Komari 探针** 的流量统计增强工具，提供：

- 📊 Telegram **流量日报 / 周报 / 月报**
- 🔥 **Top N 流量消耗榜**（支持 `/top 6h`、`/top week` 等任意时间窗口）
- 🤖 Telegram Bot **交互式查询**
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
```
启动：
```
docker compose up -d
docker compose ps
```
### 5️⃣ 初始化（只需一次
```）
docker compose exec bot \
  python /app/komari_traffic_report.py bootstrap
```
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

升级 / 重启容器不会丢数据。

## 🔄 升级方式
```
docker pull ghcr.io/wirelouis/komari-traffic-bot:latest
docker compose up -d
```
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
