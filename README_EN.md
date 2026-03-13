<p align="center">
  <a href="./README.md">简体中文</a> ｜ 
  <a href="./README_EN.md">English</a>
</p>

# komari-traffic-bot (Docker Edition)

A **Dockerized traffic statistics extension** for **Komari Probe**, providing:

- 📊 Daily / Weekly / Monthly traffic reports via Telegram
- 🔥 Top N traffic consumers (supports `/top 6h`, `/top week`, etc.)
- 🤖 Interactive Telegram Bot commands
- 🐳 Docker / docker-compose deployment
- 🕒 Fixed statistics timezone (Asia/Shanghai by default)
- 🧱 Designed for multi-node and long-running environments

> This project does **not replace Komari**.  
> It enhances Komari with **long-term aggregation, arbitrary time window Top lists,
> and Telegram-based querying**.

---

## ✨ Features

- **Scheduled Reports**
  - Daily report at 00:00 (yesterday)
  - Weekly report (last week)
  - Monthly report (last month)
- **Top Traffic Ranking**
  - `/top` – today Top N (up + down)
  - `/top 6h` – last 6 hours
  - `/top week`, `/top month`
- **Telegram Commands**
  - `/today`, `/week`, `/month`
  - `/top [Nh|today|week|month]`
  - `/ask your question (or /ai)`
- **Stability & Reliability**
  - Slow or failed Komari nodes are skipped automatically
  - Telegram network errors are retried
  - Counter reset detection & fallback
- **Data Management**
  - Historical data auto-compression
  - Sampling system for arbitrary Nh queries

---

## 🧩 Requirements

- A running **Komari panel** (API accessible)
- Docker + docker-compose
- Telegram Bot Token
- Telegram Chat ID (user or group)

---

## 🚀 Quick Start (docker-compose)

### 1️⃣ Create data directory and set permissions (required)
This container runs as a non-root user (`uid:gid = 10001:10001`) and needs write access to the `data/` directory.
```
bash
mkdir -p komari-traffic && cd komari-traffic
mkdir -p data
sudo chown -R 10001:10001 data
sudo chmod -R u+rwX,go+rX data
```
> If you encounter `PermissionError: [Errno 13] Permission denied: '/data/...'` in the logs after startup,
> re-execute the above `chown` / `chmod` commands and restart the container.
### 2️⃣ Create .env
```
cp env.example .env
# Then edit .env as needed.

# Or create .env manually:
cat > .env <<'ENV'
# Komari panel base URL (no trailing slash)
KOMARI_BASE_URL=https://your-komari.example

# Komari API timeout (seconds)
KOMARI_TIMEOUT_SECONDS=15

# Komari API auth (optional)
KOMARI_API_TOKEN=
KOMARI_API_TOKEN_HEADER=Authorization
KOMARI_API_TOKEN_PREFIX=Bearer

# Komari fetch concurrency
KOMARI_FETCH_WORKERS=6

# Telegram
TELEGRAM_BOT_TOKEN=123456:YOUR_BOT_TOKEN
TELEGRAM_CHAT_ID=123456789

# Allowed command chats (optional, comma-separated)
TELEGRAM_ALLOWED_CHAT_IDS=

# Admin chats (optional, comma-separated)
TELEGRAM_ADMIN_CHAT_IDS=

# AI (optional, enables /ask and /ai)
AI_API_BASE=
AI_API_KEY=
AI_MODEL=

# AI data-pack cache TTL in seconds (default 3600; set 0 to disable)
AI_PACK_CACHE_TTL_SECONDS=3600

# Startup notification (optional)
# Set to 0 to disable startup message
BOT_START_NOTIFY=1

# Instance label shown in startup message (optional)
BOT_INSTANCE_NAME=

# Container data directory (do not change)
DATA_DIR=/data

# Statistics timezone (default Asia/Shanghai)
STAT_TZ=Asia/Shanghai

# Top ranking size
TOP_N=3

# Sampling for /top Nh
SAMPLE_INTERVAL_SECONDS=300
SAMPLE_RETENTION_HOURS=720

# History retention
HISTORY_HOT_DAYS=60
HISTORY_RETENTION_DAYS=400

# Logging
LOG_LEVEL=INFO
LOG_FILE=
ENV
```
### 3️⃣ Create crontab
```
cat > crontab <<'CRON'
# Daily report at 00:00
0 0 * * * python /app/komari_traffic_report.py report_daily

# Weekly report (Monday)
5 0 * * 1 python /app/komari_traffic_report.py report_weekly

# Monthly report
10 0 1 * * python /app/komari_traffic_report.py report_monthly
CRON
```
### 4️⃣ docker-compose.yml
```
version: "3.9"

services:
  komari-traffic-bot:
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

  komari-traffic-cron:
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
Start services:
```
docker compose up -d
```
### 5️⃣ Initialize baseline (run once)
```
docker compose exec komari-traffic-bot \
  python /app/komari_traffic_report.py bootstrap
```
## 🤖 Telegram Command Examples
| Command      | Description                 |
| ------------ | --------------------------- |
| `/today`     | Today traffic (00:00 → now) |
| `/week`      | Current week                |
| `/month`     | Current month               |
| `/top`       | Today Top N                 |
| `/top 6h`    | Top in last 6 hours         |
| `/top week`  | Weekly Top                  |
| `/top month` | Monthly Top                 |
| `/ask question` | AI answer based on data pack |
| `/ai question`  | Alias of `/ask` |
| `/help`         | Show command help |
## 🕒 Timezone

Statistics timezone: STAT_TZ (default Asia/Shanghai)

Scheduler timezone: container TZ

This ensures daily reports are triggered at local midnight.

## 📦 Data Persistence

All runtime data is stored in ./data:

Baselines

Samples (for /top Nh)

History (daily records & compressed archives)

Telegram update offset

Upgrades and restarts will not lose data.

## 🔄 Upgrade
```
docker pull ghcr.io/wirelouis/komari-traffic-bot:latest
docker compose up -d
```


## 🔐 Admin commands (admin chats only)

- `/archive` -> sends a confirmation code, then execute with `/confirm_archive <code>`
- `/bootstrap` -> blocked when historical-risk is detected; execute via `/confirm_bootstrap <code>`
- `/rebuild_baselines` -> sends confirmation code, then execute via `/confirm_rebuild_baselines <code>`

> By default only `TELEGRAM_CHAT_ID` is admin. Set `TELEGRAM_ADMIN_CHAT_IDS` to override.

## 🤖 /ask data scope

`/ask` and `/ai` can only use computed `data_pack` fields, including:

- today per-node deltas (with human-readable units)
- last 24h top nodes (with human-readable units)
- last 24h hourly buckets (including peak/valley hour)
- yesterday per-node hourly trend (for node-specific busy-hour analysis)
- last 7 days daily totals + per-node aggregate ranking (with human-readable units)

If data is insufficient, AI should explicitly say it cannot determine from current data.


## ℹ️ Startup notification

By default the bot sends a startup message containing: instance label, stats timezone, and number of allowed command chats.

- Use `BOT_INSTANCE_NAME` to set a meaningful instance label (e.g. `hk-vps-prod`).
- Use `BOT_START_NOTIFY=0` to disable startup notifications.


> Note: data is continuously sampled/generated into local files (e.g. samples/history), but it is **not continuously pushed to AI in background**. AI is called only when `/ask` or `/ai` is triggered.

Additionally, `/ask` data-pack caching is enabled locally by default (1 hour): short follow-up questions reuse cached pack; expired cache is rebuilt automatically.


### About TELEGRAM_ALLOWED_CHAT_IDS / TELEGRAM_ADMIN_CHAT_IDS

Both variables can be left empty; they fallback to `TELEGRAM_CHAT_ID`, so the bot can still receive commands by default.
