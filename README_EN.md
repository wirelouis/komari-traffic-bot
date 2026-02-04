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
  - `/top [Nh|week|month]`
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

### 1️⃣ Create working directory

```
bash
mkdir -p komari-traffic && cd komari-traffic
mkdir -p data
```
### 2️⃣ Create .env
```
cat > .env <<'ENV'
# Komari panel base URL (no trailing slash)
KOMARI_BASE_URL=https://your-komari.example

# Telegram
TELEGRAM_BOT_TOKEN=123456:YOUR_BOT_TOKEN
TELEGRAM_CHAT_ID=123456789

# Container data directory (do not change)
DATA_DIR=/data

# Top ranking size
TOP_N=3

# Sampling for /top Nh
SAMPLE_INTERVAL_SECONDS=300
SAMPLE_RETENTION_HOURS=720

# History retention
HISTORY_HOT_DAYS=60
HISTORY_RETENTION_DAYS=400
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
