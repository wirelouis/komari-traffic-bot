# ⚠️ 需要重启服务!

## 问题诊断

你遇到的两个问题:
1. **节点页面 RAM/Disk 不显示** 
2. **柱状图刻度重叠显示"93.42亿0亿0 GiB"**

代码已经修复并推送,但**你的Web服务还在运行旧代码**!

## 已修复的代码

### 1. 后端修复 (c3f8357)
- `web_app.py:404-406` - `normalize_machine()` 现在提取 cpu/ram/disk
- `web_app.py:474-477` - `enrich_nodes_with_komari()` 复制健康数据到node对象

### 2. 前端修复 (93913ae, d544e66)  
- `app.js:898` - 刻度从5个→2个(只显示0%和100%)
- `styles.css:993` - 添加 `pointer-events: none`
- `index.html` - 版本号改为 `?v=20260611-ticks2`

## 如何修复

### Docker部署
```bash
cd C:\Users\20232\komari-traffic-hub

# 拉取最新代码
git pull

# 重新构建并重启
docker compose down
docker compose build
docker compose up -d
```

### 本地开发
```bash
cd C:\Users\20232\komari-traffic-hub

# 拉取最新代码
git pull

# 重启web进程
# 找到并kill现有的uvicorn进程,然后重新运行:
python -m uvicorn web_app:app --host 0.0.0.0 --port 8080
```

## 验证修复

1. **强制刷新浏览器**: Ctrl + Shift + R (清除CSS/JS缓存)
2. **检查版本**: F12 → Network → 查看 app.js 是否是 `?v=20260611-ticks2`
3. **节点页面**: 应该显示每台机器的 CPU/RAM/Disk 百分比
4. **总览页面**: 柱状图刻度轴只显示两个标签(0 B 和最大值)

## 为什么需要重启

- **后端**: Python代码在进程启动时加载,修改后必须重启进程
- **前端**: 修改了版本号 `?v=` 但浏览器可能还缓存了旧的HTML

## 确认问题已解决

运行以下检查:
```bash
# 1. 确认代码是最新的
git log --oneline -1
# 应该显示: 93913ae fix: 刻度轴改为只显示首尾两个标签,彻底解决重叠

# 2. 检查web_app.py中normalize_machine
grep -A3 "\"disk\": node.get" web_app.py
# 应该看到 404-406 行有 cpu/ram/disk

# 3. 检查app.js中的刻度配置  
grep "const ticks = " static/app.js
# 应该看到: const ticks = [0, 1].map(...)
```

如果还有问题,请提供:
- `docker compose ps` 或进程列表输出
- 浏览器F12 Console中的错误信息
- `/api/nodes` 响应数据(F12 → Network → nodes → Response)
