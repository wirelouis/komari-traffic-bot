const SIDEBAR_STORAGE_KEY = "komari.sidebarCollapsed";
const THEME_STORAGE_KEY = "komari.themeMode";
const THEME_MODES = ["auto", "light", "dark"];
const DISPLAY_LIMITS = {
  overviewNodes: 8,
  analyticsNodes: 10,
  analyticsGroups: 18,
  taskRuns: 6,
};

const routeConfig = {
  "/": {
    title: "流量分析工作台",
    subtitle: "查看探针流量、节点排行和服务状态。",
  },
  "/nodes": {
    title: "节点流量分析",
    subtitle: "按时间窗口比较节点上下行、合计流量和 Komari 机器绑定。",
  },
  "/alerts": {
    title: "告警控制",
    subtitle: "查看告警状态、阈值、静默窗口，并执行检查或推送。",
  },
  "/telegram": {
    title: "推送控制",
    subtitle: "预览周期报表、测试 Telegram，并查看计划任务。",
  },
  "/ai": {
    title: "数据问答",
    subtitle: "刷新数据包、使用快捷问题，快速定位流量异常。",
  },
  "/analytics": {
    title: "流量分析",
    subtitle: "按日期范围查看 SQLite 长期统计、节点贡献和分组趋势。",
  },
  "/system": {
    title: "系统健康",
    subtitle: "查看配置状态、运行记录、低敏配置和数据维护动作。",
  },
};

const state = {
  authenticated: false,
  overview: null,
  route: "/",
  nodesHours: 24,
  nodes: [],
  machines: [],
  schedules: [],
  selectedNodeUuid: "",
  bindingSourceId: "",
  sidebarCollapsed: false,
  themeMode: "auto",
  aiAsking: false,
  system: null,
};

const $ = (id) => document.getElementById(id);

function setVisible(id, visible) {
  $(id).classList.toggle("hidden", !visible);
}

function loginFields() {
  return [$("login-username"), $("login-password")].filter(Boolean);
}

function loginRememberEnabled() {
  return Boolean($("login-remember")?.checked);
}

function setLoginFieldMode({ remember = false, clear = false } = {}) {
  const form = $("login-form");
  const username = $("login-username");
  const password = $("login-password");
  if (!form || !username || !password) return;
  form.setAttribute("autocomplete", remember ? "on" : "off");
  if (remember) {
    form.removeAttribute("data-lpignore");
  } else {
    form.setAttribute("data-lpignore", "true");
  }
  username.setAttribute("name", remember ? "username" : "komari-user-field");
  password.setAttribute("name", remember ? "password" : "komari-pass-field");
  username.setAttribute("autocomplete", remember ? "username" : "new-password");
  password.setAttribute("autocomplete", remember ? "current-password" : "new-password");
  loginFields().forEach((input) => {
    if (clear) input.value = "";
    if (remember) {
      input.removeAttribute("readonly");
      input.removeAttribute("data-lpignore");
    } else {
      input.setAttribute("readonly", "readonly");
      input.setAttribute("data-lpignore", "true");
    }
  });
}

function lockAndClearLoginFields() {
  setLoginFieldMode({ remember: loginRememberEnabled(), clear: true });
}

function handleLoginRememberChange() {
  const remember = loginRememberEnabled();
  setLoginFieldMode({ remember, clear: !remember });
}

function unlockLoginFields() {
  if (loginRememberEnabled()) return;
  loginFields().forEach((input) => {
    input.removeAttribute("readonly");
  });
}

function showLoginView(message = "") {
  state.authenticated = false;
  state.overview = null;
  if ($("login-remember")) $("login-remember").checked = false;
  lockAndClearLoginFields();
  $("login-error").textContent = message;
  setVisible("login-view", true);
  setVisible("app-view", false);
  window.setTimeout(lockAndClearLoginFields, 80);
  window.setTimeout(lockAndClearLoginFields, 360);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function stripHtml(value) {
  return String(value ?? "").replace(/<[^>]+>/g, "");
}

function friendlyError(value) {
  const text = String(value || "").trim();
  if (!text) return "请求失败";
  if (/Invalid URL|No scheme supplied|MissingSchema|KOMARI_BASE_URL/i.test(text)) {
    return "Komari API 未配置或不可达，暂时没有可展示的数据。";
  }
  return text;
}

function normalizeThemeMode(value) {
  return THEME_MODES.includes(value) ? value : "auto";
}

function loadThemePreference() {
  try {
    state.themeMode = normalizeThemeMode(window.localStorage.getItem(THEME_STORAGE_KEY));
  } catch (_error) {
    state.themeMode = "auto";
  }
}

function saveThemePreference() {
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, state.themeMode);
  } catch (_error) {
    // Ignore storage failures in private browsing or restricted WebViews.
  }
}

function applyThemeMode() {
  document.documentElement.dataset.theme = state.themeMode;
  document.documentElement.style.colorScheme = state.themeMode === "dark" ? "dark" : (state.themeMode === "light" ? "light" : "light dark");
  document.querySelectorAll("#theme-switch [data-theme]").forEach((button) => {
    button.classList.toggle("active", button.dataset.theme === state.themeMode);
  });
}

// --- Motion helpers --------------------------------------------------------
const prefersReducedMotion = () =>
  Boolean(window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches);

let progressHideTimer = null;
function startRouteProgress() {
  const bar = $("route-progress");
  if (!bar) return;
  window.clearTimeout(progressHideTimer);
  bar.classList.remove("done");
  void bar.offsetWidth; // restart the slide cleanly on rapid navigation
  bar.classList.add("loading");
}
function stopRouteProgress() {
  const bar = $("route-progress");
  if (!bar) return;
  bar.classList.remove("loading");
  bar.classList.add("done");
  progressHideTimer = window.setTimeout(() => bar.classList.remove("done"), 360);
}

function retriggerPop(el) {
  if (!el) return;
  el.classList.remove("kt-pop");
  void el.offsetWidth;
  el.classList.add("kt-pop");
}

// Animate a metric value: count up the numeric part (keeping its unit suffix),
// otherwise just set the text with a subtle pop. Honors reduced-motion.
function setMetricValue(el, finalText) {
  if (!el) return;
  const text = String(finalText ?? "--");
  const match = text.match(/^(-?\d[\d,]*(?:\.\d+)?)(\s*\D[\s\S]*)?$/);
  if (!match || prefersReducedMotion()) {
    el.textContent = text;
    if (!match && !prefersReducedMotion()) retriggerPop(el);
    return;
  }
  const target = parseFloat(match[1].replace(/,/g, ""));
  const suffix = match[2] || "";
  const decimals = (match[1].split(".")[1] || "").length;
  if (!Number.isFinite(target)) {
    el.textContent = text;
    return;
  }
  const duration = 650;
  const startTime = performance.now();
  function step(now) {
    const t = Math.min(1, (now - startTime) / duration);
    const eased = 1 - Math.pow(1 - t, 3);
    el.textContent = `${(target * eased).toFixed(decimals)}${suffix}`;
    if (t < 1) requestAnimationFrame(step);
    else el.textContent = `${target.toFixed(decimals)}${suffix}`;
  }
  requestAnimationFrame(step);
}

// --- Skeleton placeholders -------------------------------------------------
function skelCards(n) {
  return Array.from({ length: n }, () => `<div class="kt-skeleton kt-skel-card"></div>`).join("");
}
function skelRows(n) {
  return `<div class="kt-skel-stack">${Array.from({ length: n }, () => `<div class="kt-skeleton kt-skel-row"></div>`).join("")}</div>`;
}
function skelTableRows(rows, cols) {
  const cells = Array.from({ length: cols }, () => `<td><div class="kt-skeleton kt-skel-line lg"></div></td>`).join("");
  return Array.from({ length: rows }, () => `<tr>${cells}</tr>`).join("");
}
function setSkel(id, html) {
  const el = $(id);
  // Only show a skeleton when the container has no real content yet (first paint).
  // Avoids flashing skeletons over existing data on refresh / navigating back.
  if (el && el.children.length === 0) el.innerHTML = html;
}
function showRouteSkeleton(route) {
  switch (route) {
    case "/":
      setSkel("overview-health", skelCards(4));
      setSkel("top-list", skelRows(4));
      setSkel("trend-chart", `<div class="kt-skeleton kt-skel-chart"></div>`);
      break;
    case "/nodes":
      setSkel("nodes-table", skelTableRows(6, 5));
      break;
    case "/alerts":
      setSkel("alerts-summary", skelCards(4));
      setSkel("alerts-body", skelRows(2));
      setSkel("alert-thresholds", skelRows(5));
      break;
    case "/telegram":
      setSkel("telegram-summary", skelCards(4));
      setSkel("telegram-schedules", skelRows(2));
      setSkel("telegram-task-runs", skelRows(3));
      break;
    case "/ai":
      setSkel("ai-summary", skelCards(4));
      setSkel("ai-sources", skelRows(3));
      break;
    case "/system":
      setSkel("system-summary", skelCards(4));
      setSkel("system-services", skelRows(3));
      setSkel("system-data", skelRows(2));
      setSkel("system-task-runs", skelRows(3));
      break;
    default:
      break;
  }
}

// On a failed load, replace any skeleton placeholders still showing in the
// active view with a friendly empty state so nothing shimmers forever.
function clearRouteSkeleton() {
  const view = document.querySelector(".route-view:not(.hidden)");
  if (!view) return;
  const containers = new Set();
  view.querySelectorAll(".kt-skeleton").forEach((el) => {
    const holder = el.closest("[id]");
    if (holder && holder !== view) containers.add(holder);
  });
  containers.forEach((c) => {
    c.innerHTML = `<div class="empty-state">暂时无法加载，请稍后重试。</div>`;
  });
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });
  const data = await response.json().catch(() => ({ ok: false, error: { message: "响应无法解析" } }));
  if (!response.ok || data.ok === false) {
    const error = new Error(friendlyError(data.error?.message || "请求失败"));
    error.payload = data;
    error.status = response.status;
    throw error;
  }
  return data.data;
}

async function postJson(path, body = {}) {
  return api(path, { method: "POST", body: JSON.stringify(body) });
}

function trafficRangeQuery(extra = {}) {
  const query = new URLSearchParams({
    from: $("traffic-range-from").value,
    to: $("traffic-range-to").value,
    group: $("traffic-range-group").value,
  });
  Object.entries(extra).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") query.set(key, String(value));
  });
  return query;
}

function filenameFromDisposition(header, fallback) {
  const text = String(header || "");
  const utf8 = text.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8) return decodeURIComponent(utf8[1]);
  const plain = text.match(/filename="?([^";]+)"?/i);
  return plain ? plain[1] : fallback;
}

function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function normalizeRoute(value) {
  let path = "/";
  try {
    path = new URL(value, window.location.origin).pathname;
  } catch (_error) {
    path = "/";
  }
  if (path.length > 1 && path.endsWith("/")) path = path.slice(0, -1);
  return routeConfig[path] ? path : "/";
}

function routeSearch() {
  return new URLSearchParams(window.location.search);
}

function routeUrl(target) {
  const url = new URL(target, window.location.origin);
  const route = normalizeRoute(url.pathname);
  return `${route}${url.search}`;
}

function updateTopbar(route) {
  const config = routeConfig[route] || routeConfig["/"];
  $("topbar-title").textContent = config.title;
  $("topbar-subtitle").textContent = config.subtitle;
  document.title = `${config.title} - Komari Traffic Console`;
}

function showRoute(route) {
  const nextRoute = normalizeRoute(route);
  state.route = nextRoute;
  document.querySelectorAll(".route-view").forEach((view) => {
    view.classList.toggle("hidden", view.dataset.route !== nextRoute);
  });
  document.querySelectorAll(".nav-link[data-route], .brand[data-route]").forEach((link) => {
    link.classList.toggle("active", link.dataset.route === nextRoute);
  });
  updateTopbar(nextRoute);
}

async function navigateRoute(target, options = {}) {
  const nextUrl = routeUrl(target);
  const nextRoute = normalizeRoute(nextUrl);
  const currentUrl = `${window.location.pathname}${window.location.search}`;
  if (options.replace || (!routeConfig[window.location.pathname] && currentUrl !== nextUrl)) {
    window.history.replaceState({ route: nextRoute }, "", nextUrl);
  } else if (options.push && currentUrl !== nextUrl) {
    window.history.pushState({ route: nextRoute }, "", nextUrl);
  }
  showRoute(nextRoute);
  if (options.scroll !== false) window.scrollTo(0, 0);
  if (options.load !== false && state.authenticated) {
    await loadCurrentRoute(Boolean(options.forceOverview));
  }
}

function updateStatus(message, good = true) {
  const pill = $("status-pill");
  pill.textContent = message;
  pill.classList.toggle("good", good);
  pill.classList.toggle("bad", !good);
}

function noteText(period) {
  if (!period?.ok) return friendlyError(period?.error?.message || "不可用");
  const data = period.data;
  if (data.note === "baseline_missing") return "基线缺失";
  const count = data.node_count ?? data.nodes?.length ?? 0;
  return data.hidden_node_count ? `${count} 个节点 · 展示摘要` : `${count} 个节点`;
}

function totalText(period) {
  if (!period?.ok) return "--";
  return period.data?.total?.total_human || "--";
}

function formatBytes(value) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let n = Math.max(0, Number(value || 0));
  for (let i = 0; i < units.length; i += 1) {
    if (n < 1024 || i === units.length - 1) {
      return units[i] === "B" ? `${Math.round(n)} B` : `${n.toFixed(2)} ${units[i]}`;
    }
    n /= 1024;
  }
  return "0 B";
}

function percentOf(value, max) {
  const n = Number(value || 0);
  const m = Math.max(1, Number(max || 0));
  return Math.max(0, Math.min(100, (n / m) * 100));
}

function sumBy(rows, key) {
  return (rows || []).reduce((total, row) => total + Number(row?.[key] || 0), 0);
}

function compactTrafficRows(rows, limit, label = "其他节点") {
  const items = Array.isArray(rows) ? rows : [];
  const maxItems = Math.max(1, Number(limit || items.length || 1));
  if (items.length <= maxItems) {
    return { rows: items, hidden: 0, hiddenTotal: 0 };
  }
  const visible = items.slice(0, maxItems);
  const rest = items.slice(maxItems);
  const up = sumBy(rest, "up");
  const down = sumBy(rest, "down");
  const total = up + down;
  return {
    rows: [
      ...visible,
      {
        uuid: "__other__",
        name: `${rest.length} 个${label}`,
        up,
        down,
        total,
        up_human: formatBytes(up),
        down_human: formatBytes(down),
        total_human: formatBytes(total),
        compact_other: true,
      },
    ],
    hidden: rest.length,
    hiddenTotal: total,
  };
}

function compactCompareRows(rows, limit) {
  const items = Array.isArray(rows) ? rows : [];
  const maxItems = Math.max(1, Number(limit || items.length || 1));
  if (items.length <= maxItems) {
    return { rows: items, hidden: 0 };
  }
  const visible = items.slice(0, maxItems);
  const rest = items.slice(maxItems);
  const last24 = sumBy(rest, "last24");
  const last7d = sumBy(rest, "last7d");
  return {
    rows: [
      ...visible,
      {
        uuid: "__other__",
        name: `${rest.length} 个其他节点`,
        last24,
        last7d,
        last24_human: formatBytes(last24),
        last7d_human: formatBytes(last7d),
        compact_other: true,
      },
    ],
    hidden: rest.length,
  };
}

function metric(stat, key) {
  const value = stat?.[key];
  return value === null || value === undefined || Number.isNaN(Number(value)) ? "--" : `${value}%`;
}

function hoursLabel(hours) {
  const value = Number(hours || 0);
  if (value === 1) return "最近 1 小时";
  if (value === 6) return "最近 6 小时";
  if (value === 24) return "最近 24 小时";
  if (value === 168) return "最近 7 天";
  if (value === 720) return "最近 30 天";
  return `${value} 小时窗口`;
}

function miniCard(label, value, note = "", status = "") {
  const statusText = status === "good" ? "正常" : (status === "bad" ? "需处理" : "注意");
  return `
    <article class="mini-card">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value || "--")}</strong>
      ${note ? `<small class="tiny">${escapeHtml(note)}</small>` : ""}
      ${status ? `<span class="pill ${escapeHtml(status)}">${escapeHtml(statusText)}</span>` : ""}
    </article>`;
}

function isoDay(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function setDefaultRangeDates() {
  const to = new Date();
  const from = new Date(to.getTime() - 29 * 24 * 3600 * 1000);
  if (!$("traffic-range-to").value) $("traffic-range-to").value = isoDay(to);
  if (!$("traffic-range-from").value) $("traffic-range-from").value = isoDay(from);
}

function runTypeLabel(type) {
  const map = { report: "报表", alert: "告警", ai: "AI", sample: "采样", maintenance: "维护" };
  return map[type] || type || "任务";
}

function aiSourceLabel(key) {
  const map = {
    today: "今日节点",
    last_1h: "最近 1 小时",
    last_24h: "最近 24 小时",
    last_7d: "最近 7 天",
    history: "历史趋势",
    alerts: "告警状态",
  };
  return map[key] || key || "数据源";
}

function runStatusClass(status) {
  if (status === "success") return "good";
  if (status === "failed") return "bad";
  return "";
}

function runStatusText(status) {
  if (status === "success") return "成功";
  if (status === "failed") return "失败";
  return status || "未知";
}

function statusLevelClass(level, ok = false) {
  if (level === "bad") return "bad";
  if (level === "warn" || level === "muted") return "warn";
  return ok ? "good" : "";
}

function statusLevelText(level, ok = false) {
  if (level === "bad") return "需处理";
  if (level === "warn") return "提醒";
  if (level === "muted") return "已关闭";
  return ok || level === "ok" ? "正常" : "待确认";
}

function taskRunNote(run) {
  if (run.error) return run.error;
  if (run.summary) return run.summary;
  if (run.status === "success") return "任务已完成。";
  if (run.status === "failed") return "任务执行失败，请查看错误。";
  return "任务状态未记录完整。";
}

function renderTaskRuns(targetId, runs) {
  const target = $(targetId);
  const rawItems = runs || [];
  const items = rawItems.slice(0, DISPLAY_LIMITS.taskRuns);
  const hidden = Math.max(0, rawItems.length - items.length);
  target.innerHTML = items.length
    ? `${items.map((run) => `
      <div class="task-run-row ${run.status === "failed" ? "failed" : ""}">
        <span>
          <strong>${escapeHtml(runTypeLabel(run.type))}</strong><br>
          <span class="tiny">${escapeHtml(taskRunNote(run))}</span><br>
          <span class="tiny">${escapeHtml(run.started_at_text || "未记录时间")} · 耗时 ${escapeHtml(run.duration_text || "--")}</span>
        </span>
        <span class="pill ${runStatusClass(run.status)}">${escapeHtml(runStatusText(run.status))}</span>
      </div>`).join("")}${hidden ? `<div class="compact-note">还有 ${hidden} 条较早记录未展开。</div>` : ""}`
    : `<div class="empty-state">最近没有需要展示的任务记录。</div>`;
}

async function loadTaskRuns(targetId, type = "", limit = DISPLAY_LIMITS.taskRuns) {
  const query = new URLSearchParams({ limit: String(limit) });
  if (type) query.set("type", type);
  const data = await api(`/api/tasks/runs?${query.toString()}`);
  renderTaskRuns(targetId, data.runs || []);
  return data.runs || [];
}

function nodeByUuid(uuid) {
  return state.nodes.find((node) => String(node.uuid) === String(uuid));
}

function bindingLabel(binding) {
  if (!binding) return "未绑定";
  if (binding.mode === "manual") return binding.stale ? "手动失效" : "手动";
  if (binding.mode === "auto") return "自动";
  return "未绑定";
}

function nodeWebUrl(node) {
  return node?.komari?.web_url || "";
}

async function jumpToNode(uuid) {
  if (!uuid) return;
  await navigateRoute(`/nodes?node=${encodeURIComponent(uuid)}`, { push: true });
}

function bindNodeJumpButtons() {
  document.querySelectorAll("[data-jump-node]").forEach((button) => {
    button.addEventListener("click", () => jumpToNode(button.dataset.jumpNode));
  });
}

function renderOverviewHealth(system) {
  const target = $("overview-health");
  if (!target) return;
  if (!system) {
    target.innerHTML = [
      miniCard("系统健康", "同步中", "稍后显示状态"),
      miniCard("最近任务", "--", "暂无记录"),
      miniCard("长期统计", "--", "稍后显示状态"),
      miniCard("计划任务", "--", "稍后显示状态"),
    ].join("");
    return;
  }
  const summary = system.summary || {};
  const latestReport = system.latest_runs?.report;
  const db = system.data?.sqlite || {};
  const schedules = system.runtime?.schedules || {};
  target.innerHTML = [
    miniCard("系统健康", `${summary.healthy || 0}/${summary.total || 0}`, (summary.issues || []).length ? `待确认：${(summary.issues || []).join("、")}` : "核心配置正常", (summary.issues || []).length ? "bad" : "good"),
    miniCard("最近任务", latestReport?.status ? runStatusText(latestReport.status) : "暂无", latestReport?.started_at_text || "未记录", latestReport?.status === "failed" ? "bad" : ""),
    miniCard("长期统计", db.ok ? "正常" : "异常", db.ok ? "历史流量会继续保存" : "历史流量可能无法保存", db.ok ? "good" : "bad"),
    miniCard("计划任务", `${schedules.enabled || 0}/${schedules.total || 0}`, "启用 / 总数"),
  ].join("");
}

function renderOverview(data) {
  state.overview = data;
  const periods = data.periods || {};
  setMetricValue($("metric-today"), totalText(periods.today));
  setMetricValue($("metric-week"), totalText(periods.week));
  setMetricValue($("metric-month"), totalText(periods.month));
  $("metric-today-note").textContent = noteText(periods.today);
  $("metric-week-note").textContent = noteText(periods.week);
  $("metric-month-note").textContent = noteText(periods.month);
  $("metric-services").textContent = [
    data.services?.komari?.configured ? "Komari" : "",
    data.services?.telegram?.configured ? "TG" : "",
    data.services?.ai?.configured ? "AI" : "",
    data.services?.alerts?.enabled ? "告警" : "",
  ].filter(Boolean).join(" / ") || "未配置";
  $("metric-time").textContent = data.now || "--";
  $("range-label").textContent = "24h / 7d";

  const topNodes = periods.today?.data?.top_nodes || periods.today?.data?.nodes?.slice(0, 5) || [];
  $("top-list").innerHTML = topNodes.length
    ? topNodes.map((n, index) => `
      <li>
        <button class="top-node-button" data-jump-node="${escapeHtml(n.uuid)}" title="跳到 ${escapeHtml(n.name)}">
          <span class="rank">${index + 1}.</span>
          <span>
            <span class="node-name">${escapeHtml(n.name)}</span><br>
            <span class="tiny">${escapeHtml(n.total_human)} · 下行 ${escapeHtml(n.down_human)} / 上行 ${escapeHtml(n.up_human)}</span>
          </span>
        </button>
      </li>`).join("")
    : `<li class="muted">暂无 Top 数据</li>`;

  renderChart(data);
  bindNodeJumpButtons();
}

function renderChart(data) {
  const byId = new Map();
  const mergeNodes = (nodes, key) => {
    (nodes || []).forEach((node) => {
      const uuid = String(node.uuid || node.name || "");
      if (!uuid) return;
      const row = byId.get(uuid) || { uuid, name: node.name || uuid, last24: 0, last7d: 0, compact_other: Boolean(node.compact_other) };
      row.name = node.name || row.name;
      row.compact_other = row.compact_other || Boolean(node.compact_other);
      row[key] = Number(node.total || 0);
      row[`${key}_human`] = node.total_human || formatBytes(node.total);
      byId.set(uuid, row);
    });
  };
  mergeNodes(data.records?.last_24h?.data?.nodes || [], "last24");
  mergeNodes(data.records?.last_7d?.data?.nodes || [], "last7d");
  const allNodes = [...byId.values()].sort((a, b) => {
    if (a.compact_other !== b.compact_other) return a.compact_other ? 1 : -1;
    return (b.last7d + b.last24) - (a.last7d + a.last24);
  });
  const chart = $("trend-chart");
  if (!allNodes.length) {
    const msg = data.records?.last_24h?.error?.message || data.records?.last_7d?.error?.message || "暂无 records 数据";
    chart.innerHTML = `<div class="empty-state">${escapeHtml(friendlyError(msg))}</div>`;
    return;
  }
  const hiddenFromApi = Math.max(
    Number(data.records?.last_24h?.data?.hidden_node_count || 0),
    Number(data.records?.last_7d?.data?.hidden_node_count || 0),
  );
  const alreadyCompact = allNodes.some((node) => node.compact_other);
  const compact = alreadyCompact ? { rows: allNodes, hidden: hiddenFromApi } : compactCompareRows(allNodes, DISPLAY_LIMITS.overviewNodes);
  const nodes = compact.rows;
  const totalNodeCount = Math.max(
    Number(data.records?.last_24h?.data?.node_count || 0),
    Number(data.records?.last_7d?.data?.node_count || 0),
    allNodes.length,
  );
  const max = Math.max(...nodes.flatMap((n) => [n.last24, n.last7d]), 1);
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((ratio) => ({ ratio, label: formatBytes(max * ratio) }));
  chart.innerHTML = `
    <div class="traffic-chart">
      <div class="traffic-chart-legend">
        <span><i class="legend-dot day"></i>最近 24h</span>
        <span><i class="legend-dot week"></i>最近 7d</span>
        <span>展示 ${Math.min(totalNodeCount, DISPLAY_LIMITS.overviewNodes)} / ${totalNodeCount} 个节点</span>
        ${compact.hidden ? `<span>${compact.hidden} 个节点已汇总</span>` : ""}
      </div>
      <div class="traffic-axis">
        <span></span>
        <div class="axis-ticks">${ticks.map((tick) => `<span style="left:${tick.ratio * 100}%">${escapeHtml(tick.label)}</span>`).join("")}</div>
      </div>
      <div class="traffic-chart-rows">
        ${nodes.map((n) => `
          <${n.compact_other ? "div" : "button"} class="traffic-chart-row ${n.compact_other ? "compact-other" : ""}" ${n.compact_other ? "" : `data-jump-node="${escapeHtml(n.uuid)}" title="${escapeHtml(n.name)}"`}>
            <span class="traffic-y-label">${escapeHtml(n.name)}</span>
            <span class="traffic-bars">
              <span class="traffic-bar-line">
                <span class="traffic-bar day" style="width:${percentOf(n.last24, max).toFixed(2)}%"></span>
                <span class="traffic-value">${escapeHtml(n.last24_human || formatBytes(n.last24))}</span>
              </span>
              <span class="traffic-bar-line">
                <span class="traffic-bar week" style="width:${percentOf(n.last7d, max).toFixed(2)}%"></span>
                <span class="traffic-value">${escapeHtml(n.last7d_human || formatBytes(n.last7d))}</span>
              </span>
            </span>
          </${n.compact_other ? "div" : "button"}>`).join("")}
      </div>
    </div>`;
  bindNodeJumpButtons();
}

async function loadOverview() {
  const overview = await api("/api/overview");
  renderOverview(overview);
  try {
    const system = await api("/api/system/status");
    renderOverviewHealth(system);
  } catch (_error) {
    renderOverviewHealth(null);
  }
}

function renderNodesTable() {
  $("nodes-table").innerHTML = state.nodes.length
    ? state.nodes.map((n) => {
      const machine = n.komari?.machine;
      const binding = n.binding || {};
      const stale = Boolean(binding.stale);
      const url = nodeWebUrl(n);
      return `
        <tr data-uuid="${escapeHtml(n.uuid)}">
          <td><span class="node-name">${escapeHtml(n.name)}</span><div class="tiny">${escapeHtml(n.uuid)}</div></td>
          <td>
            <div class="traffic-cell">
              <strong>${escapeHtml(n.total_human)}</strong>
              <span class="tiny">下行 ${escapeHtml(n.down_human)} · 上行 ${escapeHtml(n.up_human)}</span>
            </div>
          </td>
          <td>
            <div class="health-cell">
              <span>CPU ${metric(n.cpu, "avg")}</span>
              <span class="tiny">RAM ${metric(n.ram, "avg")} · Disk ${metric(n.disk, "avg")}</span>
            </div>
          </td>
          <td>
            <div class="machine-cell">
              <span>${escapeHtml(machine?.name || (stale ? "绑定异常" : "未绑定"))}</span>
              <span class="tiny">${escapeHtml(bindingLabel(binding))}${machine?.region ? ` · ${escapeHtml(machine.region)}` : ""}</span>
            </div>
          </td>
          <td>
            <span class="node-actions">
              <button class="text-btn" data-node-action="detail" data-uuid="${escapeHtml(n.uuid)}">详情</button>
              <button class="text-btn" data-node-action="open" data-uuid="${escapeHtml(n.uuid)}" ${url ? "" : "disabled"}>打开</button>
            </span>
          </td>
        </tr>`;
    }).join("")
    : `<tr><td colspan="5">暂无节点数据</td></tr>`;

  document.querySelectorAll("#nodes-table tr[data-uuid]").forEach((row) => {
    row.addEventListener("click", async (event) => {
      if (event.target.closest("button,a,select")) return;
      await selectNode(row.dataset.uuid, { scroll: false });
    });
  });
  document.querySelectorAll("[data-node-action]").forEach((button) => {
    button.addEventListener("click", async (event) => {
      event.stopPropagation();
      const uuid = button.dataset.uuid;
      const action = button.dataset.nodeAction;
      if (action === "open") openKomariNode(uuid);
      if (action === "detail") await selectNode(uuid, { scroll: true });
    });
  });
}

async function loadNodes(hours = state.nodesHours) {
  state.nodesHours = hours;
  try {
    const data = await api(`/api/nodes?hours=${hours}`);
    state.nodes = data.nodes || [];
    state.machines = data.machines || state.machines || [];
    renderNodesTable();
    const target = routeSearch().get("node") || state.selectedNodeUuid;
    if (target && nodeByUuid(target)) {
      await selectNode(target, { scroll: Boolean(routeSearch().get("node")) });
    } else {
      $("node-detail").textContent = "选择节点查看详情；点击打开按钮进入 Komari 机器。";
    }
  } catch (error) {
    $("nodes-table").innerHTML = `<tr><td colspan="5">${escapeHtml(friendlyError(error.message))}</td></tr>`;
  }
}

async function selectNode(uuid, options = {}) {
  state.selectedNodeUuid = uuid;
  document.querySelectorAll("#nodes-table tr[data-uuid]").forEach((row) => {
    row.classList.toggle("selected", row.dataset.uuid === uuid);
  });
  const row = document.querySelector(`#nodes-table tr[data-uuid="${CSS.escape(uuid)}"]`);
  if (options.scroll && row) row.scrollIntoView({ block: "center", behavior: "smooth" });
  await loadNodeDetail(uuid);
}

function renderNodeDetail(node) {
  const machine = node.komari?.machine;
  const binding = node.binding || {};
  const bindingNeedsAttention = binding.mode !== "auto" || Boolean(binding.stale);
  const machineNote = machine
    ? [machine.uuid, machine.region, machine.group].filter(Boolean).join(" · ")
    : (binding.stale ? "绑定目标不存在或 Komari 暂不可达" : "暂无可打开的 Komari 机器");
  $("node-detail").innerHTML = `
      <div class="detail-drawer">
      <div class="detail-main">
        <div class="detail-title">
          <h3>${escapeHtml(node.name)}</h3>
          <p class="tiny">${escapeHtml(node.uuid)}</p>
        </div>
        <div class="detail-actions">
          <span class="pill ${binding.stale ? "bad" : "good"}">${escapeHtml(bindingLabel(binding))}</span>
          <span class="soft-label">${escapeHtml(hoursLabel(state.nodesHours))}</span>
          <button class="primary-btn" id="detail-open-node" ${nodeWebUrl(node) ? "" : "disabled"}>打开 Komari 机器</button>
        </div>
      </div>
      <div class="detail-grid">
        <div class="detail-item"><span>合计</span><strong>${escapeHtml(node.total_human)}</strong></div>
        <div class="detail-item"><span>下行</span><strong>${escapeHtml(node.down_human)}</strong></div>
        <div class="detail-item"><span>上行</span><strong>${escapeHtml(node.up_human)}</strong></div>
        <div class="detail-item"><span>Komari 机器</span><strong>${escapeHtml(machine?.name || "未绑定")}</strong><small>${escapeHtml(machineNote)}</small></div>
        <div class="detail-item"><span>CPU 平均</span><strong>${metric(node.cpu, "avg")}</strong><small>峰值 ${metric(node.cpu, "max")}</small></div>
        <div class="detail-item"><span>RAM 平均</span><strong>${metric(node.ram, "avg")}</strong><small>峰值 ${metric(node.ram, "max")}</small></div>
        <div class="detail-item"><span>Disk 平均</span><strong>${metric(node.disk, "avg")}</strong><small>峰值 ${metric(node.disk, "max")}</small></div>
        <div class="detail-item"><span>记录数</span><strong>${escapeHtml(node.record_count || "--")}</strong><small>${escapeHtml(node.from || "")} ${node.to ? `→ ${escapeHtml(node.to)}` : ""}</small></div>
      </div>
      <details class="advanced-bind" ${bindingNeedsAttention ? "open" : ""}>
        <summary>${bindingNeedsAttention ? "绑定设置需要确认" : "绑定设置"}</summary>
        <div class="advanced-bind-body">
          <span class="tiny">默认按节点 uuid 自动绑定；只有自动匹配不准时再手动覆盖。</span>
          <button class="ghost-btn" id="detail-bind-node">修改绑定</button>
        </div>
      </details>
    </div>`;
  $("detail-open-node").addEventListener("click", () => openKomariNode(node.uuid));
  $("detail-bind-node").addEventListener("click", () => openBindingPanel(node.uuid));
}

async function loadNodeDetail(uuid) {
  $("node-detail").textContent = "加载节点详情...";
  try {
    const data = await api(`/api/nodes/${encodeURIComponent(uuid)}?hours=${state.nodesHours}`);
    renderNodeDetail(data.node);
  } catch (error) {
    $("node-detail").textContent = friendlyError(error.message);
  }
}

function openKomariNode(uuid) {
  const node = nodeByUuid(uuid);
  const url = nodeWebUrl(node);
  if (!url) {
    updateStatus("节点未绑定 Komari 机器", false);
    selectNode(uuid).catch(() => null);
    return;
  }
  window.open(url, "_blank", "noopener");
}

function openBindingPanel(uuid) {
  const node = nodeByUuid(uuid);
  if (!node) return;
  state.bindingSourceId = uuid;
  $("bind-node-name").textContent = `绑定：${node.name}`;
  $("bind-node-note").textContent = `当前 ${bindingLabel(node.binding)}，默认使用同 uuid 自动绑定。`;
  const current = node.binding?.mode === "manual" ? node.binding.komari_uuid : "";
  $("bind-machine-select").innerHTML = [
    `<option value="">自动绑定（按节点 uuid）</option>`,
    ...state.machines.map((machine) => `<option value="${escapeHtml(machine.uuid)}">${escapeHtml(machine.name)} · ${escapeHtml(machine.uuid)}</option>`),
  ].join("");
  $("bind-machine-select").value = current;
  $("node-bind-panel").classList.remove("hidden");
}

async function saveBinding(clear = false) {
  if (!state.bindingSourceId) return;
  const komariUuid = clear ? "" : $("bind-machine-select").value;
  await postJson("/api/node-bindings", {
    source_id: state.bindingSourceId,
    komari_uuid: komariUuid,
  });
  updateStatus(clear ? "绑定覆盖已清除" : "绑定已保存", true);
  await loadNodes(state.nodesHours);
  openBindingPanel(state.bindingSourceId);
}

function renderAlerts(data) {
  $("alert-count").textContent = `${data.active_count || 0} active`;
  $("alerts-summary").innerHTML = [
    miniCard("启用状态", data.enabled ? "已启用" : "未启用", data.in_silence_window ? "当前处于静默时段" : "", data.enabled ? "good" : "bad"),
    miniCard("Active 告警", String(data.active_count || 0), "当前未恢复事件"),
    miniCard("冷却时间", data.cooldown_text || "--", `${data.window_minutes || 0}m 窗口`),
    miniCard("告警 Chat", data.alert_chat || "未配置", data.muted_until ? `静默至 ${data.muted_until}` : "未静默"),
  ].join("");

  const active = data.active || [];
  $("alerts-body").innerHTML = active.length
    ? active.map((item) => `
      <div class="alert-row">
        <span><strong>${escapeHtml(item.title)}</strong><br><span class="tiny">${escapeHtml(item.type || item.key)}</span></span>
        <span class="tiny">${escapeHtml(item.last_seen_text || "未记录")}</span>
      </div>`).join("")
    : `<div class="empty-state">当前无 active 告警。</div>`;

  const thresholds = data.thresholds || {};
  const thresholdRows = [
    ["窗口总流量", thresholds.total_window],
    ["窗口单节点", thresholds.node_window],
    ["日总流量", thresholds.daily_total],
    ["日单节点", thresholds.daily_node],
    ["静默窗口", data.silence_windows || "未配置"],
  ];
  $("alert-thresholds").innerHTML = thresholdRows
    .map(([label, value]) => `<div class="status-row"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value || "未配置")}</strong></div>`)
    .join("");
}

async function loadAlerts() {
  try {
    const [alerts, config] = await Promise.all([
      api("/api/alerts"),
      api("/api/system/config"),
    ]);
    renderAlerts(alerts);
    renderAlertConfig(config);
  } catch (error) {
    $("alerts-body").textContent = friendlyError(error.message);
    $("alert-config-form").innerHTML = `<div class="empty-state">${escapeHtml(friendlyError(error.message))}</div>`;
  }
}

async function runAlertCheck(notify) {
  $("alert-result").textContent = "检查中...";
  try {
    const data = await postJson("/api/alerts/check", { notify });
    renderAlertCheckResult(data);
    updateStatus(notify ? "告警已检查并推送" : "告警已检查", true);
    await loadAlerts();
  } catch (error) {
    $("alert-result").textContent = friendlyError(error.message);
    updateStatus(error.message, false);
  }
}

function renderAlertCheckResult(data) {
  const summary = data.summary || {};
  const level = summary.level === "warn" ? "bad" : "good";
  const title = summary.title || (data.events?.length ? "检查完成，发现事件" : "检查完成，暂无异常");
  const message = summary.message || `当前 active 告警 ${data.active_count || 0} 个。`;
  const items = summary.items || [];
  $("alert-result").innerHTML = `
    <div class="alert-check-card ${level}">
      <div>
        <strong>${escapeHtml(title)}</strong>
        <p>${escapeHtml(message)}</p>
      </div>
      <div class="alert-check-meta">
        <span class="pill ${level}">事件 ${escapeHtml(summary.events_count ?? (data.events || []).length)}</span>
        <span class="pill">Active ${escapeHtml(summary.active_count ?? data.active_count ?? 0)}</span>
        <span class="pill">${summary.notified ? "已推送" : "未推送"}</span>
      </div>
      ${items.length ? `<ul>${items.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>` : ""}
    </div>`;
}

async function muteAlerts(hours) {
  $("alert-result").textContent = "设置静默中...";
  try {
    const data = await postJson("/api/alerts/mute", { hours: Number(hours || 1) });
    $("alert-result").textContent = `已静默至 ${data.muted_until}`;
    await loadAlerts();
  } catch (error) {
    $("alert-result").textContent = friendlyError(error.message);
  }
}

function scheduleBody() {
  return {
    enabled: $("schedule-enabled").checked,
    scope: $("schedule-scope").value,
    mode: $("schedule-mode").value,
    time: $("schedule-time").value || "09:00",
    weekday: Number($("schedule-weekday").value || 0),
    month_day: Number($("schedule-month-day").value || 1),
    chat: "",
  };
}

function updateScheduleFormVisibility() {
  const scope = $("schedule-scope").value;
  $("schedule-weekday-wrap").classList.toggle("hidden", scope !== "weekly");
  $("schedule-month-day-wrap").classList.toggle("hidden", scope !== "monthly");
}

function resetScheduleForm() {
  $("schedule-id").value = "";
  $("schedule-scope").value = "daily";
  $("schedule-time").value = "09:00";
  $("schedule-weekday").value = "0";
  $("schedule-month-day").value = "1";
  $("schedule-mode").value = "full";
  $("schedule-enabled").checked = true;
  $("save-schedule-btn").textContent = "保存计划";
  updateScheduleFormVisibility();
}

function editSchedule(id) {
  const item = state.schedules.find((schedule) => schedule.id === id);
  if (!item) return;
  $("schedule-id").value = item.id;
  $("schedule-scope").value = item.scope || "daily";
  $("schedule-time").value = item.time || "09:00";
  $("schedule-weekday").value = String(item.weekday ?? 0);
  $("schedule-month-day").value = String(item.month_day ?? 1);
  $("schedule-mode").value = item.mode || "full";
  $("schedule-enabled").checked = Boolean(item.enabled);
  $("save-schedule-btn").textContent = "更新计划";
  updateScheduleFormVisibility();
  $("schedule-scope").focus();
}

async function saveSchedule() {
  const id = $("schedule-id").value;
  const options = {
    method: id ? "PATCH" : "POST",
    body: JSON.stringify(scheduleBody()),
  };
  $("telegram-result").textContent = id ? "更新计划中..." : "保存计划中...";
  try {
    await api(id ? `/api/schedules/${encodeURIComponent(id)}` : "/api/schedules", options);
    $("telegram-result").textContent = id ? "计划已更新。" : "计划已保存。";
    resetScheduleForm();
    await loadTelegramStatus();
  } catch (error) {
    $("telegram-result").textContent = friendlyError(error.message);
  }
}

async function deleteSchedule(id) {
  if (!id || !window.confirm("删除这条推送计划？")) return;
  $("telegram-result").textContent = "删除计划中...";
  try {
    await api(`/api/schedules/${encodeURIComponent(id)}`, { method: "DELETE" });
    $("telegram-result").textContent = "计划已删除。";
    resetScheduleForm();
    await loadTelegramStatus();
  } catch (error) {
    $("telegram-result").textContent = friendlyError(error.message);
  }
}

async function runScheduleNow(id) {
  if (!id) return;
  $("telegram-result").textContent = "立即发送中...";
  try {
    const data = await postJson(`/api/schedules/${encodeURIComponent(id)}/run-now`, {});
    $("telegram-result").textContent = `已发送：${data.label || "计划任务"}，目标 ${data.chat || "默认 Chat"}；运行记录已写入。`;
    await loadTelegramStatus();
  } catch (error) {
    $("telegram-result").textContent = friendlyError(error.message);
  }
}

function bindScheduleActions() {
  document.querySelectorAll("[data-schedule-action]").forEach((button) => {
    button.addEventListener("click", () => {
      const id = button.dataset.scheduleId;
      const action = button.dataset.scheduleAction;
      if (action === "edit") editSchedule(id);
      if (action === "delete") deleteSchedule(id);
      if (action === "run") runScheduleNow(id);
    });
  });
}

function renderTelegramStatus(data) {
  $("telegram-status-pill").textContent = data.configured ? "已配置" : "未配置";
  $("telegram-status-pill").classList.toggle("good", Boolean(data.configured));
  $("telegram-status-pill").classList.toggle("bad", !data.configured);
  const schedules = data.schedules || [];
  state.schedules = schedules;
  $("telegram-summary").innerHTML = [
    miniCard("发送状态", data.configured ? "可发送" : "不可发送", data.bot_token_configured ? "Bot Token 已配置" : "缺少 Bot Token", data.configured ? "good" : "bad"),
    miniCard("默认 Chat", data.chat || "未配置"),
    miniCard("告警 Chat", data.alert_chat || "未配置"),
    miniCard("应用内计划", `${schedules.length} 条`, "面板可编辑"),
  ].join("");
  const appRows = schedules.length
    ? schedules.map((item) => `
      <div class="schedule-row ${item.enabled ? "" : "muted-row"}">
        <span>
          <strong>${escapeHtml(item.label || "推送计划")}</strong><br>
          <span class="tiny">${item.enabled ? "已启用" : "已停用"} · ${item.mode === "top" ? "Top 报表" : "完整报表"} · ${item.chat_masked ? `Chat ${escapeHtml(item.chat_masked)}` : "默认 Chat"}</span><br>
          <span class="tiny">上次：${escapeHtml(item.last_run?.started_at_text || "暂无")} ${item.last_status ? `· ${escapeHtml(runStatusText(item.last_status))}` : ""}</span><br>
          <span class="tiny">下次：${escapeHtml(item.next_run_text || (item.enabled ? "等待计算" : "已停用"))}</span>
        </span>
        <span class="schedule-actions">
          <span class="pill ${item.enabled ? "good" : ""}">${item.enabled ? "启用" : "停用"}</span>
          <button class="text-btn" data-schedule-action="edit" data-schedule-id="${escapeHtml(item.id)}">编辑</button>
          <button class="text-btn" data-schedule-action="run" data-schedule-id="${escapeHtml(item.id)}">立即发送</button>
          <button class="text-btn danger" data-schedule-action="delete" data-schedule-id="${escapeHtml(item.id)}">删除</button>
        </span>
      </div>`).join("")
    : `<div class="empty-state">还没有应用内计划，可用下方表单新增每日、每周或每月推送。</div>`;
  $("telegram-schedules").innerHTML = appRows;
  bindScheduleActions();
}

async function loadTelegramStatus() {
  try {
    const status = await api("/api/telegram/status");
    const schedules = await api("/api/schedules");
    renderTelegramStatus({
      ...status,
      schedules: schedules.schedules || [],
      schedule_path: schedules.path,
    });
    await loadTaskRuns("telegram-task-runs", "report", DISPLAY_LIMITS.taskRuns);
  } catch (error) {
    $("telegram-summary").innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
  }
}

function reportRequestBody() {
  return {
    scope: $("report-scope").value,
    mode: $("report-mode").value,
  };
}

async function previewReport() {
  $("telegram-preview").textContent = "生成预览中...";
  try {
    const data = await postJson("/api/telegram/preview", reportRequestBody());
    $("telegram-preview").textContent = stripHtml(data.message || "");
    $("telegram-result").textContent = `预览已生成，目标 chat：${data.chat || "未配置"}`;
  } catch (error) {
    $("telegram-preview").textContent = friendlyError(error.message);
  }
}

async function sendReport() {
  $("telegram-result").textContent = "发送中...";
  try {
    const data = await postJson("/api/telegram/report", reportRequestBody());
    $("telegram-result").textContent = `已发送到 ${data.chat}；运行记录已写入。`;
    await loadTelegramStatus();
  } catch (error) {
    $("telegram-result").textContent = friendlyError(error.message);
  }
}

function renderAiStatus(data) {
  $("ai-status").textContent = data.configured ? "已配置" : "未配置";
  $("ai-status").classList.toggle("good", Boolean(data.configured));
  $("ai-status").classList.toggle("bad", !data.configured);
  $("ai-summary").innerHTML = [
    miniCard("AI 状态", data.configured ? "可用" : "不可用", data.model ? `模型 ${data.model}` : "未配置模型", data.configured ? "good" : "bad"),
    miniCard("缓存状态", data.cache_valid ? "有效" : "待刷新", data.cache_created_at_text || "尚未生成"),
    miniCard("缓存 TTL", data.cache_ttl_seconds ? `${Math.round(data.cache_ttl_seconds / 60)} 分钟` : "实时生成"),
    miniCard("数据源", `${(data.data_sources || []).length} 项`, "供 AI 分析使用"),
  ].join("");
  const sources = data.data_sources || [];
  $("ai-sources").innerHTML = sources.length
    ? sources.map((item) => `
      <div class="source-row">
        <span><strong>${escapeHtml(aiSourceLabel(item.key))}</strong><br><span class="tiny">${item.count || 0} 条记录</span></span>
        <span class="pill ${item.status === "ok" ? "good" : "bad"}">${escapeHtml(item.status)}</span>
      </div>`).join("")
    : `<div class="empty-state">暂无 AI 数据包缓存。</div>`;
}

async function loadAiStatus() {
  try {
    renderAiStatus(await api("/api/ai/status"));
  } catch (error) {
    $("ai-sources").innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
  }
}

async function refreshAiPack() {
  if (state.aiAsking) return;
  $("ai-answer").textContent = "刷新数据包中...";
  try {
    const data = await postJson("/api/ai/refresh", {});
    renderAiStatus(data);
    $("ai-answer").textContent = "AI 数据包已刷新。";
  } catch (error) {
    $("ai-answer").textContent = friendlyError(error.message);
  }
}

function setAiBusy(busy) {
  state.aiAsking = Boolean(busy);
  $("ai-ask-btn").disabled = state.aiAsking;
  $("ai-refresh-btn").disabled = state.aiAsking;
  document.querySelectorAll(".ai-prompt").forEach((button) => {
    button.disabled = state.aiAsking;
  });
}

function fillAiPrompt(button) {
  if (state.aiAsking) return;
  $("ai-question").value = button.dataset.question || "";
  document.querySelectorAll(".ai-prompt").forEach((item) => item.classList.remove("active"));
  button.classList.add("active");
  $("ai-answer").textContent = "已填入快捷问题，确认后点击“提问”。";
  $("ai-question").focus();
}

async function askAi() {
  if (state.aiAsking) return;
  const value = String($("ai-question").value || "").trim();
  if (!value) {
    $("ai-answer").textContent = "请输入问题。";
    return;
  }
  $("ai-question").value = value;
  $("ai-answer").textContent = "分析中...";
  setAiBusy(true);
  try {
    const data = await api("/api/ai/ask", {
      method: "POST",
      body: JSON.stringify({ question: value }),
    });
    $("ai-answer").textContent = stripHtml(data.answer || "");
    await loadAiStatus();
  } catch (error) {
    $("ai-answer").textContent = friendlyError(error.message);
  } finally {
    setAiBusy(false);
  }
}

function renderSystemStatus(data) {
  state.system = data;
  const summary = data.summary || {};
  const issues = summary.issues || [];
  const warnings = summary.warnings || [];
  const dbOk = data.data?.sqlite?.ok !== false;
  const schedules = data.runtime?.schedules || {};
  const overall = issues.length ? "需处理" : (warnings.length ? "有提醒" : "正常");
  const overallClass = issues.length ? "bad" : (warnings.length ? "warn" : "good");
  $("system-status-pill").textContent = overall;
  $("system-status-pill").classList.toggle("good", overallClass === "good");
  $("system-status-pill").classList.toggle("bad", overallClass === "bad");
  $("system-status-pill").classList.toggle("warn", overallClass === "warn");
  $("system-summary").innerHTML = [
    miniCard("整体状态", overall, issues.length ? `需要处理：${issues.join("、")}` : (warnings.length ? `提醒：${warnings.join("、")}` : "核心功能正常"), overallClass),
    miniCard("最近失败", String(summary.recent_failures || 0), "只展示最近几条，失败会出现在下方", summary.recent_failures ? "bad" : "good"),
    miniCard("长期统计", dbOk ? "正常" : "异常", dbOk ? "流量汇总可以继续累积" : "统计数据可能无法保存", dbOk ? "good" : "bad"),
    miniCard("推送计划", `${schedules.enabled || 0}/${schedules.total || 0}`, "已启用 / 全部计划"),
  ].join("");

  const services = data.health_items || data.services || [];
  $("system-services").innerHTML = services.length
    ? services.map((item) => {
      const cls = statusLevelClass(item.level, item.ok);
      const detail = [item.message, item.detail].filter(Boolean).join(" ");
      return `
        <div class="status-row ${cls}">
          <span>
            <strong>${escapeHtml(item.label)}</strong><br>
            <span class="tiny">${escapeHtml(detail || "状态正常。")}</span>
            ${item.fix ? `<br><span class="tiny fix-text">${escapeHtml(item.fix)}</span>` : ""}
          </span>
          <span class="pill ${cls}">${escapeHtml(statusLevelText(item.level, item.ok))}</span>
        </div>`;
    }).join("")
    : `<div class="empty-state">还没有系统状态。</div>`;

  const dataStatus = data.data_status || [];
  $("system-data").innerHTML = dataStatus.length
    ? dataStatus.map((item) => {
      const cls = statusLevelClass(item.level, item.level === "ok");
      return `
        <div class="status-row ${cls}">
          <span>
            <strong>${escapeHtml(item.label)}</strong><br>
            <span class="tiny">${escapeHtml(item.message || item.detail || "状态正常。")}</span>
            ${item.fix ? `<br><span class="tiny fix-text">${escapeHtml(item.fix)}</span>` : ""}
          </span>
          <span class="pill ${cls}">${escapeHtml(statusLevelText(item.level, item.level === "ok"))}</span>
        </div>`;
    }).join("")
    : `<div class="empty-state">数据状态正常。</div>`;
  renderMaintenanceStatus(data.data?.maintenance || {});
}

function renderMaintenanceStatus(status) {
  const ok = status.ok !== false;
  const oldRuns = Number(status.old_task_runs || 0);
  const hasCleanup = ok && oldRuns > 0;
  $("maintenance-status-pill").textContent = ok ? (hasCleanup ? "可清理" : "正常") : "异常";
  $("maintenance-status-pill").classList.toggle("good", ok && !hasCleanup);
  $("maintenance-status-pill").classList.toggle("bad", !ok);
  $("maintenance-status-pill").classList.toggle("warn", hasCleanup);
  $("maintenance-summary").innerHTML = [
    miniCard("维护状态", ok ? (hasCleanup ? "建议清理" : "正常") : "异常", ok ? "不会影响长期流量统计" : (status.error || "请检查容器日志"), ok ? (hasCleanup ? "warn" : "good") : "bad"),
    miniCard("运行记录", hasCleanup ? `${oldRuns} 条偏旧` : "无需处理", hasCleanup ? "可点击下方按钮清理" : "最近记录保持精简"),
    miniCard("保留策略", status.retention_enabled ? `${status.retention_days || 0} 天` : "关闭", status.retention_enabled ? "超过保留天数的任务记录可清理" : "不会自动建议清理"),
    miniCard("长期统计", "保留", "清理任务记录不会删除流量汇总", "good"),
  ].join("");
}

function configFieldsByGroup(fields) {
  const groups = new Map();
  fields.forEach((field) => {
    const group = field.group || "基础";
    if (!groups.has(group)) groups.set(group, []);
    groups.get(group).push(field);
  });
  return groups;
}

function configFieldHtml(field, scope) {
  const key = escapeHtml(field.key);
  const id = `runtime-config-${escapeHtml(scope)}-${key}`;
  if (field.type === "boolean") {
    return `
      <label class="config-field config-toggle" for="${id}">
        <span>
          <strong>${escapeHtml(field.label || field.key)}</strong>
          <small>${escapeHtml(field.note || "")}</small>
        </span>
        <input id="${id}" data-config-key="${key}" type="checkbox" ${field.value ? "checked" : ""}>
      </label>`;
  }
  const type = field.type === "number" ? "number" : "text";
  const attrs = [
    `id="${id}"`,
    `data-config-key="${key}"`,
    `type="${type}"`,
    field.type === "bytes" ? `inputmode="text"` : "",
    field.min !== undefined ? `min="${escapeHtml(field.min)}"` : "",
    field.max !== undefined ? `max="${escapeHtml(field.max)}"` : "",
    `value="${escapeHtml(field.value ?? "")}"`,
  ].filter(Boolean).join(" ");
  return `
    <label class="config-field" for="${id}">
      <span>${escapeHtml(field.label || field.key)}</span>
      <input ${attrs}>
      <small>${escapeHtml(field.note || "")}</small>
    </label>`;
}

function renderConfigEditor(targetId, fields, emptyText) {
  const target = $(targetId);
  if (!fields.length) {
    target.innerHTML = `<div class="empty-state">${escapeHtml(emptyText)}</div>`;
    return;
  }
  const groups = configFieldsByGroup(fields);
  target.innerHTML = Array.from(groups.entries()).map(([group, items]) => `
    <section class="config-group">
      <h4 class="config-group-title">${escapeHtml(group)}</h4>
      <div class="config-group-grid">
        ${items.map((field) => configFieldHtml(field, targetId)).join("")}
      </div>
    </section>`).join("");
}

function configPayloadFrom(rootId) {
  const payload = {};
  const root = $(rootId);
  if (!root) return payload;
  root.querySelectorAll("[data-config-key]").forEach((input) => {
    const key = input.dataset.configKey;
    if (!key) return;
    if (input.type === "checkbox") payload[key] = input.checked;
    else if (input.type === "number") payload[key] = Number(input.value);
    else payload[key] = input.value;
  });
  return payload;
}

function systemConfigFields(config) {
  return (config.editable || []).filter((field) => field.group !== "告警");
}

function alertConfigFields(config) {
  return (config.editable || []).filter((field) => field.group === "告警");
}

function renderSystemConfig(config) {
  renderConfigEditor("editable-config-form", systemConfigFields(config), "暂无可编辑配置。");
  $("config-result").textContent = "这些配置不包含 token、密码或 API Key，保存后立即生效。";
}

function renderAlertConfig(config) {
  renderConfigEditor("alert-config-form", alertConfigFields(config), "暂无可编辑的告警配置。");
  $("alert-config-result").textContent = "阈值留空或填 0 表示关闭；静默时段格式如 23:00-07:00。";
}

async function saveConfigFromForm({ formId, resultId, buttonId, afterSave }) {
  $(resultId).textContent = "保存配置中...";
  $(buttonId).disabled = true;
  try {
    const data = await api("/api/system/config", {
      method: "POST",
      body: JSON.stringify(configPayloadFrom(formId)),
    });
    if (afterSave) await afterSave(data);
    $(resultId).textContent = "配置已保存并立即生效。";
  } catch (error) {
    $(resultId).textContent = friendlyError(error.message);
  } finally {
    $(buttonId).disabled = false;
  }
}

async function saveSystemConfig() {
  await saveConfigFromForm({
    formId: "editable-config-form",
    resultId: "config-result",
    buttonId: "save-config-btn",
    afterSave: async (data) => {
      renderSystemConfig(data.config || {});
      await loadTaskRuns("system-task-runs", $("task-run-filter").value, DISPLAY_LIMITS.taskRuns);
    },
  });
}

async function saveAlertConfig() {
  await saveConfigFromForm({
    formId: "alert-config-form",
    resultId: "alert-config-result",
    buttonId: "save-alert-config-btn",
    afterSave: async (data) => {
      renderAlertConfig(data.config || {});
      await loadAlerts();
    },
  });
}

function setMaintenanceBusy(busy) {
  $("prune-task-runs-btn").disabled = busy;
  $("vacuum-db-btn").disabled = busy;
}

async function pruneTaskRuns() {
  $("maintenance-result").textContent = "清理旧运行记录中...";
  setMaintenanceBusy(true);
  try {
    const data = await postJson("/api/system/maintenance/prune-task-runs", {});
    renderMaintenanceStatus(data.maintenance || {});
    $("maintenance-result").textContent = `已清理 ${data.result?.deleted || 0} 条旧运行记录，当前剩余 ${data.result?.remaining || 0} 条。`;
    await loadTaskRuns("system-task-runs", $("task-run-filter").value, DISPLAY_LIMITS.taskRuns);
  } catch (error) {
    $("maintenance-result").textContent = friendlyError(error.message);
  } finally {
    setMaintenanceBusy(false);
  }
}

async function vacuumDb() {
  $("maintenance-result").textContent = "压缩 SQLite 中...";
  setMaintenanceBusy(true);
  try {
    const data = await postJson("/api/system/maintenance/vacuum", {});
    renderMaintenanceStatus(data.maintenance || {});
    $("maintenance-result").textContent = "数据维护完成，长期统计库状态正常。";
    await loadTaskRuns("system-task-runs", $("task-run-filter").value, DISPLAY_LIMITS.taskRuns);
  } catch (error) {
    $("maintenance-result").textContent = friendlyError(error.message);
  } finally {
    setMaintenanceBusy(false);
  }
}

function renderTrafficRange(data) {
  const nodes = data.nodes || [];
  const groups = data.groups || [];
  const visibleGroups = groups.length > DISPLAY_LIMITS.analyticsGroups ? groups.slice(-DISPLAY_LIMITS.analyticsGroups) : groups;
  const compactNodes = data.compact || nodes.some((node) => node.compact_other)
    ? { rows: nodes, hidden: Number(data.hidden_node_count || 0), hiddenTotal: 0 }
    : compactTrafficRows(nodes, DISPLAY_LIMITS.analyticsNodes);
  const topNodes = compactNodes.rows;
  const maxNode = Math.max(...topNodes.map((node) => Number(node.total || 0)), 1);
  const maxGroup = Math.max(...visibleGroups.map((group) => Number(group.total?.total || 0)), 1);
  const source = data.source ? String(data.source) : "sqlite";
  $("analytics-status-pill").textContent = source;
  $("analytics-status-pill").classList.toggle("good", true);
  $("traffic-range-result").innerHTML = `
    <div class="range-summary">
      ${miniCard("区间合计", data.total?.total_human || "--", `${escapeHtml(data.from)} -> ${escapeHtml(data.to)}`)}
      ${miniCard("下行 / 上行", `${data.total?.down_human || "--"} / ${data.total?.up_human || "--"}`, "区间累计")}
      ${miniCard("节点展示", `${Math.min(data.node_count ?? nodes.length, DISPLAY_LIMITS.analyticsNodes)}/${data.node_count ?? nodes.length}`, compactNodes.hidden ? `其余 ${compactNodes.hidden} 个已汇总` : "全部展示")}
      ${miniCard("趋势分组", `${visibleGroups.length}/${groups.length}`, `${data.day_count || 0} 天 · ${data.group || "daily"}`)}
    </div>
    <div class="analytics-grid">
      <article class="analytics-panel analytics-wide">
        <div class="panel-head compact-head">
          <div>
            <h3>分组趋势</h3>
          </div>
          <span class="soft-label">${escapeHtml(visibleGroups.length === groups.length ? (data.group || "daily") : `最近 ${visibleGroups.length} 组`)}</span>
        </div>
        <div class="analytics-axis">
          <span>0</span>
          <span>${escapeHtml(formatBytes(maxGroup / 2))}</span>
          <span>${escapeHtml(formatBytes(maxGroup))}</span>
        </div>
        <div class="analytics-group-list">
          ${visibleGroups.length ? visibleGroups.map((group) => `
            <div class="analytics-group-row">
              <span class="analytics-label" title="${escapeHtml(group.label)}">${escapeHtml(group.label)}</span>
              <span class="analytics-track">
                <span class="analytics-fill total" style="width:${percentOf(group.total?.total, maxGroup).toFixed(2)}%"></span>
              </span>
              <span class="analytics-value">${escapeHtml(group.total?.total_human || "--")}</span>
            </div>`).join("") : `<div class="empty-state">暂无分组数据。</div>`}
          ${groups.length > visibleGroups.length ? `<div class="compact-note">较早的 ${groups.length - visibleGroups.length} 组已隐藏，调整日期范围可查看。</div>` : ""}
        </div>
      </article>
      <article class="analytics-panel">
        <div class="panel-head compact-head">
          <div>
            <h3>节点贡献</h3>
          </div>
          <span class="soft-label">${compactNodes.hidden ? "Top + 其余" : `Top ${escapeHtml(String(topNodes.length || 0))}`}</span>
        </div>
        <div class="analytics-node-bars">
          ${topNodes.length ? topNodes.map((node, index) => `
            <${node.compact_other ? "div" : "button"} class="analytics-node-row ${node.compact_other ? "compact-other" : ""}" ${node.compact_other ? "" : `data-jump-node="${escapeHtml(node.uuid)}" title="跳到 ${escapeHtml(node.name)}"`}>
              <span class="analytics-rank">${node.compact_other ? "..." : index + 1}</span>
              <span class="analytics-node-main">
                <span class="analytics-node-title">${escapeHtml(node.name)}</span>
                <span class="analytics-track">
                  <span class="analytics-fill down" style="width:${percentOf(node.down, maxNode).toFixed(2)}%"></span>
                  <span class="analytics-fill up" style="left:${percentOf(node.down, maxNode).toFixed(2)}%;width:${percentOf(node.up, maxNode).toFixed(2)}%"></span>
                </span>
                <span class="tiny">下行 ${escapeHtml(node.down_human)} / 上行 ${escapeHtml(node.up_human)}</span>
              </span>
              <span class="analytics-value">${escapeHtml(node.total_human)}</span>
            </${node.compact_other ? "div" : "button"}>`).join("") : `<div class="empty-state">这个区间暂无节点汇总。</div>`}
        </div>
      </article>
    </div>`;
  bindNodeJumpButtons();
}

async function loadTrafficRange() {
  setDefaultRangeDates();
  $("traffic-range-result").innerHTML = `<div class="range-summary">${skelCards(4)}</div><div class="kt-skeleton kt-skel-chart"></div>`;
  $("analytics-status-pill").textContent = "查询中";
  $("analytics-status-pill").classList.remove("good", "bad");
  try {
    const query = trafficRangeQuery();
    renderTrafficRange(await api(`/api/traffic/range?${query.toString()}`));
  } catch (error) {
    $("analytics-status-pill").textContent = "异常";
    $("analytics-status-pill").classList.add("bad");
    $("traffic-range-result").innerHTML = `<div class="empty-state">${escapeHtml(friendlyError(error.message))}</div>`;
  }
}

async function exportTrafficRangeCsv() {
  setDefaultRangeDates();
  const button = $("export-traffic-range-btn");
  button.disabled = true;
  $("analytics-status-pill").textContent = "导出中";
  $("analytics-status-pill").title = "";
  $("analytics-status-pill").classList.remove("good", "bad");
  try {
    const query = trafficRangeQuery();
    const response = await fetch(`/api/traffic/range/export.csv?${query.toString()}`, {
      credentials: "same-origin",
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => null);
      throw new Error(friendlyError(payload?.error?.message || "导出失败"));
    }
    const blob = await response.blob();
    const filename = filenameFromDisposition(
      response.headers.get("content-disposition"),
      `komari-traffic-${$("traffic-range-from").value}-${$("traffic-range-to").value}.csv`,
    );
    downloadBlob(blob, filename);
    $("analytics-status-pill").textContent = "已导出";
    $("analytics-status-pill").title = "";
    $("analytics-status-pill").classList.add("good");
  } catch (error) {
    const message = friendlyError(error.message);
    $("analytics-status-pill").textContent = "导出失败";
    $("analytics-status-pill").title = message;
    $("analytics-status-pill").classList.add("bad");
    console.warn(message);
  } finally {
    button.disabled = false;
  }
}

async function loadAnalyticsPage() {
  setDefaultRangeDates();
  await loadTrafficRange();
}

async function loadSystemPage() {
  const [system, config] = await Promise.all([
    api("/api/system/status"),
    api("/api/system/config"),
  ]);
  renderSystemStatus(system);
  renderSystemConfig(config);
  await loadTaskRuns("system-task-runs", $("task-run-filter").value, DISPLAY_LIMITS.taskRuns);
}

async function loadCurrentRoute(forceOverview = false) {
  updateStatus("加载中", true);
  startRouteProgress();
  showRouteSkeleton(state.route);
  try {
    if (forceOverview || state.route === "/") await loadOverview();
    if (state.route === "/nodes") await loadNodes(state.nodesHours);
    if (state.route === "/alerts") await loadAlerts();
    if (state.route === "/telegram") await loadTelegramStatus();
    if (state.route === "/ai") await loadAiStatus();
    if (state.route === "/analytics") await loadAnalyticsPage();
    if (state.route === "/system") await loadSystemPage();
    updateStatus("已同步", true);
  } catch (error) {
    if (error.status === 401) {
      showLoginView();
      return;
    }
    updateStatus(friendlyError(error.message), false);
  } finally {
    clearRouteSkeleton();
    stopRouteProgress();
  }
}

async function checkSession() {
  const data = await api("/api/auth/session");
  state.authenticated = Boolean(data.authenticated);
  if (state.authenticated) {
    setVisible("login-view", false);
    setVisible("app-view", true);
    await loadCurrentRoute(true);
  } else {
    showLoginView();
  }
}

async function doLogin(event) {
  event.preventDefault();
  unlockLoginFields();
  $("login-error").textContent = "";
  try {
    await api("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({
        username: $("login-username").value,
        password: $("login-password").value,
        remember: loginRememberEnabled(),
      }),
    });
    state.authenticated = true;
    lockAndClearLoginFields();
    setVisible("login-view", false);
    setVisible("app-view", true);
    await loadCurrentRoute(true);
  } catch (error) {
    $("login-error").textContent = friendlyError(error.message);
  }
}

function canUseDesktopSidebar() {
  return window.matchMedia("(min-width: 921px)").matches;
}

function loadSidebarPreference() {
  try {
    state.sidebarCollapsed = window.localStorage.getItem(SIDEBAR_STORAGE_KEY) === "true";
  } catch (_error) {
    state.sidebarCollapsed = false;
  }
}

function saveSidebarPreference() {
  try {
    window.localStorage.setItem(SIDEBAR_STORAGE_KEY, state.sidebarCollapsed ? "true" : "false");
  } catch (_error) {
    // Ignore storage failures in private browsing or restricted WebViews.
  }
}

function applySidebarState() {
  const collapsed = state.sidebarCollapsed && canUseDesktopSidebar();
  $("app-view").classList.toggle("sidebar-collapsed", collapsed);
  const toggle = $("sidebar-toggle");
  toggle.setAttribute("aria-expanded", String(!collapsed));
  toggle.setAttribute("aria-label", collapsed ? "展开导航" : "收起导航");
  toggle.setAttribute("title", collapsed ? "展开导航" : "收起导航");
}

function bindIconFallbacks() {
  document.querySelectorAll(".brand-icon").forEach((img) => {
    img.addEventListener("error", () => {
      img.hidden = true;
      const fallback = img.nextElementSibling;
      if (fallback) fallback.hidden = false;
    });
  });
}

function bindEvents() {
  $("login-form").addEventListener("submit", doLogin);
  $("login-remember").addEventListener("change", handleLoginRememberChange);
  loginFields().forEach((input) => {
    input.addEventListener("pointerdown", unlockLoginFields);
    input.addEventListener("focus", unlockLoginFields);
    input.addEventListener("keydown", unlockLoginFields);
  });
  $("refresh-btn").addEventListener("click", () => loadCurrentRoute(true));
  $("logout-btn").addEventListener("click", async () => {
    await postJson("/api/auth/logout", {}).catch(() => null);
    showLoginView();
  });
  $("sidebar-toggle").addEventListener("click", () => {
    state.sidebarCollapsed = !state.sidebarCollapsed;
    saveSidebarPreference();
    applySidebarState();
  });
  document.querySelectorAll("#theme-switch [data-theme]").forEach((button) => {
    button.addEventListener("click", () => {
      state.themeMode = normalizeThemeMode(button.dataset.theme);
      saveThemePreference();
      applyThemeMode();
    });
  });
  window.addEventListener("resize", applySidebarState);
  window.addEventListener("popstate", () => navigateRoute(`${window.location.pathname}${window.location.search}`, { load: true, scroll: false }));
  document.querySelectorAll(".nav-link[data-route], .brand[data-route]").forEach((link) => {
    link.addEventListener("click", (event) => {
      event.preventDefault();
      navigateRoute(link.dataset.route, { push: true });
    });
  });
  document.querySelectorAll("#range-tabs button").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll("#range-tabs button").forEach((b) => b.classList.remove("active"));
      button.classList.add("active");
      loadNodes(Number(button.dataset.hours));
    });
  });
  $("save-binding-btn").addEventListener("click", () => saveBinding(false));
  $("clear-binding-btn").addEventListener("click", () => saveBinding(true));
  $("close-binding-btn").addEventListener("click", () => $("node-bind-panel").classList.add("hidden"));
  $("check-alerts-btn").addEventListener("click", () => runAlertCheck(false));
  $("notify-alerts-btn").addEventListener("click", () => runAlertCheck(true));
  $("mute-btn").addEventListener("click", () => muteAlerts(Number($("mute-hours").value || 1)));
  document.querySelectorAll(".quick-mute").forEach((button) => {
    button.addEventListener("click", () => muteAlerts(Number(button.dataset.hours || 1)));
  });
  $("unmute-btn").addEventListener("click", async () => {
    $("alert-result").textContent = "解除静默中...";
    try {
      await postJson("/api/alerts/unmute", {});
      $("alert-result").textContent = "已解除告警静默。";
      await loadAlerts();
    } catch (error) {
      $("alert-result").textContent = friendlyError(error.message);
    }
  });
  $("preview-report-btn").addEventListener("click", previewReport);
  $("send-report-btn").addEventListener("click", sendReport);
  $("save-schedule-btn").addEventListener("click", saveSchedule);
  $("reset-schedule-btn").addEventListener("click", resetScheduleForm);
  $("schedule-scope").addEventListener("change", updateScheduleFormVisibility);
  $("tg-test-btn").addEventListener("click", async () => {
    $("telegram-result").textContent = "测试发送中...";
    try {
      const data = await postJson("/api/telegram/test", {});
      $("telegram-result").textContent = `测试消息已发送到 ${data.chat}`;
    } catch (error) {
      $("telegram-result").textContent = friendlyError(error.message);
    }
  });
  $("ai-refresh-btn").addEventListener("click", refreshAiPack);
  $("ai-ask-btn").addEventListener("click", () => askAi());
  document.querySelectorAll(".ai-prompt").forEach((button) => {
    button.addEventListener("click", () => fillAiPrompt(button));
  });
  $("load-traffic-range-btn").addEventListener("click", loadTrafficRange);
  $("export-traffic-range-btn").addEventListener("click", exportTrafficRangeCsv);
  $("traffic-range-group").addEventListener("change", loadTrafficRange);
  $("task-run-filter").addEventListener("change", () => loadTaskRuns("system-task-runs", $("task-run-filter").value, DISPLAY_LIMITS.taskRuns));
  $("save-config-btn").addEventListener("click", saveSystemConfig);
  $("save-alert-config-btn").addEventListener("click", saveAlertConfig);
  $("prune-task-runs-btn").addEventListener("click", pruneTaskRuns);
  $("vacuum-db-btn").addEventListener("click", vacuumDb);
  resetScheduleForm();
  setDefaultRangeDates();
}

function initRoute() {
  const initialRoute = normalizeRoute(window.location.pathname);
  const initialUrl = routeUrl(`${window.location.pathname}${window.location.search}`);
  if (initialUrl !== `${window.location.pathname}${window.location.search}`) {
    window.history.replaceState({ route: initialRoute }, "", initialUrl);
  }
  showRoute(initialRoute);
}

loadThemePreference();
loadSidebarPreference();
bindIconFallbacks();
bindEvents();
initRoute();
applyThemeMode();
applySidebarState();
checkSession().catch((error) => {
  showLoginView(friendlyError(error.message));
});
