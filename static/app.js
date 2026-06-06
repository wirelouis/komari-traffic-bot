const SIDEBAR_STORAGE_KEY = "komari.sidebarCollapsed";

const routeConfig = {
  "/": {
    eyebrow: "Dashboard",
    title: "流量分析工作台",
    subtitle: "查看探针流量、节点排行和服务状态。",
  },
  "/nodes": {
    eyebrow: "Nodes",
    title: "节点流量分析",
    subtitle: "按时间窗口比较节点上下行、合计流量和 Komari 机器绑定。",
  },
  "/alerts": {
    eyebrow: "Alerts",
    title: "告警控制",
    subtitle: "查看告警状态、阈值、静默窗口，并执行检查或推送。",
  },
  "/telegram": {
    eyebrow: "Telegram",
    title: "推送控制",
    subtitle: "预览周期报表、测试 Telegram，并查看计划任务。",
  },
  "/ai": {
    eyebrow: "AI",
    title: "数据问答",
    subtitle: "刷新数据包、使用快捷问题，快速定位流量异常。",
  },
};

const state = {
  authenticated: false,
  overview: null,
  route: "/",
  nodesHours: 24,
  nodes: [],
  machines: [],
  selectedNodeUuid: "",
  bindingSourceId: "",
  sidebarCollapsed: false,
};

const $ = (id) => document.getElementById(id);

function setVisible(id, visible) {
  $(id).classList.toggle("hidden", !visible);
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
  $("topbar-eyebrow").textContent = config.eyebrow;
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
  return `${data.nodes?.length || 0} 个节点`;
}

function totalText(period) {
  if (!period?.ok) return "--";
  return period.data?.total?.total_human || "--";
}

function metric(stat, key) {
  const value = stat?.[key];
  return value === null || value === undefined ? "--" : `${value}%`;
}

function miniCard(label, value, note = "", status = "") {
  return `
    <article class="mini-card">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value || "--")}</strong>
      ${note ? `<small class="tiny">${escapeHtml(note)}</small>` : ""}
      ${status ? `<span class="pill ${escapeHtml(status)}">${escapeHtml(status === "good" ? "正常" : "注意")}</span>` : ""}
    </article>`;
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

function renderOverview(data) {
  state.overview = data;
  const periods = data.periods || {};
  $("metric-today").textContent = totalText(periods.today);
  $("metric-week").textContent = totalText(periods.week);
  $("metric-month").textContent = totalText(periods.month);
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
  const nodes = data.records?.last_24h?.data?.top_nodes || data.records?.last_7d?.data?.top_nodes || [];
  const chart = $("trend-chart");
  if (!nodes.length) {
    const msg = data.records?.last_24h?.error?.message || data.records?.last_7d?.error?.message || "暂无 records 数据";
    chart.innerHTML = `<div class="empty-state">${escapeHtml(friendlyError(msg))}</div>`;
    return;
  }
  const max = Math.max(...nodes.map((n) => Number(n.total || 0)), 1);
  chart.innerHTML = `
    <div class="bar-chart" style="--bar-count: ${nodes.length}">
      ${nodes.map((n) => {
        const pct = Math.max(4, Math.round((Number(n.total || 0) / max) * 100));
        return `
          <button class="bar-item" data-jump-node="${escapeHtml(n.uuid)}" title="${escapeHtml(n.name)} · ${escapeHtml(n.total_human)}">
            <span class="bar-track"><span class="bar-fill" style="height: ${pct}%"></span></span>
            <span class="bar-label">${escapeHtml(n.name)}</span>
          </button>`;
      }).join("")}
    </div>`;
  bindNodeJumpButtons();
}

async function loadOverview() {
  const overview = await api("/api/overview");
  renderOverview(overview);
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
            <div class="machine-cell">
              <span>${escapeHtml(machine?.name || (stale ? "绑定异常" : "未绑定"))}</span>
              <span class="tiny">${escapeHtml(bindingLabel(binding))}${machine?.region ? ` · ${escapeHtml(machine.region)}` : ""}</span>
            </div>
          </td>
          <td>${escapeHtml(n.down_human)}</td>
          <td>${escapeHtml(n.up_human)}</td>
          <td><strong>${escapeHtml(n.total_human)}</strong></td>
          <td>${metric(n.cpu, "avg")}</td>
          <td>${metric(n.ram, "avg")}</td>
          <td>${metric(n.disk, "avg")}</td>
          <td>
            <span class="node-actions">
              <button class="text-btn" data-node-action="open" data-uuid="${escapeHtml(n.uuid)}" ${url ? "" : "disabled"}>打开</button>
              <button class="text-btn" data-node-action="bind" data-uuid="${escapeHtml(n.uuid)}">绑定</button>
              <button class="text-btn" data-node-action="detail" data-uuid="${escapeHtml(n.uuid)}">详情</button>
            </span>
          </td>
        </tr>`;
    }).join("")
    : `<tr><td colspan="9">暂无节点数据</td></tr>`;

  document.querySelectorAll("#nodes-table tr[data-uuid]").forEach((row) => {
    row.addEventListener("click", (event) => {
      if (event.target.closest("button,a,select")) return;
      openKomariNode(row.dataset.uuid);
    });
  });
  document.querySelectorAll("[data-node-action]").forEach((button) => {
    button.addEventListener("click", async (event) => {
      event.stopPropagation();
      const uuid = button.dataset.uuid;
      const action = button.dataset.nodeAction;
      if (action === "open") openKomariNode(uuid);
      if (action === "bind") openBindingPanel(uuid);
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
      $("node-detail").textContent = "点击 Top 排行可跳到节点；点击表格行可打开 Komari 探针机器。";
    }
  } catch (error) {
    $("nodes-table").innerHTML = `<tr><td colspan="9">${escapeHtml(friendlyError(error.message))}</td></tr>`;
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
  $("node-detail").innerHTML = `
    <div class="panel-head">
      <div>
        <p class="eyebrow">Node Detail</p>
        <h3>${escapeHtml(node.name)}</h3>
      </div>
      <span class="pill ${binding.stale ? "bad" : "good"}">${escapeHtml(bindingLabel(binding))}</span>
    </div>
    <div class="detail-grid">
      <div class="detail-item"><span>合计</span><strong>${escapeHtml(node.total_human)}</strong></div>
      <div class="detail-item"><span>下行</span><strong>${escapeHtml(node.down_human)}</strong></div>
      <div class="detail-item"><span>上行</span><strong>${escapeHtml(node.up_human)}</strong></div>
      <div class="detail-item"><span>Komari 机器</span><strong>${escapeHtml(machine?.name || "未绑定")}</strong></div>
      <div class="detail-item"><span>CPU 平均</span><strong>${metric(node.cpu, "avg")}</strong></div>
      <div class="detail-item"><span>RAM 平均</span><strong>${metric(node.ram, "avg")}</strong></div>
      <div class="detail-item"><span>Disk 平均</span><strong>${metric(node.disk, "avg")}</strong></div>
      <div class="detail-item"><span>UUID</span><strong>${escapeHtml(node.uuid)}</strong></div>
    </div>
    <div class="inline-form">
      <button class="primary-btn" id="detail-open-node" ${nodeWebUrl(node) ? "" : "disabled"}>打开 Komari 机器</button>
      <button class="ghost-btn" id="detail-bind-node">绑定机器</button>
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
    renderAlerts(await api("/api/alerts"));
  } catch (error) {
    $("alerts-body").textContent = friendlyError(error.message);
  }
}

async function runAlertCheck(notify) {
  $("alert-result").textContent = "检查中...";
  try {
    const data = await postJson("/api/alerts/check", { notify });
    $("alert-result").textContent = JSON.stringify(data, null, 2);
    updateStatus(notify ? "告警已检查并推送" : "告警已检查", true);
    await loadAlerts();
  } catch (error) {
    $("alert-result").textContent = friendlyError(error.message);
    updateStatus(error.message, false);
  }
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

function renderTelegramStatus(data) {
  $("telegram-status-pill").textContent = data.configured ? "已配置" : "未配置";
  $("telegram-status-pill").classList.toggle("good", Boolean(data.configured));
  $("telegram-status-pill").classList.toggle("bad", !data.configured);
  $("telegram-summary").innerHTML = [
    miniCard("发送状态", data.configured ? "可发送" : "不可发送", data.bot_token_configured ? "Bot Token 已配置" : "缺少 Bot Token", data.configured ? "good" : "bad"),
    miniCard("默认 Chat", data.chat || "未配置"),
    miniCard("告警 Chat", data.alert_chat || "未配置"),
    miniCard("计划任务", `${(data.schedules || []).length} 条`, "来自 crontab"),
  ].join("");
  const schedules = data.schedules || [];
  $("telegram-schedules").innerHTML = schedules.length
    ? schedules.map((item) => `
      <div class="schedule-row">
        <span><strong>${escapeHtml(item.label)}</strong><br><span class="tiny">${escapeHtml(item.command)}</span></span>
        <span class="pill">${escapeHtml(item.schedule)}</span>
      </div>`).join("")
    : `<div class="empty-state">当前 Web 容器未读取到计划任务文件。</div>`;
}

async function loadTelegramStatus() {
  try {
    renderTelegramStatus(await api("/api/telegram/status"));
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
    $("telegram-result").textContent = `已发送到 ${data.chat}`;
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
        <span><strong>${escapeHtml(item.key)}</strong><br><span class="tiny">${item.count || 0} 条记录</span></span>
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
  $("ai-answer").textContent = "刷新数据包中...";
  try {
    const data = await postJson("/api/ai/refresh", {});
    renderAiStatus(data);
    $("ai-answer").textContent = "AI 数据包已刷新。";
  } catch (error) {
    $("ai-answer").textContent = friendlyError(error.message);
  }
}

async function askAi(question) {
  const value = String(question || $("ai-question").value || "").trim();
  if (!value) {
    $("ai-answer").textContent = "请输入问题。";
    return;
  }
  $("ai-question").value = value;
  $("ai-answer").textContent = "分析中...";
  try {
    const data = await api("/api/ai/ask", {
      method: "POST",
      body: JSON.stringify({ question: value }),
    });
    $("ai-answer").textContent = stripHtml(data.answer || "");
    await loadAiStatus();
  } catch (error) {
    $("ai-answer").textContent = friendlyError(error.message);
  }
}

async function loadCurrentRoute(forceOverview = false) {
  updateStatus("加载中", true);
  try {
    if (forceOverview || state.route === "/") await loadOverview();
    if (state.route === "/nodes") await loadNodes(state.nodesHours);
    if (state.route === "/alerts") await loadAlerts();
    if (state.route === "/telegram") await loadTelegramStatus();
    if (state.route === "/ai") await loadAiStatus();
    updateStatus("已同步", true);
  } catch (error) {
    if (error.status === 401) {
      state.authenticated = false;
      setVisible("login-view", true);
      setVisible("app-view", false);
      return;
    }
    updateStatus(friendlyError(error.message), false);
  }
}

async function checkSession() {
  const data = await api("/api/auth/session");
  state.authenticated = Boolean(data.authenticated);
  setVisible("login-view", !state.authenticated);
  setVisible("app-view", state.authenticated);
  if (state.authenticated) await loadCurrentRoute(true);
}

async function doLogin(event) {
  event.preventDefault();
  $("login-error").textContent = "";
  try {
    await api("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({
        username: $("login-username").value,
        password: $("login-password").value,
      }),
    });
    state.authenticated = true;
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
  $("refresh-btn").addEventListener("click", () => loadCurrentRoute(true));
  $("logout-btn").addEventListener("click", async () => {
    await postJson("/api/auth/logout", {}).catch(() => null);
    state.authenticated = false;
    state.overview = null;
    setVisible("login-view", true);
    setVisible("app-view", false);
  });
  $("sidebar-toggle").addEventListener("click", () => {
    state.sidebarCollapsed = !state.sidebarCollapsed;
    saveSidebarPreference();
    applySidebarState();
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
    button.addEventListener("click", () => askAi(button.dataset.question));
  });
}

function initRoute() {
  const initialRoute = normalizeRoute(window.location.pathname);
  const initialUrl = routeUrl(`${window.location.pathname}${window.location.search}`);
  if (initialUrl !== `${window.location.pathname}${window.location.search}`) {
    window.history.replaceState({ route: initialRoute }, "", initialUrl);
  }
  showRoute(initialRoute);
}

loadSidebarPreference();
bindIconFallbacks();
bindEvents();
initRoute();
applySidebarState();
checkSession().catch((error) => {
  state.authenticated = false;
  setVisible("login-view", true);
  setVisible("app-view", false);
  $("login-error").textContent = friendlyError(error.message);
});
