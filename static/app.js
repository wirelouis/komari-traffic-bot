const state = {
  overview: null,
  nodesHours: 24,
  nodes: [],
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

async function api(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const data = await response.json().catch(() => ({ ok: false, error: { message: "响应无法解析" } }));
  if (!response.ok || data.ok === false) {
    const error = new Error(data.error?.message || "请求失败");
    error.payload = data;
    error.status = response.status;
    throw error;
  }
  return data.data;
}

function noteText(period) {
  if (!period?.ok) return period?.error?.message || "不可用";
  const data = period.data;
  if (data.note === "baseline_missing") return "基线缺失";
  return `${data.nodes?.length || 0} 个节点`;
}

function totalText(period) {
  if (!period?.ok) return "--";
  return period.data?.total?.total_human || "--";
}

function updateStatus(message, good = true) {
  const pill = $("status-pill");
  pill.textContent = message;
  pill.classList.toggle("good", good);
  pill.classList.toggle("bad", !good);
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
  $("ai-status").textContent = data.services?.ai?.configured ? "已配置" : "未配置";

  const topNodes = periods.today?.data?.top_nodes || periods.today?.data?.nodes?.slice(0, 5) || [];
  $("top-list").innerHTML = topNodes.length
    ? topNodes.map((n) => `<li><span class="node-name">${escapeHtml(n.name)}</span><br><span class="tiny">${escapeHtml(n.total_human)} · 下行 ${escapeHtml(n.down_human)} / 上行 ${escapeHtml(n.up_human)}</span></li>`).join("")
    : `<li class="muted">暂无 Top 数据</li>`;

  renderChart(data);
}

function renderChart(data) {
  const nodes = data.records?.last_24h?.data?.top_nodes || data.records?.last_7d?.data?.top_nodes || [];
  const chart = $("trend-chart");
  if (!nodes.length) {
    const msg = data.records?.last_24h?.error?.message || data.records?.last_7d?.error?.message || "暂无 records 数据";
    chart.innerHTML = `<div class="detail-strip">${escapeHtml(msg)}</div>`;
    return;
  }
  const width = 680;
  const height = 220;
  const max = Math.max(...nodes.map((n) => Number(n.total || 0)), 1);
  const barGap = 12;
  const barWidth = Math.max(26, (width - 80 - barGap * nodes.length) / nodes.length);
  const bars = nodes.map((n, i) => {
    const h = Math.max(8, Math.round((Number(n.total || 0) / max) * 136));
    const x = 48 + i * (barWidth + barGap);
    const y = 170 - h;
    return `
      <rect x="${x}" y="${y}" width="${barWidth}" height="${h}" rx="4" fill="#9a5a35"></rect>
      <text x="${x + barWidth / 2}" y="190" text-anchor="middle" font-size="11" fill="#746f68">${escapeHtml(String(n.name || "").slice(0, 8))}</text>
      <title>${escapeHtml(n.name)} ${escapeHtml(n.total_human)}</title>
    `;
  }).join("");
  chart.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Top traffic chart">
      <line x1="36" y1="170" x2="${width - 24}" y2="170" stroke="#ded7ca"></line>
      <line x1="36" y1="124" x2="${width - 24}" y2="124" stroke="#eee5d8"></line>
      <line x1="36" y1="78" x2="${width - 24}" y2="78" stroke="#eee5d8"></line>
      <text x="36" y="28" font-size="12" fill="#746f68">Top 流量节点</text>
      ${bars}
    </svg>`;
}

function metric(stat, key) {
  const value = stat?.[key]?.avg;
  return value === null || value === undefined ? "--" : `${value}%`;
}

async function loadNodes(hours = state.nodesHours) {
  state.nodesHours = hours;
  $("range-label").textContent = `${hours}h`;
  try {
    const data = await api(`/api/nodes?hours=${hours}`);
    state.nodes = data.nodes || [];
    $("nodes-table").innerHTML = state.nodes.length
      ? state.nodes.map((n) => `
        <tr data-uuid="${escapeHtml(n.uuid)}">
          <td><span class="node-name">${escapeHtml(n.name)}</span><div class="tiny">${escapeHtml(n.uuid)}</div></td>
          <td>${escapeHtml(n.down_human)}</td>
          <td>${escapeHtml(n.up_human)}</td>
          <td><strong>${escapeHtml(n.total_human)}</strong></td>
          <td>${metric(n.cpu, "avg")}</td>
          <td>${metric(n.ram, "avg")}</td>
          <td>${metric(n.disk, "avg")}</td>
        </tr>`).join("")
      : `<tr><td colspan="7">暂无节点数据</td></tr>`;
    document.querySelectorAll("#nodes-table tr[data-uuid]").forEach((row) => {
      row.addEventListener("click", () => loadNodeDetail(row.dataset.uuid));
    });
  } catch (error) {
    $("nodes-table").innerHTML = `<tr><td colspan="7">${escapeHtml(error.message)}</td></tr>`;
  }
}

async function loadNodeDetail(uuid) {
  $("node-detail").textContent = "加载节点详情...";
  try {
    const data = await api(`/api/nodes/${encodeURIComponent(uuid)}?hours=${state.nodesHours}`);
    const n = data.node;
    $("node-detail").textContent = `${n.name}\n合计 ${n.total_human}，下行 ${n.down_human}，上行 ${n.up_human}\nCPU 平均 ${metric(n.cpu, "avg")}，RAM 平均 ${metric(n.ram, "avg")}，Disk 平均 ${metric(n.disk, "avg")}`;
  } catch (error) {
    $("node-detail").textContent = error.message;
  }
}

function renderAlerts(data) {
  $("alert-count").textContent = `${data.active_count || 0} active`;
  const thresholds = data.thresholds || {};
  const active = data.active || [];
  $("alerts-body").innerHTML = `
    <div>启用：<strong>${data.enabled ? "是" : "否"}</strong></div>
    <div>告警 chat：${escapeHtml(data.alert_chat || "未配置")}</div>
    <div>静默：${escapeHtml(data.muted_until || (data.in_silence_window ? "当前静默时段" : "否"))}</div>
    <div>阈值：${escapeHtml(Object.values(thresholds).filter(Boolean).join(" / ") || "未配置流量阈值")}</div>
    <div>${active.length ? active.map((a) => `<p>${escapeHtml(a.title)}</p>`).join("") : "当前无 active 告警。"}</div>
  `;
}

async function loadAlerts() {
  try {
    renderAlerts(await api("/api/alerts"));
  } catch (error) {
    $("alerts-body").textContent = error.message;
  }
}

async function loadAll() {
  updateStatus("加载中", true);
  try {
    const overview = await api("/api/overview");
    renderOverview(overview);
    await Promise.all([loadNodes(state.nodesHours), loadAlerts()]);
    updateStatus("已同步", true);
  } catch (error) {
    if (error.status === 401) {
      setVisible("login-view", true);
      setVisible("app-view", false);
      return;
    }
    updateStatus(error.message, false);
  }
}

async function checkSession() {
  const data = await api("/api/auth/session");
  setVisible("login-view", !data.authenticated);
  setVisible("app-view", data.authenticated);
  if (data.authenticated) await loadAll();
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
    setVisible("login-view", false);
    setVisible("app-view", true);
    await loadAll();
  } catch (error) {
    $("login-error").textContent = error.message;
  }
}

async function postAction(path, body, resultEl) {
  const el = resultEl ? $(resultEl) : null;
  if (el) el.textContent = "请求中...";
  try {
    const data = await api(path, { method: "POST", body: JSON.stringify(body || {}) });
    if (el) el.textContent = "完成";
    await loadAlerts();
    return data;
  } catch (error) {
    if (el) el.textContent = error.message;
    throw error;
  }
}

function bindEvents() {
  $("login-form").addEventListener("submit", doLogin);
  $("refresh-btn").addEventListener("click", loadAll);
  $("logout-btn").addEventListener("click", async () => {
    await api("/api/auth/logout", { method: "POST", body: "{}" }).catch(() => null);
    setVisible("login-view", true);
    setVisible("app-view", false);
  });
  document.querySelectorAll("#range-tabs button").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll("#range-tabs button").forEach((b) => b.classList.remove("active"));
      button.classList.add("active");
      loadNodes(Number(button.dataset.hours));
    });
  });
  $("check-alerts-btn").addEventListener("click", async () => {
    await postAction("/api/alerts/check", { notify: false });
    updateStatus("告警已检查", true);
  });
  $("mute-btn").addEventListener("click", () => postAction("/api/alerts/mute", { hours: Number($("mute-hours").value || 1) }));
  $("unmute-btn").addEventListener("click", () => postAction("/api/alerts/unmute", {}));
  $("tg-test-btn").addEventListener("click", () => postAction("/api/telegram/test", {}, "telegram-result"));
  $("send-report-btn").addEventListener("click", async () => {
    const data = await postAction("/api/telegram/report", {
      scope: $("report-scope").value,
      mode: $("report-mode").value,
    }, "telegram-result");
    $("telegram-result").textContent = `已发送到 ${data.chat}`;
  });
  $("ai-ask-btn").addEventListener("click", async () => {
    $("ai-answer").textContent = "分析中...";
    try {
      const data = await api("/api/ai/ask", {
        method: "POST",
        body: JSON.stringify({ question: $("ai-question").value }),
      });
      $("ai-answer").textContent = stripHtml(data.answer || "");
    } catch (error) {
      $("ai-answer").textContent = error.message;
    }
  });
  document.querySelectorAll(".nav-link").forEach((link) => {
    link.addEventListener("click", () => {
      document.querySelectorAll(".nav-link").forEach((item) => item.classList.remove("active"));
      link.classList.add("active");
    });
  });
}

bindEvents();
checkSession().catch((error) => {
  setVisible("login-view", true);
  setVisible("app-view", false);
  $("login-error").textContent = error.message;
});
