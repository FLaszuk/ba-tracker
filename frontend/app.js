// app.js — BA-Tracker Frontend Logic
// Odpowiada za: pobieranie danych z API, renderowanie wykresów, tabeli, motyw

const API = "https://ba-tracker-api-ruy0.onrender.com/api";

// ══════════════════════════════════════════════════════════
// STAN APLIKACJI
// ══════════════════════════════════════════════════════════
const state = {
    activeTab: "aircraft",
    selectedMonth: null,
    charts: {},
    tableData: [],
};

// ══════════════════════════════════════════════════════════
// PALETA KOLORÓW  (Chart.js)
// ══════════════════════════════════════════════════════════
const PALETTE = [
    "#3b82f6", "#10b981", "#8b5cf6", "#f59e0b",
    "#ec4899", "#14b8a6", "#f97316", "#a3e635",
];

// ══════════════════════════════════════════════════════════
// API HELPERS
// ══════════════════════════════════════════════════════════
async function api(path) {
    try {
        const res = await fetch(API + path);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return await res.json();
    } catch (err) {
        console.error("API error:", path, err);
        showError(`API error: ${err.message}. Run: python run_server.py`);
        return null;
    }
}

function showError(msg) {
    const banner = document.getElementById("error-banner");
    document.getElementById("error-msg").textContent = msg;
    banner.classList.remove("hidden");
    setTimeout(() => banner.classList.add("hidden"), 8000);
}

// ══════════════════════════════════════════════════════════
// INIT
// ══════════════════════════════════════════════════════════
document.addEventListener("DOMContentLoaded", async () => {
    loadTheme();
    await loadMonths();
    await loadData();
});

// ══════════════════════════════════════════════════════════
// MONTHS SELECTOR
// ══════════════════════════════════════════════════════════
async function loadMonths() {
    const data = await api("/months");
    if (!data) return;

    const sel = document.getElementById("month-selector");
    sel.innerHTML = "";
    const months = data.months.reverse(); // newest first

    months.forEach((m, i) => {
        const opt = document.createElement("option");
        opt.value = m;
        opt.textContent = formatMonth(m);
        if (i === 0) opt.selected = true;
        sel.appendChild(opt);
    });

    state.selectedMonth = months[0];
}

function formatMonth(m) {
    const [y, mo] = m.split("-");
    const names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    return `${names[parseInt(mo)]} ${y}`;
}

// ══════════════════════════════════════════════════════════
// TAB SWITCHING
// ══════════════════════════════════════════════════════════
function switchTab(tab) {
    state.activeTab = tab;

    ["aircraft", "engines", "report"].forEach(t => {
        document.getElementById(`view-${t}`).classList.toggle("hidden", t !== tab);
        document.getElementById(`tab-${t}`).classList.toggle("active", t === tab);
    });

    loadData();
}

// ══════════════════════════════════════════════════════════
// LOAD ALL DATA FOR CURRENT TAB + MONTH
// ══════════════════════════════════════════════════════════
async function loadData() {
    const sel = document.getElementById("month-selector");
    const month = sel?.value;
    if (!month) return;
    state.selectedMonth = month;

    await loadKPI(month);

    if (state.activeTab === "aircraft") {
        await Promise.all([
            loadAircraftPie(month),
            loadTopModels(month),
            loadAircraftTrends(),
            loadAircraftTable(month),
        ]);
    } else if (state.activeTab === "engines") {
        await Promise.all([
            loadEnginePie(month),
            loadTopEngines(month),
            loadEngineTrends(),
            loadEngineTable(month),
        ]);
    } else if (state.activeTab === "report") {
        await loadReport(month);
    }
}

// ══════════════════════════════════════════════════════════
// KPI
// ══════════════════════════════════════════════════════════
async function loadKPI(month) {
    const data = await api(`/kpi/${month}`);
    if (!data) return;

    document.getElementById("kpi-hours-val").textContent =
        Number(data.total_flight_hours).toLocaleString();
    document.getElementById("kpi-landings-val").textContent =
        Number(data.total_landings).toLocaleString();
    document.getElementById("kpi-aircraft-val").textContent =
        data.unique_aircraft || "—";

    if (state.activeTab === "engines") {
        document.getElementById("kpi-manufacturers-val").textContent =
            data.unique_engine_manufacturers;
        document.getElementById("kpi-mfr-label").textContent = "Engine Manufacturers";
    } else {
        document.getElementById("kpi-manufacturers-val").textContent =
            data.unique_aircraft_manufacturers;
        document.getElementById("kpi-mfr-label").textContent = "Aircraft Manufacturers";
    }
}

// ══════════════════════════════════════════════════════════
// CHART HELPERS
// ══════════════════════════════════════════════════════════
function destroyChart(id) {
    if (state.charts[id]) {
        state.charts[id].destroy();
        delete state.charts[id];
    }
}

function getChartDefaults() {
    const dark = document.documentElement.getAttribute("data-theme") === "dark";
    return {
        gridColor: dark ? "rgba(255,255,255,0.06)" : "rgba(0,0,0,0.06)",
        legendColor: dark ? "#94a3b8" : "#64748b",
        labelColor: dark ? "#f1f5f9" : "#1e293b",
    };
}

// ══════════════════════════════════════════════════════════
// AIRCRAFT PIE CHART
// ══════════════════════════════════════════════════════════
async function loadAircraftPie(month) {
    const data = await api(`/aircraft-market-share/${month}`);
    if (!data) return;

    destroyChart("aircraft-pie");
    const { labelColor, legendColor } = getChartDefaults();
    const ctx = document.getElementById("chart-aircraft-pie").getContext("2d");

    const labels = data.data.map(d => d.aircraft_manufacturer);
    const values = data.data.map(d => d.total_hours);

    state.charts["aircraft-pie"] = new Chart(ctx, {
        type: "doughnut",
        data: {
            labels,
            datasets: [{
                data: values, backgroundColor: PALETTE, borderWidth: 2,
                borderColor: "transparent", hoverOffset: 8
            }],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            cutout: "62%",
            plugins: {
                legend: {
                    position: "bottom", labels: {
                        color: legendColor, padding: 16,
                        font: { size: 12 }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: ctx => ` ${ctx.label}: ${ctx.parsed.toLocaleString()}h (${data.data[ctx.dataIndex].market_share_pct}%)`,
                    },
                },
            },
        },
    });

    // Update ranking
    renderRanking("aircraft-ranking", data.data, "total_hours", "h", "aircraft_manufacturer");
}

// ══════════════════════════════════════════════════════════
// ENGINE PIE CHART
// ══════════════════════════════════════════════════════════
async function loadEnginePie(month) {
    const data = await api(`/engine-market-share/${month}`);
    if (!data) return;

    destroyChart("engine-pie");
    const { legendColor } = getChartDefaults();
    const ctx = document.getElementById("chart-engine-pie").getContext("2d");

    const labels = data.data.map(d => d.engine_manufacturer);
    const values = data.data.map(d => d.total_hours);

    state.charts["engine-pie"] = new Chart(ctx, {
        type: "doughnut",
        data: {
            labels,
            datasets: [{
                data: values, backgroundColor: PALETTE, borderWidth: 2,
                borderColor: "transparent", hoverOffset: 8
            }],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            cutout: "62%",
            plugins: {
                legend: {
                    position: "bottom", labels: {
                        color: legendColor, padding: 16,
                        font: { size: 12 }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: ctx => ` ${ctx.label}: ${ctx.parsed.toLocaleString()}h (${data.data[ctx.dataIndex].market_share_pct}%)`,
                    },
                },
            },
        },
    });

    renderRanking("engine-ranking", data.data, "total_hours", "h", "engine_manufacturer");
}

// ══════════════════════════════════════════════════════════
// RANKING LIST
// ══════════════════════════════════════════════════════════
function renderRanking(listId, data, valueKey, unit, nameKey) {
    const list = document.getElementById(listId);
    list.innerHTML = "";
    const max = data[0]?.[valueKey] || 1;

    data.slice(0, 6).forEach((item, i) => {
        const rankClass = i === 0 ? "gold" : i === 1 ? "silver" : i === 2 ? "bronze" : "";
        const pct = Math.round((item[valueKey] / max) * 100);
        const li = document.createElement("li");
        li.innerHTML = `
      <span class="rank-num ${rankClass}">${i + 1}</span>
      <span class="rank-name">${item[nameKey]}</span>
      <div class="rank-bar-wrap">
        <div class="rank-bar" style="width:${pct}%; background:${PALETTE[i]}"></div>
      </div>
      <span class="rank-val">${Number(item[valueKey]).toLocaleString()}${unit}</span>
    `;
        list.appendChild(li);
    });
}

// ══════════════════════════════════════════════════════════
// TOP MODELS BAR CHART
// ══════════════════════════════════════════════════════════
async function loadTopModels(month) {
    const data = await api(`/top-models/${month}?limit=10`);
    if (!data) return;

    destroyChart("top-models");
    const { gridColor, labelColor } = getChartDefaults();
    const ctx = document.getElementById("chart-top-models").getContext("2d");

    state.charts["top-models"] = new Chart(ctx, {
        type: "bar",
        data: {
            labels: data.data.map(d => d.aircraft_model),
            datasets: [{
                label: "Landings",
                data: data.data.map(d => d.total_landings),
                backgroundColor: PALETTE[0] + "cc",
                borderColor: PALETTE[0],
                borderWidth: 1,
                borderRadius: 4,
            }],
        },
        options: {
            indexAxis: "y",
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: gridColor }, ticks: { color: labelColor } },
                y: { grid: { display: false }, ticks: { color: labelColor, font: { size: 11 } } },
            },
        },
    });
}

// ══════════════════════════════════════════════════════════
// TOP ENGINES BAR CHART
// ══════════════════════════════════════════════════════════
async function loadTopEngines(month) {
    const data = await api(`/top-engines/${month}?limit=10`);
    if (!data) return;

    destroyChart("top-engines");
    const { gridColor, labelColor } = getChartDefaults();
    const ctx = document.getElementById("chart-top-engines").getContext("2d");

    state.charts["top-engines"] = new Chart(ctx, {
        type: "bar",
        data: {
            labels: data.data.map(d => d.engine_model),
            datasets: [{
                label: "Flight Hours",
                data: data.data.map(d => d.total_hours),
                backgroundColor: PALETTE[2] + "cc",
                borderColor: PALETTE[2],
                borderWidth: 1,
                borderRadius: 4,
            }],
        },
        options: {
            indexAxis: "y",
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: gridColor }, ticks: { color: labelColor } },
                y: { grid: { display: false }, ticks: { color: labelColor, font: { size: 11 } } },
            },
        },
    });
}

// ══════════════════════════════════════════════════════════
// TREND CHARTS (LINE)
// ══════════════════════════════════════════════════════════
async function loadAircraftTrends() {
    const data = await api("/trends/aircraft");
    if (!data) return;
    renderTrendChart("chart-aircraft-trend", data, "aircraft");
}

async function loadEngineTrends() {
    const data = await api("/trends/engines");
    if (!data) return;
    renderTrendChart("chart-engine-trend", data, "engines");
}

function renderTrendChart(canvasId, data, type) {
    const key = canvasId;
    destroyChart(key);
    const { gridColor, labelColor } = getChartDefaults();
    const ctx = document.getElementById(canvasId).getContext("2d");

    const months = data.months;
    const mfrs = Object.keys(data.manufacturers);

    const datasets = mfrs.slice(0, 5).map((mfr, i) => ({
        label: mfr,
        data: months.map(m => data.manufacturers[mfr]?.[m]?.hours ?? 0),
        borderColor: PALETTE[i],
        backgroundColor: PALETTE[i] + "22",
        borderWidth: 2,
        pointRadius: 3,
        tension: 0.35,
        fill: false,
    }));

    state.charts[key] = new Chart(ctx, {
        type: "line",
        data: { labels: months.map(m => formatMonth(m)), datasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            interaction: { mode: "index" },
            plugins: {
                legend: { labels: { color: labelColor, font: { size: 11 } } },
            },
            scales: {
                x: { grid: { color: gridColor }, ticks: { color: labelColor, maxRotation: 45 } },
                y: { grid: { color: gridColor }, ticks: { color: labelColor } },
            },
        },
    });
}

// ══════════════════════════════════════════════════════════
// DATA TABLES
// ══════════════════════════════════════════════════════════
async function loadAircraftTable(month) {
    const data = await api(`/table/${month}?limit=100`);
    if (!data) return;
    state.tableData = data.data;
    renderAircraftTable(data.data);
}

function renderAircraftTable(rows) {
    const tbody = document.getElementById("aircraft-table-body");
    tbody.innerHTML = "";
    rows.forEach(r => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
      <td>${r.aircraft_manufacturer}</td>
      <td>${r.aircraft_model}</td>
      <td style="color:var(--text-muted);font-size:0.82rem">${r.engine_manufacturer}</td>
      <td>${Number(r.total_hours).toLocaleString()}h</td>
      <td>${Number(r.total_landings).toLocaleString()}</td>
      <td><span class="share-badge">${r.aircraft_market_share}%</span></td>
    `;
        tbody.appendChild(tr);
    });
}

function filterTable() {
    const q = document.getElementById("filter-acft-mfr").value.toLowerCase();
    const filtered = state.tableData.filter(r =>
        r.aircraft_manufacturer.toLowerCase().includes(q) ||
        r.aircraft_model.toLowerCase().includes(q)
    );
    renderAircraftTable(filtered);
}

async function loadEngineTable(month) {
    const data = await api(`/table/${month}?limit=100`);
    if (!data) return;
    const tbody = document.getElementById("engine-table-body");
    tbody.innerHTML = "";
    data.data.forEach(r => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
      <td>${r.engine_manufacturer}</td>
      <td style="color:var(--text-muted);font-size:0.82rem">${r.engine_model}</td>
      <td style="color:var(--text-muted);font-size:0.82rem">${r.aircraft_model}</td>
      <td>${Number(r.total_hours).toLocaleString()}h</td>
      <td>${Number(r.total_landings).toLocaleString()}</td>
      <td><span class="share-badge">${r.engine_market_share}%</span></td>
    `;
        tbody.appendChild(tr);
    });
}

// ══════════════════════════════════════════════════════════
// MONTHLY REPORT
// ══════════════════════════════════════════════════════════
async function loadReport(month) {
    const [reportData, acData, engData] = await Promise.all([
        api(`/report/${month}`),
        api(`/aircraft-market-share/${month}`),
        api(`/engine-market-share/${month}`),
    ]);

    if (reportData?.report) {
        const r = reportData.report;
        setKpiChange("rep-hours-curr", "rep-hours-chg",
            r.flight_hours?.current, r.flight_hours?.change_pct, "h");
        setKpiChange("rep-land-curr", "rep-land-chg",
            r.landings?.current, r.landings?.change_pct, "");
        setKpiChange("rep-acft-curr", "rep-acft-chg",
            r.aircraft?.current, r.aircraft?.change_pct, "");

        document.getElementById("report-footer").textContent =
            `Comparing ${formatMonth(month)} vs ${formatMonth(r.previous_month)} — Generated ${new Date().toLocaleDateString()}`;
    }

    if (acData) {
        destroyChart("rep-aircraft-pie");
        const { legendColor } = getChartDefaults();
        const ctx = document.getElementById("chart-rep-aircraft-pie").getContext("2d");
        state.charts["rep-aircraft-pie"] = new Chart(ctx, {
            type: "doughnut",
            data: {
                labels: acData.data.map(d => d.aircraft_manufacturer),
                datasets: [{
                    data: acData.data.map(d => d.total_hours),
                    backgroundColor: PALETTE, borderWidth: 2, borderColor: "transparent"
                }],
            },
            options: {
                responsive: true, maintainAspectRatio: false, cutout: "60%",
                plugins: { legend: { position: "bottom", labels: { color: legendColor, font: { size: 11 } } } }
            },
        });
    }

    if (engData) {
        destroyChart("rep-engine-pie");
        const { legendColor } = getChartDefaults();
        const ctx = document.getElementById("chart-rep-engine-pie").getContext("2d");
        state.charts["rep-engine-pie"] = new Chart(ctx, {
            type: "doughnut",
            data: {
                labels: engData.data.map(d => d.engine_manufacturer),
                datasets: [{
                    data: engData.data.map(d => d.total_hours),
                    backgroundColor: PALETTE, borderWidth: 2, borderColor: "transparent"
                }],
            },
            options: {
                responsive: true, maintainAspectRatio: false, cutout: "60%",
                plugins: { legend: { position: "bottom", labels: { color: legendColor, font: { size: 11 } } } }
            },
        });
    }
}

function setKpiChange(valId, chgId, value, pct, unit) {
    document.getElementById(valId).textContent =
        value != null ? Number(value).toLocaleString() + unit : "—";
    const el = document.getElementById(chgId);
    if (pct != null) {
        const sign = pct >= 0 ? "+" : "";
        el.textContent = `${sign}${pct}% vs prev. month`;
        el.className = "kpi-change " + (pct >= 0 ? "positive" : "negative");
    } else {
        el.textContent = "No comparison data";
        el.className = "kpi-change";
    }
}

// ══════════════════════════════════════════════════════════
// CSV EXPORT
// ══════════════════════════════════════════════════════════
function exportCSV() {
    const month = state.selectedMonth;
    if (!month) return;
    window.open(`${API}/export/${month}`, "_blank");
}

// ══════════════════════════════════════════════════════════
// DARK / LIGHT THEME
// ══════════════════════════════════════════════════════════
function loadTheme() {
    const saved = localStorage.getItem("ba-tracker-theme") || "dark";
    document.documentElement.setAttribute("data-theme", saved);
    document.getElementById("theme-btn").textContent = saved === "dark" ? "🌙" : "☀️";
}

function toggleTheme() {
    const current = document.documentElement.getAttribute("data-theme");
    const next = current === "dark" ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", next);
    localStorage.setItem("ba-tracker-theme", next);
    document.getElementById("theme-btn").textContent = next === "dark" ? "🌙" : "☀️";

    // Rebuild all charts with new grid colors
    loadData();
}
