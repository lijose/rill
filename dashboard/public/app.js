// Application State
let ws = null;
let activeConnections = []; // Array of { id, address, token, status: 'connecting'|'connected'|'error'|'disconnected', lastError: null, metricsHistory: [], lastMetrics }
let activeConnectionId = null; // ID of the currently selected tab connection
let nextConnectionId = 1;
let resourceChart = null;
// Removed business charts as they are replaced by scheduled query views
let currentView = 'metrics'; // 'metrics' | 'alert-settings' | 'alert-logs'

// Alert Settings & Rules State (Persisted via localStorage)
let alertSettings = JSON.parse(localStorage.getItem('rill_alert_settings') || 'null') || {
    soundEnabled: true,
    soundDuration: 3,
    soundTone: 'beep',
    webhookEnabled: false,
    webhookUrl: '',
    webhookFormat: 'slack'
};

let alertRules = JSON.parse(localStorage.getItem('rill_alert_rules') || 'null') || [
    { id: 'default-cpu', enabled: true, metricField: 'cpu', operator: '>', threshold: 85, cooldownSeconds: 15, lastTriggered: 0 },
    { id: 'default-mem', enabled: true, metricField: 'memPct', operator: '>', threshold: 90, cooldownSeconds: 15, lastTriggered: 0 }
];

let alertLogs = JSON.parse(localStorage.getItem('rill_alert_logs') || '[]');
let alertLogFilter = 'all'; // 'all' | 'error' | 'warning' | 'user' | 'engine' | 'rule'
let audioCtx = null;
let knownBusinessKeys = new Set();

// DOM Elements
const connectForm = document.getElementById('connect-form');
const quackAddressInput = document.getElementById('quack-address');
const quackTokenInput = document.getElementById('quack-token');
const connectBtn = document.getElementById('connect-btn');
const connectError = document.getElementById('connect-error');
const connectionStatusPill = document.getElementById('connection-status-pill');

const connectionPanel = document.getElementById('connection-panel');
const dashboardContent = document.getElementById('dashboard-content');
const currentConnectedAddr = document.getElementById('current-connected-addr');
const disconnectBtn = document.getElementById('disconnect-btn');

// Tabs DOM
const tabsContainer = document.getElementById('tabs-container');
const tabsBar = document.getElementById('tabs-bar');
const tabAddBtn = document.getElementById('tab-add-btn');

// Navigation & Views DOM
const tabBtnMetrics = document.getElementById('tab-btn-metrics');
const tabBtnQueries = document.getElementById('tab-btn-queries');
const tabBtnAlertSettings = document.getElementById('tab-btn-alert-settings');
const tabBtnAlertLogs = document.getElementById('tab-btn-alert-logs');
const viewMetrics = document.getElementById('view-metrics');
const viewQueries = document.getElementById('view-queries');
const viewAlertSettings = document.getElementById('view-alert-settings');
const viewAlertLogs = document.getElementById('view-alert-logs');
const alertBadgeCount = document.getElementById('alert-badge-count');
const alertToastContainer = document.getElementById('alert-toast-container');

// System Metrics DOM
const valCpu = document.getElementById('val-cpu');
const barCpu = document.getElementById('bar-cpu');
const valMem = document.getElementById('val-mem');
const subMemPct = document.getElementById('sub-mem-pct');
const barMem = document.getElementById('bar-mem');
const valPyarrow = document.getElementById('val-pyarrow');
const valPyarrowPeak = document.getElementById('val-pyarrow-peak');
const valRps = document.getElementById('val-rps');
const valTotalRecords = document.getElementById('val-total-records');
const valLatency = document.getElementById('val-latency');
const valAvgLatency = document.getElementById('val-avg-latency');
const valDuckdbLive = document.getElementById('val-duckdb-live');
const valDuckdbMetrics = document.getElementById('val-duckdb-metrics');

// Custom Queries DOM
const sqlQueryEditor = document.getElementById('sql-query-editor');
const runQueryBtn = document.getElementById('run-query-btn');
const showTablesBtn = document.getElementById('show-tables-btn');
const scheduleInterval = document.getElementById('schedule-interval');
const scheduleQueryBtn = document.getElementById('schedule-query-btn');
const queryErrorMsg = document.getElementById('query-error-msg');
const queryElapsedTag = document.getElementById('query-elapsed-tag');
const queryResultsThead = document.getElementById('query-results-thead');
const queryResultsTbody = document.getElementById('query-results-tbody');
const scheduledQueriesTbody = document.getElementById('scheduled-queries-tbody');

// Custom Query Chart DOM
const columnMappingPanel = document.getElementById('column-mapping-panel');
const queryChartType = document.getElementById('query-chart-type');
const queryXCol = document.getElementById('query-x-col');
const queryYCol = document.getElementById('query-y-col');
const queryColorCol = document.getElementById('query-color-col');
const queryRenderChartBtn = document.getElementById('query-render-chart-btn');
const queryChartCard = document.getElementById('query-chart-card');
let customQueryChart = null;
let lastQueryResult = null;

// Business DOM & Controls
// Removed businessCardsGrid
const historyFilter = document.getElementById('history-filter');

// Alert Settings & Rules DOM
const alertSoundEnableToggle = document.getElementById('alert-sound-enable-toggle');
const alertDurationSlider = document.getElementById('alert-duration-slider');
const alertDurationInput = document.getElementById('alert-duration-input');
const alertToneSelect = document.getElementById('alert-tone-select');
const testSoundBtn = document.getElementById('test-sound-btn');

const webhookEnableToggle = document.getElementById('webhook-enable-toggle');
const webhookUrlInput = document.getElementById('webhook-url-input');
const webhookFormatSelect = document.getElementById('webhook-format-select');
const testWebhookBtn = document.getElementById('test-webhook-btn');
const webhookTestStatus = document.getElementById('webhook-test-status');

const addRuleForm = document.getElementById('add-rule-form');
const ruleMetricSelect = document.getElementById('rule-metric-select');
const ruleBizOptgroup = document.getElementById('rule-biz-optgroup');
const ruleOperatorSelect = document.getElementById('rule-operator-select');
const ruleThresholdInput = document.getElementById('rule-threshold-input');
const ruleCooldownInput = document.getElementById('rule-cooldown-input');
const alertRulesTbody = document.getElementById('alert-rules-tbody');

const alertLogsTbody = document.getElementById('alert-logs-tbody');
const clearAlertLogsBtn = document.getElementById('clear-alert-logs-btn');

// Initialize WebSocket to Node.js Quack Bridge
function initWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(`${protocol}//${window.location.host}`);

    ws.onopen = () => {
        console.log("Connected to Node.js WebSocket Quack Bridge.");
    };

    ws.onmessage = (event) => {
        try {
            const msg = JSON.parse(event.data);
            const { tabId, payload } = msg;

            const connObj = activeConnections.find(c => c.id === tabId);
            if (!connObj) return;

            if (payload.type === 'query_result') {
                if (activeConnectionId !== tabId) return;
                renderQueryResults(payload);
            } else if (payload.type === 'scheduled_result') {
                if (activeConnectionId !== tabId) return;
                updateScheduledQueryResult(payload);
            } else if (payload.error) {
                console.error(`Error for tab ${tabId}:`, payload.error);
                showToast('Quack Worker Error', payload.error, '', 'error');
                connObj.status = 'error';
                connObj.lastError = payload.error;
                renderTabs();

                if (activeConnectionId === tabId && !connObj.lastMetrics) {
                    connectError.innerText = `Connection failed: ${payload.error}`;
                    connectBtn.disabled = false;
                    connectBtn.innerText = 'Connect Console';
                } else if (activeConnectionId === tabId) {
                    connectionStatusPill.classList.remove('connected');
                    connectionStatusPill.querySelector('.status-text').innerText = 'Offline / Error';
                }

                if (payload.error.includes("Quack connection closed")) {
                    showToast('Reconnecting...', `Attempting to reconnect ${connObj.address} in 3s...`, '', 'info');
                    setTimeout(() => {
                        const currentConn = activeConnections.find(c => c.id === tabId);
                        if (currentConn && (currentConn.status === 'error' || currentConn.status === 'disconnected')) {
                            currentConn.status = 'connecting';
                            currentConn.lastError = null;
                            if (activeConnectionId === tabId) renderTabs();
                            ws.send(JSON.stringify({
                                action: 'connect',
                                tabId: tabId,
                                address: currentConn.address,
                                token: currentConn.token
                            }));
                        }
                    }, 3000);
                }
            } else if (payload.status === 'connected') {
                console.log(`Tab ${tabId} successfully attached to Quack server.`);
                connObj.status = 'connected';
                connObj.lastError = null;
                connectBtn.disabled = false;
                connectBtn.innerText = 'Connect Console';
                selectTab(connObj.id);
            } else if (payload.type === 'metrics') {
                if (connObj.status !== 'connected') {
                    connObj.status = 'connected';
                    connObj.lastError = null;
                    renderTabs();
                    if (activeConnectionId === connObj.id) {
                        connectionStatusPill.classList.add('connected');
                        connectionStatusPill.querySelector('.status-text').innerText = 'Connected';
                    }
                }

                const data = payload.data;
                let businessMetrics = {};
                try {
                    if (data.business_metrics) {
                        businessMetrics = JSON.parse(data.business_metrics);
                    }
                } catch (e) {}

                const totalMem = Number(data.total_memory || 0);
                const usedMem = Number(data.used_memory || 0);
                const memPct = totalMem > 0 ? (usedMem / totalMem) * 100 : 0;

                let tsSecs;
                if (typeof data.timestamp === 'string' && data.timestamp.includes('T')) {
                    tsSecs = new Date(data.timestamp).getTime() / 1000;
                } else {
                    tsSecs = Number(data.timestamp);
                }

                const latestRecord = {
                    timestamp: tsSecs,
                    cpu: Number(data.cpu_usage || 0),
                    memTotal: totalMem,
                    memUsed: usedMem,
                    memPct: memPct,
                    pyarrowAlloc: Number(data.pyarrow_allocated_bytes || 0),
                    pyarrowMax: Number(data.pyarrow_max_memory || 0),
                    lastRecords: Number(data.last_batch_records || 0),
                    totalRecords: Number(data.total_records_processed || 0),
                    latency: Number(data.last_batch_latency_ms || 0),
                    avgLatency: Number(data.avg_batch_latency_ms || 0),
                    rps: Number(data.records_per_second || 0),
                    duckdbLive: Number(data.duckdb_live_bytes || 0),
                    duckdbMetrics: Number(data.duckdb_metrics_bytes || 0),
                    businessMetrics: businessMetrics
                };

                connObj.metricsHistory.push(latestRecord);
                if (connObj.metricsHistory.length > 1000) {
                    connObj.metricsHistory.shift();
                }
                connObj.lastMetrics = latestRecord;

                // Dynamically populate business metrics inside trigger dropdown
                updateBusinessMetricsDropdown(businessMetrics);

                // Evaluate alerts on every metrics snapshot
                evaluateAlerts(latestRecord, connObj);

                if (activeConnectionId === connObj.id && currentView === 'metrics') {
                    updateDashboardUI(latestRecord);
                    updateChartUI();
                }
            } else if (payload.type === 'engine_alerts') {
                // Server-side engine/user alerts from rill_alerts table
                handleEngineAlerts(payload.data, connObj);
            } else if (payload.type === 'scheduled_query_sync') {
                const config = payload.config;
                window.scheduledConfigs = window.scheduledConfigs || {};
                
                if (!window.scheduledConfigs[config.queryId]) {
                    window.scheduledConfigs[config.queryId] = config;
                    
                    // Render in the UI table if active connection
                    if (activeConnectionId === tabId) {
                        const tr = document.createElement('tr');
                        tr.id = `sched-row-${config.queryId}`;
                        const customName = config.name || '-';
                        const sql = config.sql || '';
                        tr.innerHTML = `
                            <td><strong>${customName}</strong></td>
                            <td>${config.queryId.substring(0, 8)}...</td>
                            <td title="${sql}">${sql.length > 30 ? sql.substring(0, 30) + '...' : sql}</td>
                            <td>${config.intervalSeconds}s</td>
                            <td class="last-run">Pending</td>
                            <td class="last-result">Waiting...</td>
                            <td style="text-align: right;">
                                <button class="btn btn-secondary unschedule-btn" data-qid="${config.queryId}">Stop</button>
                            </td>
                        `;
                        
                        const emptyRow = scheduledQueriesTbody.querySelector('.empty-table-row');
                        if (emptyRow) emptyRow.remove();
                        
                        scheduledQueriesTbody.appendChild(tr);
                        
                        tr.querySelector('.unschedule-btn').addEventListener('click', () => {
                            ws.send(JSON.stringify({
                                action: 'unschedule_query',
                                tabId: activeConnectionId,
                                queryId: config.queryId
                            }));
                            
                            tr.remove();
                            if (scheduledQueriesTbody.children.length === 0) {
                                scheduledQueriesTbody.innerHTML = '<tr class="empty-table-row"><td colspan="6" class="empty-table">No queries scheduled.</td></tr>';
                            }
                            
                            delete window.scheduledConfigs[config.queryId];
                            if (window.scheduledCharts && window.scheduledCharts[config.queryId]) {
                                window.scheduledCharts[config.queryId].destroy();
                                delete window.scheduledCharts[config.queryId];
                            }
                            const widget = document.getElementById(`widget-${config.queryId}`);
                            if (widget) widget.remove();
                            
                            const grid = document.getElementById('scheduled-widgets-grid');
                            if (grid && grid.children.length === 0) {
                                document.getElementById('scheduled-widgets-container').style.display = 'none';
                            }
                        });
                    }
                }
            }
        } catch (err) {
            console.error("Error processing WebSocket message:", err);
        }
    };

    ws.onclose = () => {
        console.warn("WebSocket disconnected. Reconnecting in 3 seconds...");
        setTimeout(initWebSocket, 3000);
    };
}

initWebSocket();

// Initialize Chart.js
function initChart() {
    Chart.defaults.color = '#9ca3af';
    Chart.defaults.font.family = "'Outfit', sans-serif";

    // 1. System Resource Chart
    const ctxSys = document.getElementById('resource-chart').getContext('2d');
    resourceChart = new Chart(ctxSys, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'CPU Usage (%)',
                    data: [],
                    borderColor: '#6366f1',
                    backgroundColor: 'rgba(99, 102, 241, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4,
                    pointRadius: 0
                },
                {
                    label: 'Memory Usage (%)',
                    data: [],
                    borderColor: '#06b6d4',
                    backgroundColor: 'rgba(6, 182, 212, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4,
                    pointRadius: 0
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: { mode: 'index', intersect: false }
            },
            scales: {
                x: { grid: { color: 'rgba(255, 255, 255, 0.03)' }, ticks: { maxTicksLimit: 10 } },
                y: { min: 0, max: 100, grid: { color: 'rgba(255, 255, 255, 0.03)' } }
            }
        }
    });

    // Old business charts removed
}

// Format Helpers
function formatBytes(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

function formatTime(timestamp) {
    const date = new Date(timestamp * 1000);
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function extractNumeric(val) {
    if (typeof val === 'number') return val;
    if (typeof val === 'string') {
        const cleaned = val.replace(/[^0-9.-]+/g, '');
        const n = parseFloat(cleaned);
        return isNaN(n) ? null : n;
    }
    return null;
}

function sortRowsX(a, b, col) {
    const vA = a[col];
    const vB = b[col];
    if (typeof vA === 'string' && isNaN(Number(vA))) {
        return String(vA).localeCompare(String(vB));
    }
    return extractNumeric(vA) - extractNumeric(vB);
}

// Update UI Dashboard metrics & Business cards
function updateDashboardUI(latest) {
    // 1. System Cards
    valCpu.innerText = `${latest.cpu.toFixed(1)}%`;
    barCpu.style.width = `${latest.cpu}%`;

    const usedGB = (latest.memUsed / (1024 * 1024 * 1024)).toFixed(2);
    const totalGB = (latest.memTotal / (1024 * 1024 * 1024)).toFixed(2);
    valMem.innerText = `${usedGB} GB / ${totalGB} GB`;
    subMemPct.innerText = `${latest.memPct.toFixed(1)}% used`;
    barMem.style.width = `${latest.memPct}%`;

    if (latest.memPct > 85) {
        barMem.style.background = 'var(--accent-red)';
    } else if (latest.memPct > 70) {
        barMem.style.background = 'var(--accent-purple)';
    } else {
        barMem.style.background = 'linear-gradient(90deg, var(--accent-indigo) 0%, var(--accent-teal) 100%)';
    }

    valPyarrow.innerText = formatBytes(latest.pyarrowAlloc);
    valPyarrowPeak.innerText = `Peak: ${formatBytes(latest.pyarrowMax)}`;

    valRps.innerText = `${latest.rps.toFixed(2)} /s`;
    valTotalRecords.innerText = `Total: ${latest.totalRecords.toLocaleString()} processed`;

    valLatency.innerText = `${latest.latency.toFixed(2)} ms`;
    valAvgLatency.innerText = `Avg: ${latest.avgLatency.toFixed(2)} ms`;
    
    if (valDuckdbLive && valDuckdbMetrics) {
        valDuckdbLive.innerText = `Live: ${formatBytes(latest.duckdbLive)}`;
        valDuckdbMetrics.innerText = `Metrics: ${formatBytes(latest.duckdbMetrics)}`;
    }

    // Old business cards logic removed
}

function updateChartUI() {
    if (!resourceChart || activeConnectionId === null || currentView !== 'metrics') return;
    
    const connObj = activeConnections.find(c => c.id === activeConnectionId);
    if (!connObj) return;

    const activeFilter = historyFilter.value;
    const now = Date.now() / 1000;
    
    let filteredHistory = connObj.metricsHistory;
    if (activeFilter !== 'all') {
        const secondsLimit = parseInt(activeFilter, 10);
        filteredHistory = connObj.metricsHistory.filter(item => (now - item.timestamp) <= secondsLimit);
    }

    // 1. Update System Chart
    resourceChart.data.labels = filteredHistory.map(item => formatTime(item.timestamp));
    resourceChart.data.datasets[0].data = filteredHistory.map(item => item.cpu);
    resourceChart.data.datasets[1].data = filteredHistory.map(item => item.memPct);
    resourceChart.update('none');

    // Removed Business Trend Chart and Histogram updates
}

// Render tabs list dynamically (with offline/grey tab styling)
function renderTabs() {
    tabsBar.innerHTML = '';
    
    if (activeConnections.length === 0) {
        tabsContainer.classList.add('hidden');
        return;
    }
    
    tabsContainer.classList.remove('hidden');
    
    activeConnections.forEach(c => {
        const tab = document.createElement('div');
        const isOffline = c.status === 'error' || c.status === 'disconnected';
        tab.className = `tab-item ${c.id === activeConnectionId ? 'active' : ''} ${isOffline ? 'tab-offline-grey' : ''}`;
        if (isOffline && c.lastError) {
            tab.title = `Server Offline: ${c.lastError}`;
        }
        
        const img = document.createElement('img');
        img.src = 'logo.png';
        img.className = 'tab-logo-img';
        tab.appendChild(img);
        
        if (isOffline) {
            const dot = document.createElement('span');
            dot.className = 'status-dot-grey';
            tab.appendChild(dot);
        }

        const span = document.createElement('span');
        span.innerText = isOffline ? `${c.address} (Offline)` : c.address;
        span.addEventListener('click', () => selectTab(c.id));
        tab.appendChild(span);
        
        const closeBtn = document.createElement('button');
        closeBtn.className = 'tab-close-btn';
        closeBtn.innerHTML = '&times;';
        closeBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            closeConnection(c.id);
        });
        tab.appendChild(closeBtn);
        
        tabsBar.appendChild(tab);
    });
}

// Switch active tab connection
function selectTab(id) {
    activeConnectionId = id;
    renderTabs();
    
    if (id === null) {
        connectionPanel.classList.remove('hidden');
        dashboardContent.classList.add('hidden');
        
        connectionStatusPill.classList.remove('connected');
        connectionStatusPill.querySelector('.status-text').innerText = 'Add Connection';
    } else {
        const connObj = activeConnections.find(c => c.id === id);
        if (!connObj) return;
        
        connectionPanel.classList.add('hidden');
        dashboardContent.classList.remove('hidden');
        currentConnectedAddr.innerText = connObj.address;
        
        const isOffline = connObj.status === 'error' || connObj.status === 'disconnected';
        if (isOffline) {
            connectionStatusPill.classList.remove('connected');
            connectionStatusPill.querySelector('.status-text').innerText = 'Offline / Error';
        } else {
            connectionStatusPill.classList.add('connected');
            connectionStatusPill.querySelector('.status-text').innerText = 'Connected';
        }
        
        if (!resourceChart) {
            initChart();
        }
        
        if (connObj.lastMetrics) {
            updateDashboardUI(connObj.lastMetrics);
        } else {
            valCpu.innerText = '0.0%';
            barCpu.style.width = '0%';
            valMem.innerText = '0.0 GB / 0.0 GB';
            subMemPct.innerText = '0% used';
            barMem.style.width = '0%';
            valPyarrow.innerText = '0 Bytes';
            valPyarrowPeak.innerText = 'Peak: 0 Bytes';
            valRps.innerText = '0.00 /s';
            valTotalRecords.innerText = 'Total: 0 processed';
            valLatency.innerText = '0.00 ms';
            valAvgLatency.innerText = 'Avg: 0.00 ms';
        }
        
        updateChartUI();
    }
}

// Detach and close a connection tab
function closeConnection(id) {
    const idx = activeConnections.findIndex(c => c.id === id);
    if (idx === -1) return;
    
    const connObj = activeConnections[idx];
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action: 'disconnect', tabId: connObj.id }));
    }
    
    activeConnections.splice(idx, 1);
    
    if (activeConnectionId === id) {
        if (activeConnections.length > 0) {
            selectTab(activeConnections[0].id);
        } else {
            selectTab(null);
        }
    } else {
        renderTabs();
    }
}

// Connect Console Form Handler
connectForm.addEventListener('submit', (e) => {
    e.preventDefault();
    connectError.innerText = '';
    
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        connectError.innerText = 'WebSocket not connected to server bridge.';
        return;
    }

    const address = quackAddressInput.value.trim();
    const token = quackTokenInput.value.trim();

    const duplicate = activeConnections.find(c => c.address === address);
    if (duplicate) {
        connectError.innerText = `Already connected to server at ${address}.`;
        return;
    }

    connectBtn.disabled = true;
    connectBtn.innerText = 'Connecting...';

    const connId = nextConnectionId++;
    const connectionObj = {
        id: connId,
        address: address,
        token: token,
        status: 'connecting',
        lastError: null,
        metricsHistory: [],
        lastMetrics: null
    };

    activeConnections.push(connectionObj);
    quackTokenInput.value = '';

    ws.send(JSON.stringify({
        action: 'connect',
        tabId: connId,
        address: address,
        token: token
    }));
});

disconnectBtn.addEventListener('click', () => {
    if (activeConnectionId !== null) {
        closeConnection(activeConnectionId);
    }
});

tabAddBtn.addEventListener('click', () => {
    selectTab(null);
});

historyFilter.addEventListener('change', () => {
    updateChartUI();
});

/* ==========================================================================
   VIEW NAVIGATION SWITCHING (Metrics Console, Alert Settings, Alert Logs)
   ========================================================================== */
function switchDashboardView(viewName) {
    currentView = viewName;

    // Update nav tab buttons
    [tabBtnMetrics, tabBtnAlertSettings, tabBtnAlertLogs, tabBtnQueries].forEach(btn => {
        if (btn.dataset.view === viewName) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });

    // Update view visibility
    [viewMetrics, viewAlertSettings, viewAlertLogs, viewQueries].forEach(view => {
        view.classList.add('hidden');
    });

    if (viewName === 'metrics') {
        viewMetrics.classList.remove('hidden');
        updateChartUI();
    } else if (viewName === 'alert-settings') {
        viewAlertSettings.classList.remove('hidden');
        renderAlertRules();
    } else if (viewName === 'alert-logs') {
        viewAlertLogs.classList.remove('hidden');
        renderAlertLogs();
        // Clear active badge pulse
        alertBadgeCount.classList.remove('active-alert');
    } else if (viewName === 'queries') {
        viewQueries.classList.remove('hidden');
    }
}

tabBtnMetrics.addEventListener('click', () => switchDashboardView('metrics'));
tabBtnAlertSettings.addEventListener('click', () => switchDashboardView('alert-settings'));
tabBtnAlertLogs.addEventListener('click', () => switchDashboardView('alert-logs'));
tabBtnQueries.addEventListener('click', () => switchDashboardView('queries'));

/* ==========================================================================
   WEB AUDIO API SOUND SYNTHESIS ENGINE
   ========================================================================== */
function ensureAudioContext() {
    if (!audioCtx) {
        const AudioContextClass = window.AudioContext || window.webkitAudioContext;
        audioCtx = new AudioContextClass();
    }
    if (audioCtx.state === 'suspended') {
        audioCtx.resume();
    }
}

function playAlertSound(durationSeconds, toneType) {
    if (!alertSettings.soundEnabled) return;
    try {
        ensureAudioContext();
        const now = audioCtx.currentTime;
        const duration = Number(durationSeconds) || 3;

        if (toneType === 'beep') {
            // Pulsing beep (250ms on, 150ms off)
            const pulses = Math.max(1, Math.floor(duration / 0.4));
            for (let i = 0; i < pulses; i++) {
                const startTime = now + (i * 0.4);
                const osc = audioCtx.createOscillator();
                const gain = audioCtx.createGain();
                osc.type = 'sine';
                osc.frequency.setValueAtTime(800, startTime);
                
                gain.gain.setValueAtTime(0, startTime);
                gain.gain.linearRampToValueAtTime(0.4, startTime + 0.02);
                gain.gain.setValueAtTime(0.4, startTime + 0.22);
                gain.gain.linearRampToValueAtTime(0, startTime + 0.25);

                osc.connect(gain);
                gain.connect(audioCtx.destination);
                osc.start(startTime);
                osc.stop(startTime + 0.26);
            }
        } else if (toneType === 'siren') {
            // Ramping warning siren
            const osc = audioCtx.createOscillator();
            const gain = audioCtx.createGain();
            osc.type = 'sawtooth';
            
            const cycles = Math.max(1, Math.floor(duration / 0.6));
            for (let i = 0; i < cycles; i++) {
                const t = now + (i * 0.6);
                osc.frequency.setValueAtTime(600, t);
                osc.frequency.linearRampToValueAtTime(1200, t + 0.3);
                osc.frequency.linearRampToValueAtTime(600, t + 0.6);
            }

            gain.gain.setValueAtTime(0.3, now);
            gain.gain.setValueAtTime(0.3, now + duration - 0.1);
            gain.gain.linearRampToValueAtTime(0, now + duration);

            osc.connect(gain);
            gain.connect(audioCtx.destination);
            osc.start(now);
            osc.stop(now + duration);
        } else if (toneType === 'chime') {
            // Harmonic chime/bell (C5 + C6)
            const freqs = [523.25, 1046.50];
            freqs.forEach((freq, idx) => {
                const osc = audioCtx.createOscillator();
                const gain = audioCtx.createGain();
                osc.type = 'sine';
                osc.frequency.setValueAtTime(freq, now);

                const peakGain = idx === 0 ? 0.35 : 0.15;
                gain.gain.setValueAtTime(0, now);
                gain.gain.linearRampToValueAtTime(peakGain, now + 0.03);
                gain.gain.exponentialRampToValueAtTime(0.001, now + duration);

                osc.connect(gain);
                gain.connect(audioCtx.destination);
                osc.start(now);
                osc.stop(now + duration);
            });
        } else if (toneType === 'buzzer') {
            // Urgent square wave buzzer
            const osc = audioCtx.createOscillator();
            const gain = audioCtx.createGain();
            osc.type = 'square';
            osc.frequency.setValueAtTime(150, now);

            gain.gain.setValueAtTime(0.25, now);
            gain.gain.setValueAtTime(0.25, now + duration - 0.05);
            gain.gain.linearRampToValueAtTime(0, now + duration);

            osc.connect(gain);
            gain.connect(audioCtx.destination);
            osc.start(now);
            osc.stop(now + duration);
        }
    } catch (err) {
        console.error('Web Audio API playback error:', err);
    }
}

testSoundBtn.addEventListener('click', () => {
    playAlertSound(alertSettings.soundDuration, alertSettings.soundTone);
});

/* ==========================================================================
   WEBHOOK NOTIFICATIONS ENGINE
   ========================================================================== */
async function sendWebhookAlert(rule, observedValue, serverAddr) {
    if (!alertSettings.webhookEnabled || !alertSettings.webhookUrl) return { sent: false, reason: 'disabled' };
    
    try {
        const payload = {
            ruleName: `${rule.metricField} ${rule.operator} ${rule.threshold}`,
            metricField: rule.metricField,
            operator: rule.operator,
            threshold: rule.threshold,
            observedValue: observedValue,
            serverAddress: serverAddr || 'Rill Console',
            timestamp: Math.floor(Date.now() / 1000)
        };

        const res = await fetch('/api/webhook/send', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                url: alertSettings.webhookUrl,
                format: alertSettings.webhookFormat,
                payload: payload
            })
        });

        const data = await res.json();
        return { sent: data.success, status: data.status, error: data.error };
    } catch (err) {
        console.error('Webhook dispatch failed:', err);
        return { sent: false, error: err.message };
    }
}

testWebhookBtn.addEventListener('click', async () => {
    webhookTestStatus.className = 'status-message';
    webhookTestStatus.innerText = 'Sending test webhook...';
    
    const fakeRule = { metricField: 'cpu', operator: '>', threshold: 85 };
    const result = await sendWebhookAlert(fakeRule, 91.5, 'quack:127.0.0.1:9494');
    
    if (result.sent) {
        webhookTestStatus.className = 'status-message success';
        webhookTestStatus.innerText = `✅ Webhook delivered successfully (HTTP ${result.status})`;
    } else {
        webhookTestStatus.className = 'status-message error';
        webhookTestStatus.innerText = `❌ Delivery failed: ${result.error || result.reason}`;
    }
});

/* ==========================================================================
   ALERT EVALUATION ENGINE & TOAST NOTIFICATIONS
   ========================================================================== */
function showToast(title, subtitle, statusBadgesHtml = '', severity = '') {
    const toast = document.createElement('div');
    toast.className = 'alert-toast';
    if (severity) toast.classList.add(`toast-${severity}`);
    toast.innerHTML = `
        <div class="toast-icon">🚨</div>
        <div class="toast-content">
            <div class="toast-title">${title}</div>
            <div class="toast-subtitle">${subtitle}</div>
            ${statusBadgesHtml ? `<div style="margin-top:0.5rem; display:flex; gap:0.4rem; flex-wrap:wrap;">${statusBadgesHtml}</div>` : ''}
        </div>
        <button class="toast-close">&times;</button>
    `;

    toast.querySelector('.toast-close').addEventListener('click', () => {
        toast.classList.add('toast-exit');
        setTimeout(() => toast.remove(), 300);
    });

    alertToastContainer.appendChild(toast);

    // Auto dismiss after 6 seconds
    setTimeout(() => {
        if (toast.parentElement) {
            toast.classList.add('toast-exit');
            setTimeout(() => toast.remove(), 300);
        }
    }, 6000);
}

function updateAlertLogsBadge() {
    alertBadgeCount.innerText = alertLogs.length;
    if (alertLogs.length > 0) {
        alertBadgeCount.classList.add('active-alert');
    } else {
        alertBadgeCount.classList.remove('active-alert');
    }
}

async function evaluateAlerts(latestRecord, connObj) {
    if (!alertRules || alertRules.length === 0) return;
    const now = Math.floor(Date.now() / 1000);

    for (const rule of alertRules) {
        if (!rule.enabled) continue;

        let observedValue = null;
        // Check if field is system property or inside businessMetrics
        if (latestRecord.hasOwnProperty(rule.metricField)) {
            observedValue = latestRecord[rule.metricField];
        } else if (latestRecord.businessMetrics && latestRecord.businessMetrics.hasOwnProperty(rule.metricField)) {
            observedValue = extractNumeric(latestRecord.businessMetrics[rule.metricField]);
        }

        if (observedValue === null || observedValue === undefined) continue;

        let isTriggered = false;
        if (rule.operator === '>') isTriggered = observedValue > rule.threshold;
        else if (rule.operator === '>=') isTriggered = observedValue >= rule.threshold;
        else if (rule.operator === '<') isTriggered = observedValue < rule.threshold;
        else if (rule.operator === '<=') isTriggered = observedValue <= rule.threshold;
        else if (rule.operator === '==') isTriggered = observedValue == rule.threshold;

        if (isTriggered) {
            const cooldown = Number(rule.cooldownSeconds) || 10;
            const lastTime = Number(rule.lastTriggered) || 0;
            
            if ((now - lastTime) >= cooldown) {
                rule.lastTriggered = now;
                saveAlertRules();

                // 1. Play Sound
                let soundBadge = `<span class="badge-status played">🔊 Played (${alertSettings.soundDuration}s)</span>`;
                if (!alertSettings.soundEnabled) {
                    soundBadge = `<span class="badge-status muted">🔇 Sound Muted</span>`;
                } else {
                    playAlertSound(alertSettings.soundDuration, alertSettings.soundTone);
                }

                // 2. Send Webhook
                let webhookBadge = '';
                if (alertSettings.webhookEnabled && alertSettings.webhookUrl) {
                    const whRes = await sendWebhookAlert(rule, observedValue, connObj.address);
                    if (whRes.sent) {
                        webhookBadge = `<span class="badge-status webhook-sent">🔗 Webhook Sent (200)</span>`;
                    } else {
                        webhookBadge = `<span class="badge-status webhook-error">❌ Webhook Error</span>`;
                    }
                }

                // 3. Log Event
                const formattedVal = typeof observedValue === 'number' ? observedValue.toFixed(2) : observedValue;
                const logEntry = {
                    id: `${now}-${Math.random().toString(36).substring(2, 7)}`,
                    timestamp: now,
                    severity: 'warning',
                    sourceType: 'rule',
                    source: `rule.${rule.metricField}`,
                    isEngineAlert: false,
                    ruleCondition: `${rule.metricField} ${rule.operator} ${rule.threshold}`,
                    observedValue: formattedVal,
                    serverAddress: connObj.address,
                    soundStatus: alertSettings.soundEnabled ? `Played (${alertSettings.soundDuration}s)` : 'Muted',
                    webhookStatus: alertSettings.webhookEnabled ? (webhookBadge.includes('Sent') ? 'Sent (200)' : 'Error') : 'Disabled',
                    statusBadges: `${soundBadge} ${webhookBadge}`
                };

                alertLogs.unshift(logEntry);
                if (alertLogs.length > 200) alertLogs.pop();
                localStorage.setItem('rill_alert_logs', JSON.stringify(alertLogs));

                // 4. Update Badge & Toast
                updateAlertLogsBadge();
                if (currentView === 'alert-logs') {
                    renderAlertLogs();
                }

                showToast(
                    `Alert Triggered: ${rule.metricField}`,
                    `Observed value <strong>${formattedVal}</strong> met condition (${rule.operator} ${rule.threshold}) on ${connObj.address}`,
                    `${soundBadge} ${webhookBadge}`
                );
            }
        }
    }
}

/* ==========================================================================
   SETTINGS & RULES UI RENDERING AND MANAGEMENT
   ========================================================================== */
function loadAlertSettingsUI() {
    alertSoundEnableToggle.checked = alertSettings.soundEnabled;
    alertDurationSlider.value = alertSettings.soundDuration;
    alertDurationInput.value = alertSettings.soundDuration;
    alertToneSelect.value = alertSettings.soundTone;

    webhookEnableToggle.checked = alertSettings.webhookEnabled;
    webhookUrlInput.value = alertSettings.webhookUrl || '';
    webhookFormatSelect.value = alertSettings.webhookFormat || 'slack';
}

function saveAlertSettings() {
    alertSettings.soundEnabled = alertSoundEnableToggle.checked;
    alertSettings.soundDuration = Number(alertDurationInput.value) || 3;
    alertSettings.soundTone = alertToneSelect.value;

    alertSettings.webhookEnabled = webhookEnableToggle.checked;
    alertSettings.webhookUrl = webhookUrlInput.value.trim();
    alertSettings.webhookFormat = webhookFormatSelect.value;

    localStorage.setItem('rill_alert_settings', JSON.stringify(alertSettings));
}

document.addEventListener('DOMContentLoaded', () => {
    initWebSocket();
    renderTabs();
    selectTab(null);
    renderAlertRules();
    renderAlertLogs();
    
    // Add Query View Logic
    tabBtnQueries.addEventListener('click', () => switchDashboardView('queries'));

    function generateQueryId() {
        return Date.now().toString() + Math.random().toString(36).substring(2);
    }
    
    runQueryBtn.addEventListener('click', () => {
        if (!activeConnectionId) return;
        let sql = sqlQueryEditor.value.trim();
        if (!sql) return;
        
        const limitSelect = document.getElementById('query-limit');
        if (limitSelect && limitSelect.value !== 'all' && sql.toUpperCase().startsWith('SELECT') && !/LIMIT\s+\d+/i.test(sql)) {
            if (sql.endsWith(';')) sql = sql.slice(0, -1);
            sql = `${sql} LIMIT ${limitSelect.value}`;
        }
        
        queryErrorMsg.style.display = 'none';
        queryResultsTbody.innerHTML = '<tr><td class="empty-table">Executing query...</td></tr>';
        
        ws.send(JSON.stringify({
            action: 'preview_query',
            tabId: activeConnectionId,
            queryId: 'preview_' + activeConnectionId,
            sql: sql
        }));
    });
    
    if (showTablesBtn) {
        showTablesBtn.addEventListener('click', () => {
            if (!activeConnectionId) return;
            sqlQueryEditor.value = "SELECT table_catalog, table_schema, table_name FROM information_schema.tables;";
            runQueryBtn.click();
        });
    }
    
    scheduleQueryBtn.addEventListener('click', () => {
        if (!activeConnectionId) return;
        const sql = sqlQueryEditor.value.trim();
        const interval = parseInt(scheduleInterval.value, 10);
        if (!sql || interval <= 0 || isNaN(interval)) return;
        
        const qid = generateQueryId();
        const customName = document.getElementById('query-name') ? document.getElementById('query-name').value.trim() : '';
        
        window.scheduledConfigs = window.scheduledConfigs || {};
        window.scheduledConfigs[qid] = {
            name: customName,
            sql: sql,
            type: queryChartType.value,
            xCol: queryXCol.value,
            yCol: queryYCol.value,
            colorCol: queryColorCol.value
        };
        
        ws.send(JSON.stringify({
            action: 'schedule_query',
            tabId: activeConnectionId,
            queryId: qid,
            sql: sql,
            intervalSeconds: interval
        }));
        
        const tr = document.createElement('tr');
        tr.id = `sched-row-${qid}`;
        tr.innerHTML = `
            <td><strong>${customName || '-'}</strong></td>
            <td>${qid.substring(0, 8)}...</td>
            <td title="${sql}">${sql.length > 30 ? sql.substring(0, 30) + '...' : sql}</td>
            <td>${interval}s</td>
            <td class="last-run">Pending</td>
            <td class="last-result">Waiting...</td>
            <td style="text-align: right;">
                <button class="btn btn-secondary unschedule-btn" data-qid="${qid}">Stop</button>
            </td>
        `;
        
        const emptyRow = scheduledQueriesTbody.querySelector('.empty-table-row');
        if (emptyRow) emptyRow.remove();
        
        scheduledQueriesTbody.appendChild(tr);
        
        tr.querySelector('.unschedule-btn').addEventListener('click', () => {
            ws.send(JSON.stringify({
                action: 'unschedule_query',
                tabId: activeConnectionId,
                queryId: qid
            }));
            
            tr.remove();
            if (scheduledQueriesTbody.children.length === 0) {
                scheduledQueriesTbody.innerHTML = '<tr class="empty-table-row"><td colspan="6" class="empty-table">No queries scheduled.</td></tr>';
            }
            
            if (window.scheduledConfigs && window.scheduledConfigs[qid]) delete window.scheduledConfigs[qid];
            if (window.scheduledCharts && window.scheduledCharts[qid]) {
                window.scheduledCharts[qid].destroy();
                delete window.scheduledCharts[qid];
            }
            const widget = document.getElementById(`widget-${qid}`);
            if (widget) widget.remove();
            
            const grid = document.getElementById('scheduled-widgets-grid');
            if (grid && grid.children.length === 0) {
                document.getElementById('scheduled-widgets-container').style.display = 'none';
            }
    });
    });
    
    window.renderQueryResults = function(payload) {
        lastQueryResult = null;
        if (payload.error) {
            queryErrorMsg.innerText = payload.error;
            queryErrorMsg.style.display = 'block';
            queryResultsTbody.innerHTML = '<tr class="empty-table-row"><td class="empty-table">Query failed.</td></tr>';
            columnMappingPanel.style.display = 'none';
            queryChartCard.style.display = 'none';
            return;
        }
        
        queryElapsedTag.innerText = payload.elapsed_ms.toFixed(1) + ' ms';
        
        if (!payload.columns || payload.columns.length === 0) {
            queryResultsThead.innerHTML = '<tr><th>Result</th></tr>';
            queryResultsTbody.innerHTML = '<tr><td class="empty-table">Success.</td></tr>';
            columnMappingPanel.style.display = 'none';
            queryChartCard.style.display = 'none';
            return;
        }
        
        lastQueryResult = payload;
        
        let headerRow = '<tr>';
        payload.columns.forEach(c => headerRow += `<th>${c}</th>`);
        headerRow += '</tr>';
        queryResultsThead.innerHTML = headerRow;
        
        if (!payload.rows || payload.rows.length === 0) {
            queryResultsTbody.innerHTML = `<tr><td colspan="${payload.columns.length}" class="empty-table">No rows returned.</td></tr>`;
            columnMappingPanel.style.display = 'none';
            queryChartCard.style.display = 'none';
            return;
        }
        
        let bodyRows = '';
        payload.rows.forEach(r => {
            let rowHtml = '<tr>';
            payload.columns.forEach(c => rowHtml += `<td>${r[c]}</td>`);
            rowHtml += '</tr>';
            bodyRows += rowHtml;
        });
        queryResultsTbody.innerHTML = bodyRows;
        
        // Setup column mapping
        columnMappingPanel.style.display = 'block';
        
        let opts = '';
        payload.columns.forEach(c => opts += `<option value="${c}">${c}</option>`);
        queryXCol.innerHTML = opts;
        queryYCol.innerHTML = opts;
        queryColorCol.innerHTML = '<option value="">None</option>' + opts;
        
        // Auto-select defaults if possible
        if (payload.columns.includes('ts')) queryXCol.value = 'ts';
        if (payload.columns.includes('value')) queryYCol.value = 'value';
        if (payload.columns.includes('metric_name')) queryColorCol.value = 'metric_name';
    };
    
    queryRenderChartBtn.addEventListener('click', () => {
        if (!lastQueryResult || !lastQueryResult.rows) return;
        
        const type = queryChartType.value;
        if (type === 'none') {
            queryChartCard.style.display = 'none';
            return;
        }
        
        const xCol = queryXCol.value;
        const yCol = queryYCol.value;
        const colorCol = queryColorCol.value;
        
        queryChartCard.style.display = 'block';
        
        const rows = lastQueryResult.rows;
        let datasets = [];
        
        const palette = [
            '#06b6d4', // teal
            '#6366f1', // indigo
            '#a855f7', // purple
            '#ef4444', // red
            '#f59e0b', // yellow/amber
            '#2ECC71', '#3498DB', '#9B59B6', '#E74C3C', '#F1C40F'
        ];
        
        if (colorCol) {
            const groups = {};
            rows.forEach(r => {
                const groupVal = r[colorCol];
                if (!groups[groupVal]) groups[groupVal] = [];
                groups[groupVal].push(r);
            });
            
            let colorIdx = 0;
            for (const [gName, gRows] of Object.entries(groups)) {
                // sort rows by X
                gRows.sort((a,b) => sortRowsX(a, b, xCol));
                
                datasets.push({
                    label: String(gName),
                    data: gRows.map(r => ({ x: r[xCol], y: extractNumeric(r[yCol]) })),
                    borderColor: palette[colorIdx % palette.length],
                    backgroundColor: palette[colorIdx % palette.length],
                    borderWidth: 2,
                    pointRadius: type === 'scatter' ? 4 : 2
                });
                colorIdx++;
            }
        } else {
            rows.sort((a,b) => sortRowsX(a, b, xCol));
            datasets.push({
                label: yCol,
                data: rows.map(r => ({ x: r[xCol], y: extractNumeric(r[yCol]) })),
                borderColor: palette[0],
                backgroundColor: palette[0],
                borderWidth: 2,
                pointRadius: type === 'scatter' ? 4 : 2
            });
        }
        
        if (customQueryChart) {
            customQueryChart.destroy();
        }
        
        const firstXVal = rows.length > 0 ? rows[0][xCol] : null;
        const xIsCategory = typeof firstXVal === 'string' && isNaN(Number(firstXVal));

        const ctx = document.getElementById('custom-query-chart').getContext('2d');
        customQueryChart = new Chart(ctx, {
            type: type === 'scatter' ? 'scatter' : (type === 'bar' ? 'bar' : 'line'),
            data: { datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'nearest', intersect: false },
                plugins: {
                    legend: {
                        labels: { color: 'var(--text-primary)' },
                        onClick: function(e, legendItem, legend) {
                            const index = legendItem.datasetIndex;
                            const ci = legend.chart;
                            if (ci.isDatasetVisible(index)) {
                                ci.hide(index);
                                legendItem.hidden = true;
                            } else {
                                ci.show(index);
                                legendItem.hidden = false;
                            }
                        }
                    },
                    zoom: {
                        zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'x' },
                        pan: { enabled: true, mode: 'x' }
                    }
                },
                scales: {
                    x: { type: xIsCategory ? 'category' : 'linear', ticks: { color: 'var(--text-secondary)' }, grid: { color: 'var(--border-color)' } },
                    y: { ticks: { color: 'var(--text-secondary)' }, grid: { color: 'var(--border-color)' } }
                }
            }
        });
    });

    
    window.updateScheduledQueryResult = function(payload) {
        const tr = document.getElementById(`sched-row-${payload.queryId}`);
        if (!tr) return;
        
        tr.querySelector('.last-run').innerText = formatTime(payload.ranAt / 1000);
        
        if (payload.error) {
            tr.querySelector('.last-result').innerText = payload.error;
            tr.querySelector('.last-result').style.color = 'var(--accent-red)';
            return;
        } else {
            tr.querySelector('.last-result').innerText = `Success (${payload.rows ? payload.rows.length : 0} rows, ${payload.elapsed_ms.toFixed(1)}ms)`;
            tr.querySelector('.last-result').style.color = 'var(--accent-teal)';
        }

        const config = (window.scheduledConfigs || {})[payload.queryId];
        if (!config || !payload.rows) return;
        
        const container = document.getElementById('scheduled-widgets-container');
        const grid = document.getElementById('scheduled-widgets-grid');
        if (container) container.style.display = 'block';
        
        let widget = document.getElementById(`widget-${payload.queryId}`);
        if (!widget) {
            widget = document.createElement('div');
            widget.id = `widget-${payload.queryId}`;
            widget.className = 'card chart-card';
            widget.style.cssText = 'display: flex; flex-direction: column; resize: both; overflow: hidden; width: 450px; height: 350px; min-width: 300px; min-height: 250px; background: var(--bg-card);';
            
            if (config.type === 'none') {
                widget.innerHTML = `
                    <div class="chart-header">
                        <h2>📋 ${config.name ? config.name : `Scheduled Query: ${config.sql.substring(0, 30)}...`}</h2>
                    </div>
                    <div class="table-container" style="flex: 1; overflow-y: auto; height: 100%; min-height: 200px;">
                        <table class="history-table">
                            <thead id="widget-thead-${payload.queryId}"></thead>
                            <tbody id="widget-tbody-${payload.queryId}"></tbody>
                        </table>
                    </div>
                `;
            } else {
                widget.innerHTML = `
                    <div class="chart-header">
                        <h2>📈 ${config.name ? config.name : `Scheduled Query: ${config.sql.substring(0, 30)}...`}</h2>
                    </div>
                    <div class="chart-container" style="flex: 1; height: 100%; min-height: 200px; position: relative;">
                        <canvas id="widget-canvas-${payload.queryId}"></canvas>
                    </div>
                `;
            }
            grid.appendChild(widget);
        }
        
        if (config.type === 'none') {
            const thead = document.getElementById(`widget-thead-${payload.queryId}`);
            const tbody = document.getElementById(`widget-tbody-${payload.queryId}`);
            
            let headerHtml = '<tr>';
            payload.columns.forEach(c => headerHtml += `<th>${c}</th>`);
            headerHtml += '</tr>';
            thead.innerHTML = headerHtml;
            
            let bodyHtml = '';
            payload.rows.forEach(r => {
                let rowHtml = '<tr>';
                payload.columns.forEach(c => rowHtml += `<td>${r[c]}</td>`);
                rowHtml += '</tr>';
                bodyHtml += rowHtml;
            });
            tbody.innerHTML = bodyHtml;
        } else {
            window.scheduledCharts = window.scheduledCharts || {};
            const canvas = document.getElementById(`widget-canvas-${payload.queryId}`);
            if (!canvas) return;
            
            const rows = payload.rows;
            let datasets = [];
            const palette = ['#06b6d4', '#6366f1', '#a855f7', '#ef4444', '#f59e0b', '#2ECC71', '#3498DB', '#9B59B6', '#E74C3C', '#F1C40F'];
            
            const { xCol, yCol, colorCol } = config;
            
            if (colorCol) {
                const groups = {};
                rows.forEach(r => {
                    const groupVal = r[colorCol];
                    if (!groups[groupVal]) groups[groupVal] = [];
                    groups[groupVal].push(r);
                });
                
                let colorIdx = 0;
                for (const [gName, gRows] of Object.entries(groups)) {
                    gRows.sort((a,b) => extractNumeric(a[xCol]) - extractNumeric(b[xCol]));
                    datasets.push({
                        label: String(gName),
                        data: gRows.map(r => ({ x: r[xCol], y: extractNumeric(r[yCol]) })),
                        borderColor: palette[colorIdx % palette.length],
                        backgroundColor: palette[colorIdx % palette.length],
                        borderWidth: 2,
                        pointRadius: config.type === 'scatter' ? 4 : 2
                    });
                    colorIdx++;
                }
            } else {
                rows.sort((a,b) => extractNumeric(a[xCol]) - extractNumeric(b[xCol]));
                datasets.push({
                    label: yCol,
                    data: rows.map(r => ({ x: r[xCol], y: extractNumeric(r[yCol]) })),
                    borderColor: palette[0],
                    backgroundColor: palette[0],
                    borderWidth: 2,
                    pointRadius: config.type === 'scatter' ? 4 : 2
                });
            }
            
            const firstXVal = rows.length > 0 ? rows[0][xCol] : null;
            const xIsCategory = typeof firstXVal === 'string' && isNaN(Number(firstXVal));
            
            if (window.scheduledCharts[payload.queryId]) {
                window.scheduledCharts[payload.queryId].data.datasets = datasets;
                window.scheduledCharts[payload.queryId].options.scales.x.type = xIsCategory ? 'category' : 'linear';
                window.scheduledCharts[payload.queryId].update('none');
            } else {
                const ctx = canvas.getContext('2d');
                window.scheduledCharts[payload.queryId] = new Chart(ctx, {
                    type: config.type === 'scatter' ? 'scatter' : (config.type === 'bar' ? 'bar' : 'line'),
                    data: { datasets },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { labels: { color: 'var(--text-primary)' } } },
                        scales: {
                            x: { type: xIsCategory ? 'category' : 'linear', ticks: { color: 'var(--text-secondary)' }, grid: { color: 'var(--border-color)' } },
                            y: { ticks: { color: 'var(--text-secondary)' }, grid: { color: 'var(--border-color)' } }
                        },
                        animation: false
                    }
                });
            }

            // Expose scheduled query numeric columns as business metrics for alerts
            if (rows && rows.length > 0) {
                const connObj = activeConnections.find(c => c.id === activeConnectionId);
                if (connObj && connObj.lastMetrics && connObj.lastMetrics.businessMetrics) {
                    const latestRow = rows[rows.length - 1]; // After sort, last row is usually latest in time
                    const newMetrics = {};
                    for (const [key, value] of Object.entries(latestRow)) {
                        if (key !== 'ts' && key !== 'timestamp' && typeof value === 'number' && !isNaN(value)) {
                            const metricName = `${config.name || payload.queryId} - ${key}`;
                            connObj.lastMetrics.businessMetrics[metricName] = value;
                            newMetrics[metricName] = value;
                        }
                    }
                    if (Object.keys(newMetrics).length > 0) {
                        updateBusinessMetricsDropdown(newMetrics);
                        evaluateAlerts(connObj.lastMetrics, connObj);
                    }
                }
            }
        }
    };
});

alertSoundEnableToggle.addEventListener('change', saveAlertSettings);
alertToneSelect.addEventListener('change', saveAlertSettings);
alertDurationSlider.addEventListener('input', () => {
    alertDurationInput.value = alertDurationSlider.value;
    saveAlertSettings();
});
alertDurationInput.addEventListener('input', () => {
    let val = Math.max(1, Math.min(60, Number(alertDurationInput.value) || 1));
    alertDurationSlider.value = val;
    saveAlertSettings();
});

webhookEnableToggle.addEventListener('change', saveAlertSettings);
webhookUrlInput.addEventListener('input', saveAlertSettings);
webhookFormatSelect.addEventListener('change', saveAlertSettings);

loadAlertSettingsUI();
updateAlertLogsBadge();

function updateBusinessMetricsDropdown(businessMetrics) {
    if (!businessMetrics) return;
    let addedNew = false;
    Object.keys(businessMetrics).forEach(key => {
        if (!knownBusinessKeys.has(key)) {
            knownBusinessKeys.add(key);
            const opt = document.createElement('option');
            opt.value = key;
            opt.innerText = `${key} (Business Metric)`;
            ruleBizOptgroup.appendChild(opt);
            addedNew = true;
        }
    });
}

function saveAlertRules() {
    localStorage.setItem('rill_alert_rules', JSON.stringify(alertRules));
}

function renderAlertRules() {
    alertRulesTbody.innerHTML = '';
    if (alertRules.length === 0) {
        alertRulesTbody.innerHTML = `
            <tr>
                <td colspan="6" class="empty-table">No alert rules configured. Add a rule above to start monitoring.</td>
            </tr>
        `;
        return;
    }

    alertRules.forEach(rule => {
        const tr = document.createElement('tr');
        const lastTimeStr = rule.lastTriggered > 0 ? formatTime(rule.lastTriggered) : 'Never';

        tr.innerHTML = `
            <td>
                <label class="toggle-switch-wrapper">
                    <input type="checkbox" class="rule-toggle-checkbox" data-id="${rule.id}" ${rule.enabled ? 'checked' : ''}>
                    <span class="toggle-slider"></span>
                </label>
            </td>
            <td><strong style="color:var(--text-primary);">${rule.metricField}</strong></td>
            <td><code style="background:rgba(255,255,255,0.05); padding:0.2rem 0.5rem; border-radius:4px; color:var(--accent-teal);">${rule.operator} ${rule.threshold}</code></td>
            <td>${rule.cooldownSeconds}s</td>
            <td style="color:var(--text-secondary);">${lastTimeStr}</td>
            <td style="text-align: right;">
                <button class="btn btn-secondary rule-delete-btn" data-id="${rule.id}" style="padding:0.35rem 0.75rem; border-color:rgba(239,68,68,0.3); color:#f87171;">Delete</button>
            </td>
        `;

        tr.querySelector('.rule-toggle-checkbox').addEventListener('change', (e) => {
            rule.enabled = e.target.checked;
            saveAlertRules();
        });

        tr.querySelector('.rule-delete-btn').addEventListener('click', () => {
            alertRules = alertRules.filter(r => r.id !== rule.id);
            saveAlertRules();
            renderAlertRules();
        });

        alertRulesTbody.appendChild(tr);
    });
}

addRuleForm.addEventListener('submit', (e) => {
    e.preventDefault();
    const metricField = ruleMetricSelect.value;
    const operator = ruleOperatorSelect.value;
    const threshold = parseFloat(ruleThresholdInput.value);
    const cooldownSeconds = parseInt(ruleCooldownInput.value, 10) || 10;

    if (isNaN(threshold)) return;

    const newRule = {
        id: `rule-${Date.now()}`,
        enabled: true,
        metricField,
        operator,
        threshold,
        cooldownSeconds,
        lastTriggered: 0
    };

    alertRules.push(newRule);
    saveAlertRules();
    renderAlertRules();

    ruleThresholdInput.value = '';
});

/* ==========================================================================
   ENGINE / USER ALERTS HANDLER (from rill_alerts table)
   ========================================================================== */
function handleEngineAlerts(alertsData, connObj) {
    if (!alertsData || alertsData.length === 0) return;

    for (const alert of alertsData) {
        const severity = alert.severity || 'info';
        const source = alert.source || 'engine';
        const sourceType = source === 'user' ? 'user' : (source.startsWith('engine.') || source.startsWith('compute.') || source.startsWith('metrics.') || source.startsWith('table.') ? 'engine' : 'engine');

        const logEntry = {
            id: `eng-${alert.ts}-${Math.random().toString(36).substring(2, 7)}`,
            timestamp: alert.ts,
            severity: severity,
            sourceType: sourceType,
            source: source,
            ruleCondition: alert.message || 'Engine Alert',
            observedValue: alert.detail ? alert.detail.substring(0, 200) : '',
            serverAddress: connObj.address || '-',
            soundStatus: 'N/A',
            webhookStatus: 'N/A',
            statusBadges: '',
            isEngineAlert: true
        };

        alertLogs.unshift(logEntry);

        // Toast for error and warning severity
        if (severity === 'error' || severity === 'warning') {
            const sourceLabel = sourceType === 'user' ? '👤 User Alert' : '⚙️ Engine';
            const severityIcon = severity === 'error' ? '🔴' : '🟡';
            showToast(
                `${severityIcon} ${sourceLabel}: ${alert.message}`,
                alert.detail ? `<code style="font-size:11px;color:var(--text-secondary)">${alert.detail.substring(0, 150)}</code>` : `Source: ${source}`,
                `<span class="badge-severity ${severity}">${severity}</span> <span class="badge-source ${sourceType}">${sourceLabel}</span>`,
                severity
            );

            // Play alert sound for errors
            if (severity === 'error') {
                playAlertSound(alertSettings.soundDuration, alertSettings.soundTone);
            }
        }
    }

    if (alertLogs.length > 500) alertLogs.length = 500;
    localStorage.setItem('rill_alert_logs', JSON.stringify(alertLogs));
    updateAlertLogsBadge();
    if (currentView === 'alert-logs') {
        renderAlertLogs();
    }
}

function getSeverityBadgeHtml(severity) {
    const icons = { error: '🔴', warning: '🟡', info: '🔵' };
    return `<span class="badge-severity ${severity || 'info'}">${icons[severity] || '🔵'} ${severity || 'info'}</span>`;
}

function getSourceBadgeHtml(log) {
    if (log.isEngineAlert) {
        if (log.sourceType === 'user') {
            return `<span class="badge-source user">👤 User</span>`;
        }
        return `<span class="badge-source engine">⚙️ Engine</span>`;
    }
    return `<span class="badge-source rule">⚡ Rule</span>`;
}

function renderAlertLogs() {
    alertLogsTbody.innerHTML = '';

    // Apply filter
    let filtered = alertLogs;
    if (alertLogFilter !== 'all') {
        filtered = alertLogs.filter(log => {
            if (alertLogFilter === 'error') return log.severity === 'error';
            if (alertLogFilter === 'warning') return log.severity === 'warning';
            if (alertLogFilter === 'user') return log.sourceType === 'user';
            if (alertLogFilter === 'engine') return log.isEngineAlert && log.sourceType !== 'user';
            if (alertLogFilter === 'rule') return !log.isEngineAlert;
            return true;
        });
    }

    if (filtered.length === 0) {
        alertLogsTbody.innerHTML = `
            <tr class="empty-table-row">
                <td colspan="7" class="empty-table">No alerts match the current filter.</td>
            </tr>
        `;
        return;
    }

    filtered.forEach(log => {
        const tr = document.createElement('tr');
        const severity = log.severity || (log.isEngineAlert ? 'info' : 'warning');
        const severityBadge = getSeverityBadgeHtml(severity);
        const sourceBadge = getSourceBadgeHtml(log);

        let valueDetail = '';
        if (log.isEngineAlert) {
            valueDetail = log.observedValue
                ? `<span class="alert-detail-text" title="${log.observedValue.replace(/"/g, '&quot;')}">${log.observedValue}</span>`
                : '-';
        } else {
            valueDetail = `<code style="color: #f87171; font-weight:700;">${log.observedValue}</code>`;
        }

        let statusCol = '';
        if (log.isEngineAlert) {
            statusCol = `<span class="badge-status" style="background:rgba(99,102,241,0.15);color:#818cf8;border:1px solid rgba(99,102,241,0.3);font-size:11px;">Logged</span>`;
        } else {
            statusCol = log.statusBadges || `<span class="badge-status played">🔊 ${log.soundStatus}</span>`;
        }

        tr.innerHTML = `
            <td style="white-space: nowrap; font-family: monospace;">${formatTime(log.timestamp)}</td>
            <td>${severityBadge}</td>
            <td>${sourceBadge}</td>
            <td><strong>${log.ruleCondition}</strong></td>
            <td>${valueDetail}</td>
            <td style="color: var(--text-secondary);">${log.serverAddress}</td>
            <td>${statusCol}</td>
        `;
        alertLogsTbody.appendChild(tr);
    });
}

// Alert filter bar click handlers
document.getElementById('alert-filter-bar')?.addEventListener('click', (e) => {
    const btn = e.target.closest('.alert-filter-btn');
    if (!btn) return;
    const filter = btn.dataset.filter;
    alertLogFilter = filter || 'all';
    document.querySelectorAll('.alert-filter-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    renderAlertLogs();
});

clearAlertLogsBtn.addEventListener('click', () => {
    alertLogs = [];
    localStorage.setItem('rill_alert_logs', JSON.stringify(alertLogs));
    updateAlertLogsBadge();
    renderAlertLogs();
});

// Theme / Background Color Picker Logic
const bgColorPicker = document.getElementById('bg-color-picker');
if (bgColorPicker) {
    const savedBg = localStorage.getItem('rill_bg_color');
    if (savedBg) {
        document.documentElement.style.setProperty('--bg-primary', savedBg);
        bgColorPicker.value = savedBg;
    }
    
    bgColorPicker.addEventListener('input', (e) => {
        const color = e.target.value;
        document.documentElement.style.setProperty('--bg-primary', color);
        localStorage.setItem('rill_bg_color', color);
    });
}
