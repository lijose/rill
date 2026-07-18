// Application State
let ws = null;
let activeConnections = []; // Array of { id, address, token, status: 'connecting'|'connected'|'error'|'disconnected', lastError: null, metricsHistory: [], lastMetrics }
let activeConnectionId = null; // ID of the currently selected tab connection
let nextConnectionId = 1;
let resourceChart = null;
let businessTrendChart = null;
let businessHistChart = null;
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
const tabBtnAlertSettings = document.getElementById('tab-btn-alert-settings');
const tabBtnAlertLogs = document.getElementById('tab-btn-alert-logs');
const viewMetrics = document.getElementById('view-metrics');
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

// Business DOM & Controls
const businessCardsGrid = document.getElementById('business-cards-grid');
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

            if (payload.error) {
                console.error(`Error for tab ${tabId}:`, payload.error);
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

                const latestRecord = {
                    timestamp: Number(data.timestamp),
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

    // 2. Business Trend Line Plot
    const ctxBizTrend = document.getElementById('business-trend-chart').getContext('2d');
    businessTrendChart = new Chart(ctxBizTrend, {
        type: 'line',
        data: { labels: [], datasets: [] },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: true, position: 'top', labels: { boxWidth: 12 } },
                tooltip: { mode: 'index', intersect: false }
            },
            scales: {
                x: { grid: { color: 'rgba(255, 255, 255, 0.03)' }, ticks: { maxTicksLimit: 10 } },
                y: { grid: { color: 'rgba(255, 255, 255, 0.03)' } }
            }
        }
    });

    // 3. Business Distribution Histogram (Bar Chart)
    const ctxBizHist = document.getElementById('business-hist-chart').getContext('2d');
    businessHistChart = new Chart(ctxBizHist, {
        type: 'bar',
        data: { labels: [], datasets: [] },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: { mode: 'index', intersect: false }
            },
            scales: {
                x: { grid: { color: 'rgba(255, 255, 255, 0.03)' } },
                y: { grid: { color: 'rgba(255, 255, 255, 0.03)' }, beginAtZero: true }
            }
        }
    });
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

    // 2. Business Centerpiece Cards
    businessCardsGrid.innerHTML = '';
    const keys = Object.keys(latest.businessMetrics);
    if (keys.length === 0) {
        businessCardsGrid.innerHTML = '<p class="no-data">No custom metrics registered.</p>';
    } else {
        keys.forEach(key => {
            const card = document.createElement('div');
            card.className = 'biz-kpi-card';
            
            let displayVal = latest.businessMetrics[key];
            if (typeof displayVal === 'number') {
                displayVal = displayVal.toFixed(2);
            } else if (typeof displayVal === 'object') {
                displayVal = JSON.stringify(displayVal);
            }

            card.innerHTML = `
                <div class="biz-kpi-title">${key}</div>
                <div class="biz-kpi-value">${displayVal}</div>
            `;
            businessCardsGrid.appendChild(card);
        });
    }
}

// Update all charts (System + Business Trend Line + Business Histogram)
function updateChartUI() {
    if (!resourceChart || !businessTrendChart || !businessHistChart || activeConnectionId === null || currentView !== 'metrics') return;
    
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

    // 2. Update Business Trend Chart (Line Plot)
    const labels = filteredHistory.map(item => formatTime(item.timestamp));
    const allKeys = new Set();
    filteredHistory.forEach(item => {
        if (item.businessMetrics) {
            Object.keys(item.businessMetrics).forEach(k => {
                if (extractNumeric(item.businessMetrics[k]) !== null) {
                    allKeys.add(k);
                }
            });
        }
    });

    const colors = ['#10b981', '#f59e0b', '#ec4899', '#3b82f6', '#8b5cf6'];
    const datasets = Array.from(allKeys).map((key, i) => {
        const color = colors[i % colors.length];
        return {
            label: key,
            data: filteredHistory.map(item => {
                const val = item.businessMetrics ? item.businessMetrics[key] : null;
                return extractNumeric(val) || 0;
            }),
            borderColor: color,
            backgroundColor: color.replace(')', ', 0.1)').replace('rgb', 'rgba'),
            borderWidth: 2,
            tension: 0.3,
            pointRadius: 0
        };
    });

    businessTrendChart.data.labels = labels;
    businessTrendChart.data.datasets = datasets;
    businessTrendChart.update('none');

    // 3. Update Business Distribution Chart (Histogram / Summary Bars)
    if (connObj.lastMetrics && connObj.lastMetrics.businessMetrics) {
        const histLabels = [];
        const histValues = [];
        const histColors = [];
        
        Object.keys(connObj.lastMetrics.businessMetrics).forEach((key, idx) => {
            const num = extractNumeric(connObj.lastMetrics.businessMetrics[key]);
            if (num !== null) {
                histLabels.push(key);
                histValues.push(num);
                histColors.push(colors[idx % colors.length]);
            }
        });

        businessHistChart.data.labels = histLabels;
        businessHistChart.data.datasets = [{
            label: 'Current Snapshot / Value',
            data: histValues,
            backgroundColor: histColors,
            borderRadius: 6
        }];
        businessHistChart.update('none');
    }
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
            businessCardsGrid.innerHTML = '<p class="no-data">Fetching metrics...</p>';
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
    [tabBtnMetrics, tabBtnAlertSettings, tabBtnAlertLogs].forEach(btn => {
        if (btn.dataset.view === viewName) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });

    // Toggle view panel visibility
    viewMetrics.classList.add('hidden');
    viewAlertSettings.classList.add('hidden');
    viewAlertLogs.classList.add('hidden');

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
    }
}

tabBtnMetrics.addEventListener('click', () => switchDashboardView('metrics'));
tabBtnAlertSettings.addEventListener('click', () => switchDashboardView('alert-settings'));
tabBtnAlertLogs.addEventListener('click', () => switchDashboardView('alert-logs'));

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
function showToast(title, subtitle, statusBadgesHtml = '') {
    const toast = document.createElement('div');
    toast.className = 'alert-toast';
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

function renderAlertLogs() {
    alertLogsTbody.innerHTML = '';
    if (alertLogs.length === 0) {
        alertLogsTbody.innerHTML = `
            <tr class="empty-table-row">
                <td colspan="5" class="empty-table">No alerts triggered yet.</td>
            </tr>
        `;
        return;
    }

    alertLogs.forEach(log => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td style="white-space: nowrap; font-family: monospace;">${formatTime(log.timestamp)}</td>
            <td><strong>${log.ruleCondition}</strong></td>
            <td><code style="color: #f87171; font-weight:700;">${log.observedValue}</code></td>
            <td style="color: var(--text-secondary);">${log.serverAddress}</td>
            <td>${log.statusBadges || `<span class="badge-status played">🔊 ${log.soundStatus}</span>`}</td>
        `;
        alertLogsTbody.appendChild(tr);
    });
}

clearAlertLogsBtn.addEventListener('click', () => {
    alertLogs = [];
    localStorage.setItem('rill_alert_logs', JSON.stringify(alertLogs));
    updateAlertLogsBadge();
    renderAlertLogs();
});
