// Application State
let ws = null;
let activeConnections = []; // Array of { id, address, token, metricsHistory: [], lastMetrics }
let activeConnectionId = null; // ID of the currently selected tab connection
let nextConnectionId = 1;
let resourceChart = null;
let businessTrendChart = null;
let businessHistChart = null;

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
                if (activeConnectionId === tabId && !connObj.lastMetrics) {
                    connectError.innerText = `Connection failed: ${payload.error}`;
                    connectBtn.disabled = false;
                    connectBtn.innerText = 'Connect Console';
                }
            } else if (payload.status === 'connected') {
                console.log(`Tab ${tabId} successfully attached to Quack server.`);
                connectBtn.disabled = false;
                connectBtn.innerText = 'Connect Console';
                selectTab(connObj.id);
            } else if (payload.type === 'metrics') {
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

                if (activeConnectionId === connObj.id) {
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
    if (!resourceChart || !businessTrendChart || !businessHistChart || activeConnectionId === null) return;
    
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

// Render tabs list dynamically
function renderTabs() {
    tabsBar.innerHTML = '';
    
    if (activeConnections.length === 0) {
        tabsContainer.classList.add('hidden');
        return;
    }
    
    tabsContainer.classList.remove('hidden');
    
    activeConnections.forEach(c => {
        const tab = document.createElement('div');
        tab.className = `tab-item ${c.id === activeConnectionId ? 'active' : ''}`;
        
        const span = document.createElement('span');
        span.innerText = c.address;
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
        
        connectionStatusPill.classList.add('connected');
        connectionStatusPill.querySelector('.status-text').innerText = 'Connected';
        
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
