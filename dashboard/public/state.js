/* Shared, DOM-independent state operations; also exercised by node:test. */
(function(root) {
    function escapeHtml(value) {
        return String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
    }
    function ensureConnection(connection) {
        connection.scheduledConfigs ||= Object.create(null);
        connection.scheduledResults ||= Object.create(null);
        connection.businessMetrics ||= Object.create(null);
        connection.queryMetricKeys ||= Object.create(null);
        connection.stoppedQueries ||= new Set();
        connection.ruleTriggers ||= Object.create(null);
        return connection;
    }
    function receiveResult(connection, payload) {
        ensureConnection(connection);
        const config = connection.scheduledConfigs[payload.queryId];
        if (!config || connection.stoppedQueries.has(payload.queryId)) return {};
        connection.scheduledResults[payload.queryId] = payload;
        for (const key of connection.queryMetricKeys[payload.queryId] || []) delete connection.businessMetrics[key];
        const metrics = {};
        if (!payload.error && payload.rows?.length) {
            // Prefer the latest timestamp, independent of server result ordering.
            const rows = payload.rows;
            const timeColumn = ['ts', 'timestamp', 'insert_ts'].find(k => k in rows[0]);
            const latest = timeColumn ? rows.reduce((a,b) => a[timeColumn] > b[timeColumn] ? a : b) : rows[rows.length - 1];
            for (const [key, value] of Object.entries(latest)) {
                if (!['ts','timestamp','insert_ts'].includes(key) && typeof value === 'number' && Number.isFinite(value)) {
                    metrics[`${config.name || payload.queryId} - ${key}`] = value;
                }
            }
            Object.assign(connection.businessMetrics, metrics);
        }
        connection.queryMetricKeys[payload.queryId] = Object.keys(metrics);
        if (connection.lastMetrics) connection.lastMetrics.businessMetrics = connection.businessMetrics;
        return metrics;
    }
    function reconnectMessages(connection) {
        ensureConnection(connection);
        return [
            ...Array.from(connection.stoppedQueries, queryId => ({action:'unschedule_query',tabId:connection.id,queryId})),
            ...Object.entries(connection.scheduledConfigs).map(([queryId,c]) => ({action:'schedule_query',tabId:connection.id,queryId,sql:c.sql,intervalSeconds:c.intervalSeconds}))
        ];
    }
    const api = { escapeHtml, ensureConnection, receiveResult, reconnectMessages };
    if (typeof module !== 'undefined') module.exports = api;
    root.RillState = api;
})(typeof window === 'undefined' ? globalThis : window);
