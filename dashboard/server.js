const express = require('express');
const http = require('node:http');
const path = require('node:path');
const fs = require('node:fs');
const crypto = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');
const { spawn } = require('node:child_process');
const { equalSecret, sendWebhook } = require('./security');

function getPythonBinary() {
    if (process.env.RILL_PYTHON) return process.env.RILL_PYTHON;
    for (const prefix of [process.env.VIRTUAL_ENV, process.env.CONDA_PREFIX]) {
        if (prefix) {
            const executable = path.join(prefix, 'bin', 'python3');
            if (fs.existsSync(executable)) return executable;
        }
    }
    return 'python3';
}

function createDashboard(options = {}) {
    const host = options.host || process.env.HOST || '127.0.0.1';
    const token = options.token ?? process.env.RILL_DASHBOARD_TOKEN ?? '';
    if (!['127.0.0.1', '::1', 'localhost'].includes(host) && !token) {
        throw new Error('Remote dashboard access requires RILL_DASHBOARD_TOKEN');
    }
    const allowedHosts = new Set(['localhost', '127.0.0.1', '[::1]', host,
        ...(process.env.RILL_ALLOWED_HOSTS || '').split(',').filter(Boolean)]);
    const allowedOrigins = new Set((process.env.RILL_ALLOWED_ORIGINS || '').split(',').filter(Boolean));
    const webhookOrigins = new Set((process.env.RILL_WEBHOOK_ORIGINS || 'https://hooks.slack.com,https://chat.googleapis.com').split(',').filter(Boolean));
    const spawnWorker = options.spawnWorker || spawn;
    const app = express();
    const server = http.createServer(app);
    const wss = new WebSocketServer({ noServer: true, maxPayload: 1024 * 1024 });
    const sessions = new Map();
    const activeWorkers = new Map();
    function validHost(req) {
        try { return allowedHosts.has(new URL(`http://${req.headers.host}`).hostname); } catch { return false; }
    }
    function validOrigin(req) {
        const origin = req.headers.origin;
        return origin === `${req.socket.encrypted ? 'https' : 'http'}://${req.headers.host}` || allowedOrigins.has(origin);
    }
    function authorized(req) {
        if (!token) return true;
        const session = (req.headers.cookie || '').split(';').map(x => x.trim()).find(x => x.startsWith('rill_session='))?.slice(13);
        const expires = sessions.get(session);
        if (expires && expires > Date.now()) return true;
        if (session) sessions.delete(session);
        return false;
    }
    app.disable('x-powered-by');
    app.use((req, res, next) => {
        if (!validHost(req)) return res.status(403).json({ error: 'Host not allowed' });
        res.setHeader('X-Content-Type-Options', 'nosniff');
        res.setHeader('Referrer-Policy', 'no-referrer');
        res.setHeader('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'");
        next();
    });
    app.use(express.json({ limit: '64kb' }));
    app.get('/api/session', (req, res) => res.json({ authenticated: authorized(req) }));
    app.post('/api/session', (req, res) => {
        if (!validOrigin(req) || !token || !equalSecret(req.body?.token || '', token)) {
            return res.status(403).json({ error: 'Invalid dashboard access token' });
        }
        for (const [key, expires] of sessions) if (expires <= Date.now()) sessions.delete(key);
        if (sessions.size >= 1000) return res.status(429).json({ error: 'Too many sessions' });
        const session = crypto.randomBytes(32).toString('hex');
        sessions.set(session, Date.now() + 8 * 3600 * 1000);
        res.setHeader('Set-Cookie', `rill_session=${session}; HttpOnly; SameSite=Strict; Path=/; Max-Age=28800${req.secure || process.env.RILL_COOKIE_SECURE === '1' ? '; Secure' : ''}`);
        res.json({ authenticated: true });
    });
    app.use('/api', (req, res, next) => {
        if (!authorized(req)) return res.status(401).json({ error: 'Dashboard authentication required' });
        if (req.method !== 'GET' && !validOrigin(req)) return res.status(403).json({ error: 'Origin not allowed' });
        next();
    });
    app.post('/api/webhook/send', async (req, res) => {
        try {
            const { url, format, payload } = req.body;
            if (!payload || typeof payload !== 'object') throw new Error('Alert payload is required');
            let body = payload;
            if (['slack', 'google_chat'].includes(format)) {
                body = { text: `[Rill Alert] ${payload.ruleName || 'Rule'}\n${payload.metricField} ${payload.operator} ${payload.threshold}\nObserved: ${payload.observedValue}\nServer: ${payload.serverAddress}` };
            }
            const status = await sendWebhook(url, body, webhookOrigins);
            res.json({ success: true, status });
        } catch (error) { res.status(400).json({ success: false, error: error.message }); }
    });
    for (const [route, file] of Object.entries({
        'chart.js': 'chart.js/dist/chart.umd.js',
        'hammer.js': 'hammerjs/hammer.min.js',
        'zoom.js': 'chartjs-plugin-zoom/dist/chartjs-plugin-zoom.min.js'
    })) app.get(`/vendor/${route}`, (req, res) => res.sendFile(path.join(__dirname, 'node_modules', file)));
    app.use(express.static(path.join(__dirname, 'public')));
    server.on('upgrade', (req, socket, head) => {
        if (!validHost(req) || !validOrigin(req) || !authorized(req)) {
            socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); return;
        }
        wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws));
    });
    wss.on('connection', ws => {
        const workers = new Map();
        activeWorkers.set(ws, workers);
        const send = (tabId, payload) => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ tabId, payload })); };
        ws.on('message', message => {
            let req;
            try {
                req = JSON.parse(message);
                const { tabId } = req;
                if (!Number.isSafeInteger(tabId) || tabId <= 0) throw new Error('Invalid connection ID');
                if (req.action === 'connect') {
                    if (typeof req.address !== 'string' || !/^quack:(?:\[[0-9a-f:]+\]|[a-z0-9.-]+):\d{1,5}$/i.test(req.address)) throw new Error('Invalid Quack address');
                    if (workers.size >= 16 && !workers.has(tabId)) throw new Error('Connection limit reached');
                    const old = workers.get(tabId);
                    workers.delete(tabId);
                    old?.kill();
                    const args = ['quack_worker.py', '--address', req.address];
                    if (req.token) args.push('--token', String(req.token));
                    const worker = spawnWorker(getPythonBinary(), args, { cwd: __dirname });
                    workers.set(tabId, worker);
                    let buffer = '';
                    worker.stdout.on('data', data => {
                        if (workers.get(tabId) !== worker) return;
                        buffer += data.toString();
                        let index;
                        while ((index = buffer.indexOf('\n')) !== -1) {
                            const line = buffer.slice(0, index); buffer = buffer.slice(index + 1);
                            try { send(tabId, JSON.parse(line)); } catch { /* extension diagnostic output */ }
                        }
                        if (buffer.length > 16 * 1024 * 1024) worker.kill();
                    });
                    worker.stderr.on('data', data => console.error('[Quack worker]', data.toString().slice(0, 2000)));
                    worker.stdin.on('error', error => send(tabId, { error: error.message }));
                    worker.on('error', error => send(tabId, { error: `Worker could not start: ${error.message}` }));
                    worker.on('close', code => {
                        if (workers.get(tabId) !== worker) return;
                        workers.delete(tabId);
                        send(tabId, { error: `Quack connection closed (Exit code ${code})` });
                    });
                } else if (req.action === 'disconnect') {
                    const worker = workers.get(tabId); workers.delete(tabId); worker?.kill();
                } else if (['preview_query', 'schedule_query', 'unschedule_query'].includes(req.action)) {
                    const worker = workers.get(tabId);
                    if (!worker || !worker.stdin.writable) throw new Error('Connection is not ready');
                    worker.stdin.write(JSON.stringify(req) + '\n');
                } else throw new Error('Unknown action');
            } catch (error) { send(req?.tabId, { error: error.message }); }
        });
        ws.on('close', () => {
            for (const worker of workers.values()) worker.kill();
            workers.clear(); activeWorkers.delete(ws);
        });
        ws.on('error', () => ws.close());
    });
    return { app, server, wss, activeWorkers, host };
}
if (require.main === module) {
    const { server, host } = createDashboard();
    server.listen(Number(process.env.PORT || 3000), host, () => console.log(`Dashboard: http://${host}:${server.address().port}`));
}
module.exports = { createDashboard, getPythonBinary };
